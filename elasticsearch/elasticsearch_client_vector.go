package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/tx7do/go-crud/vector"
)

// ─────────────────────────────────────────────────────────────────────────────
// 向量检索（RAG / 语义检索）
//
// 映射到 Elasticsearch 8+/9.x 的顶层 knn 子句与 dense_vector 字段：
//   - 距离度量在建 mapping 时通过 similarity 固定，查询期传入的
//     Query.Metric 会被忽略；
//   - Query.Filter 为引擎原生的 query DSL（map[string]any）或 DSL 数组，
//     作为 knn.filter 施加 pre-filter；
//   - Query.MinScore 映射到 knn.similarity（最低相似度阈值）；
//   - Query.MaxDistance 在 ES 无对应参数，忽略。
// ─────────────────────────────────────────────────────────────────────────────

// esSimilarityNames 把统一度量枚举映射到 ES mapping 的 similarity 取值。
var esSimilarityNames = map[vector.DistanceMetric]string{
	vector.MetricCosine:     "cosine",
	vector.MetricEuclidean:  "l2_norm",
	vector.MetricDotProduct: "dot_product",
}

// BuildDenseVectorMapping 构建单个 dense_vector 字段的索引 mapping JSON。
// 返回值可直接作为 CreateIndex 的 mapping 参数。
func BuildDenseVectorMapping(field string, dims int, metric vector.DistanceMetric) (string, error) {
	if field == "" {
		return "", ErrInvalidRequest
	}
	if dims <= 0 {
		return "", ErrInvalidRequest
	}
	similarity, ok := esSimilarityNames[metric]
	if !ok {
		return "", fmt.Errorf("%w: unsupported vector metric %q", ErrInvalidRequest, metric)
	}

	mapping := map[string]any{
		"mappings": map[string]any{
			"properties": map[string]any{
				field: map[string]any{
					"type":       "dense_vector",
					"dims":       dims,
					"index":      true,
					"similarity": similarity,
				},
			},
		},
	}

	raw, err := json.Marshal(mapping)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// CreateVectorIndex 创建带 dense_vector 字段的索引。
//
//	@param ctx 上下文
//	@param indexName 索引名
//	@param field 向量字段名
//	@param dims 向量维度
//	@param metric 距离度量（cosine / euclidean / dot）
func (c *Client) CreateVectorIndex(
	ctx context.Context,
	indexName, field string,
	dims int,
	metric vector.DistanceMetric,
) error {
	mapping, err := BuildDenseVectorMapping(field, dims, metric)
	if err != nil {
		return err
	}
	return c.CreateIndex(ctx, indexName, mapping, "")
}

// buildKnnClause 把统一向量查询翻译成 ES 顶层 knn 子句。
func buildKnnClause(q *vector.Query) (map[string]any, error) {
	if err := q.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidVectorQuery, err)
	}

	clause := map[string]any{
		"field":          q.Field,
		"query_vector":   q.Vector,
		"k":              q.TopK,
		"num_candidates": q.EffectiveNumCandidates(),
	}
	if q.Filter != nil {
		clause["filter"] = q.Filter
	}
	if q.MinScore > 0 {
		clause["similarity"] = q.MinScore
	}
	return clause, nil
}

// KnnSearch 纯 kNN 向量检索，返回 TopK 近邻。
//
// 结果的 _score 即 vector.Hit.Score 的来源（ES 原生相关性分，越大越相似；
// cosine 度量下 ES 把余弦相似度归一到 (1+cos)/2 ∈ [0,1]）。
//
//	示例调用：
//	  res, err := client.KnnSearch(ctx, "docs", &vector.Query{
//	      Field: "embedding", Vector: embedding, TopK: 10,
//	      Filter: map[string]any{"bool": map[string]any{
//	          "filter": map[string]any{"term": map[string]any{"tenant_id": "t1"}},
//	      }},
//	  })
func (c *Client) KnnSearch(ctx context.Context, indexName string, q *vector.Query) (*SearchResult, error) {
	return c.SearchWithKnn(ctx, indexName, nil, q)
}

// SearchWithKnn 在任意 DSL body（可含 query 实现混合检索、aggs 聚合等）基础上
// 并入 knn 子句执行搜索。body 中已存在 "knn" 键时报错，避免静默覆盖。
func (c *Client) SearchWithKnn(
	ctx context.Context,
	indexName string,
	body map[string]any,
	q *vector.Query,
) (*SearchResult, error) {
	clause, err := buildKnnClause(q)
	if err != nil {
		return nil, err
	}
	if body == nil {
		body = make(map[string]any)
	}
	if _, exists := body["knn"]; exists {
		return nil, fmt.Errorf("%w: body already contains knn clause", ErrInvalidRequest)
	}
	body["knn"] = clause

	return c.SearchWithBody(ctx, indexName, body)
}

// ToVectorResult 把 ES 搜索结果转换为统一向量检索结果。
// T 为文档类型，按 _source 反序列化（传 json.RawMessage 可取原始文档）。
func ToVectorResult[T any](sr *SearchResult) *vector.Result[T] {
	if sr == nil {
		return nil
	}
	res := &vector.Result[T]{
		Total: int64(sr.Hits.Total.Value),
		Hits:  make([]vector.Hit[T], 0, len(sr.Hits.Hits)),
	}
	for _, h := range sr.Hits.Hits {
		var value T
		if len(h.Source) > 0 {
			_ = json.Unmarshal(h.Source, &value)
		}
		res.Hits = append(res.Hits, vector.Hit[T]{
			Score: h.Score,
			Value: value,
		})
	}
	if res.Total == 0 && len(res.Hits) > 0 {
		res.Total = int64(len(res.Hits))
	}
	return res
}
