package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	opensearchV4 "github.com/opensearch-project/opensearch-go/v4"
	opensearchapiV4 "github.com/opensearch-project/opensearch-go/v4/opensearchapi"

	"github.com/tx7do/go-wind/log"

	"github.com/tx7do/go-crud/vector"
)

// ─────────────────────────────────────────────────────────────────────────────
// 向量检索（RAG / 语义检索）
//
// 映射到 OpenSearch 2.11+ 的 query.knn（k-NN query DSL）与 knn_vector 字段：
//   - 距离度量在建 mapping 时通过 method.space_type 固定，查询期传入的
//     Query.Metric 会被忽略；
//   - Query.Filter 为引擎原生的 query DSL（map[string]any），作为
//     knn.filter 施加 pre-filter（需 efficient filtering，OS 2.11+）；
//   - Query.MinScore 映射到顶层 min_score（按 _score 过滤）；
//   - Query.MaxDistance 在 OS 无对应参数，忽略；
//   - 建索引时自动写入 index.knn=true 设置。
// ─────────────────────────────────────────────────────────────────────────────

// osSpaceTypeNames 把统一度量枚举映射到 knn_vector mapping 的 space_type 取值。
var osSpaceTypeNames = map[vector.DistanceMetric]string{
	vector.MetricCosine:     "cosinesimil",
	vector.MetricEuclidean:  "l2",
	vector.MetricDotProduct: "innerproduct",
}

// BuildKnnVectorMapping 构建单个 knn_vector 字段的索引 mapping JSON
// （附带 index.knn=true 的 settings）。返回的 mapping、settings 可直接
// 作为 CreateIndex 的参数。
func BuildKnnVectorMapping(field string, dims int, metric vector.DistanceMetric) (mapping string, settings string, err error) {
	if field == "" {
		return "", "", ErrInvalidRequest
	}
	if dims <= 0 {
		return "", "", ErrInvalidRequest
	}
	spaceType, ok := osSpaceTypeNames[metric]
	if !ok {
		return "", "", fmt.Errorf("%w: unsupported vector metric %q", ErrInvalidRequest, metric)
	}

	mappingBody := map[string]any{
		"mappings": map[string]any{
			"properties": map[string]any{
				field: map[string]any{
					"type":      "knn_vector",
					"dimension": dims,
					"method": map[string]any{
						"name":       "hnsw",
						"space_type": spaceType,
						"engine":     "lucene",
					},
				},
			},
		},
	}
	settingsBody := map[string]any{
		"index": map[string]any{"knn": true},
	}

	rawMapping, err := json.Marshal(mappingBody)
	if err != nil {
		return "", "", err
	}
	rawSettings, err := json.Marshal(settingsBody)
	if err != nil {
		return "", "", err
	}
	return string(rawMapping), string(rawSettings), nil
}

// CreateVectorIndex 创建带 knn_vector 字段的索引（自动开启 index.knn）。
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
	mapping, settings, err := BuildKnnVectorMapping(field, dims, metric)
	if err != nil {
		return err
	}
	return c.CreateIndex(ctx, indexName, mapping, settings)
}

// buildKnnQueryBody 把统一向量查询翻译成 OS 的 query.knn 查询体。
func buildKnnQueryBody(q *vector.Query) (map[string]any, error) {
	if err := q.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidVectorQuery, err)
	}

	fieldSpec := map[string]any{
		"vector": q.Vector,
		"k":      q.TopK,
	}
	if q.Filter != nil {
		fieldSpec["filter"] = q.Filter
	}

	body := map[string]any{
		"query": map[string]any{
			"knn": map[string]any{
				q.Field: fieldSpec,
			},
		},
		"size": q.TopK,
	}
	if q.MinScore > 0 {
		body["min_score"] = q.MinScore
	}
	return body, nil
}

// KnnSearch 纯 kNN 向量检索，返回 TopK 近邻。
//
// 结果的 _score 即 vector.Hit.Score 的来源（OS 原生相关性分，越大越相似；
// lucene HNSW 下 cosinesimil 的 _score = (1 + cos) / 2 ∈ [0,1]）。
//
//	示例调用：
//	  resp, err := client.KnnSearch(ctx, "docs", &vector.Query{
//	      Field: "embedding", Vector: embedding, TopK: 10,
//	      Filter: map[string]any{"term": map[string]any{"tenant_id": "t1"}},
//	  })
func (c *Client) KnnSearch(ctx context.Context, indexName string, q *vector.Query) (*opensearchapiV4.SearchResp, error) {
	return c.SearchWithKnn(ctx, indexName, nil, q)
}

// SearchWithKnn 在任意 DSL body（如 _source 投影、aggs 聚合）基础上并入
// query.knn 执行搜索。body 中已存在 "query" 键时报错，避免静默覆盖。
func (c *Client) SearchWithKnn(
	ctx context.Context,
	indexName string,
	body map[string]any,
	q *vector.Query,
) (*opensearchapiV4.SearchResp, error) {
	knnBody, err := buildKnnQueryBody(q)
	if err != nil {
		return nil, err
	}
	if body == nil {
		body = make(map[string]any)
	}
	if _, exists := body["query"]; exists {
		return nil, fmt.Errorf("%w: body already contains query clause", ErrInvalidRequest)
	}
	for k, v := range knnBody {
		body[k] = v
	}

	buf := &bytes.Buffer{}
	if err = json.NewEncoder(buf).Encode(body); err != nil {
		return nil, err
	}

	searchReq := &opensearchapiV4.SearchReq{
		Indices: []string{indexName},
		Body:    buf,
	}
	var searchResult opensearchapiV4.SearchResp
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPost, searchReq, &searchResult)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to knn search documents: %v", err))
		return nil, err
	}
	if resp.IsError() {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Error(context.Background(), fmt.Sprintf("knn search document failed [%d]: %s", resp.StatusCode, string(bodyBytes)))
		return nil, ErrVectorSearch
	}

	return &searchResult, nil
}

// ToVectorResult 把 OpenSearch 搜索结果转换为统一向量检索结果。
// T 为文档类型，按 _source 反序列化（传 json.RawMessage 可取原始文档）。
func ToVectorResult[T any](sr *opensearchapiV4.SearchResp) *vector.Result[T] {
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
			Score: float64(h.Score),
			Value: value,
		})
	}
	if res.Total == 0 && len(res.Hits) > 0 {
		res.Total = int64(len(res.Hits))
	}
	return res
}
