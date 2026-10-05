package mongodb

import (
	"context"
	"errors"
	"fmt"

	bsonV2 "go.mongodb.org/mongo-driver/v2/bson"
	mongoV2 "go.mongodb.org/mongo-driver/v2/mongo"
	optionsV2 "go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/tx7do/go-wind/log"

	"github.com/tx7do/go-crud/mongodb/query"
	"github.com/tx7do/go-crud/vector"
)

// ─────────────────────────────────────────────────────────────────────────────
// 向量检索（RAG / 语义检索）— 基于 MongoDB Atlas Vector Search 的 $vectorSearch
// 聚合阶段（Atlas 7.0+，或自管 MongoDB 8.0+ 且已创建 Search 索引）。
//
//   - Query.Index 必填：$vectorSearch 依赖命名 Search 索引，
//     可用 Client.CreateVectorSearchIndex 创建；
//   - Query.Filter（或传入的 query.Builder 过滤文档）作为 $vectorSearch.filter
//     施加 pre-filter（过滤字段须在 Search 索引中可过滤）；
//   - Query.MinScore 在投影出 _vs_score 后以 $match 施加；
//   - Query.MaxDistance 无对应参数，忽略；numCandidates 未指定时取 TopK × 10
//     （Atlas 要求 numCandidates ≥ limit）；
//   - 结果分数来自 $meta: "vectorSearchScore"，Atlas 原生「越大越相似」，
//     与 vector.Hit.Score 语义一致，无需换算。
// ─────────────────────────────────────────────────────────────────────────────

// atlasSimilarityNames 把统一度量枚举映射到 Atlas knnVector 的 similarity 取值。
var atlasSimilarityNames = map[vector.DistanceMetric]string{
	vector.MetricCosine:     "cosine",
	vector.MetricEuclidean:  "euclidean",
	vector.MetricDotProduct: "dotProduct",
}

// vectorSearchScoreField 向量相似度分投影字段名。
const vectorSearchScoreField = "_vs_score"

// buildVectorSearchPipeline 构造 $vectorSearch 聚合管道。
// filterDoc 为 pre-filter 文档（可空），通常来自 query.Builder 的过滤条件。
func buildVectorSearchPipeline(q *vector.Query, filterDoc bsonV2.M) ([]bsonV2.D, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	if q.Index == "" {
		return nil, errors.New("vector query index is required for atlas vector search")
	}
	if q.NumCandidates > 0 && q.NumCandidates < q.TopK {
		return nil, fmt.Errorf("numCandidates (%d) must not be less than topK (%d)", q.NumCandidates, q.TopK)
	}

	vectorSearch := bsonV2.M{
		"index":         q.Index,
		"path":          q.Field,
		"queryVector":   q.Vector,
		"numCandidates": q.EffectiveNumCandidates(),
		"limit":         q.TopK,
	}
	if len(filterDoc) > 0 {
		vectorSearch["filter"] = filterDoc
	}

	// 投影出原始文档与相似度分，避免把分数混入业务文档字段
	pipeline := []bsonV2.D{
		{{Key: "$vectorSearch", Value: vectorSearch}},
		{{Key: "$project", Value: bsonV2.M{
			"doc":                  "$$ROOT",
			vectorSearchScoreField: bsonV2.M{"$meta": "vectorSearchScore"},
		}}},
	}
	if q.MinScore > 0 {
		pipeline = append(pipeline, bsonV2.D{
			{Key: "$match", Value: bsonV2.M{vectorSearchScoreField: bsonV2.M{"$gte": q.MinScore}}},
		})
	}
	return pipeline, nil
}

// vectorSearchHit 聚合结果解码载体。
type vectorSearchHit[ENTITY any] struct {
	Score float64 `bson:"_vs_score"`
	Doc   ENTITY  `bson:"doc"`
}

// SearchByVector 向量近邻检索（Atlas Vector Search）。
//
// qb 的过滤条件会作为 $vectorSearch.filter 施加，且沿用模块既有约定：
// 租户行级强制 InjectTenantFilterIntoBuilder 对本查询同样生效。
// 返回结果按相似度分从高到低排序；Total 即命中的条数（TopK 语义，非分页）。
//
//	示例调用：
//	  qb := query.NewQueryBuilder().SetFilter(bson.M{"category": "news"})
//	  res, err := repo.SearchByVector(ctx, qb, &vector.Query{
//	      Index: "doc_vector_index", Field: "embedding", Vector: embedding, TopK: 10,
//	  })
func (r *Repository[DTO, ENTITY]) SearchByVector(
	ctx context.Context,
	qb *query.Builder,
	q *vector.Query,
) (*vector.Result[*DTO], error) {
	if r.client == nil {
		return nil, errors.New("mongodb database is nil")
	}
	if r.collection == "" {
		return nil, errors.New("collection is empty")
	}

	// 租户行级强制：注入 tenant_id 谓词到 pre-filter（仅对 tenant-scoped 实体生效；
	// 缺身份 fail-closed / 平台放行 / 租户注入）。语义同 ListWithPaging。
	if err := InjectTenantFilterIntoBuilder[ENTITY](ctx, qb); err != nil {
		return nil, err
	}

	var filterDoc bsonV2.M
	if qb != nil {
		doc, _, err := qb.BuildFind()
		if err != nil {
			return nil, err
		}
		if m, ok := doc.(bsonV2.M); ok && len(m) > 0 {
			filterDoc = m
		}
	}

	pipeline, err := buildVectorSearchPipeline(q, filterDoc)
	if err != nil {
		return nil, err
	}

	var hits []vectorSearchHit[ENTITY]
	if err = r.client.Aggregate(ctx, r.collection, pipeline, &hits); err != nil {
		log.Error(context.Background(), fmt.Sprintf("vector search failed: %v", err))
		return nil, err
	}

	result := &vector.Result[*DTO]{
		Hits:  make([]vector.Hit[*DTO], 0, len(hits)),
		Total: int64(len(hits)),
	}
	for i := range hits {
		result.Hits = append(result.Hits, vector.Hit[*DTO]{
			Score: hits[i].Score,
			Value: r.mapper.ToDTO(&hits[i].Doc),
		})
	}
	return result, nil
}

// CreateVectorSearchIndex 为集合创建向量 Search 索引（Atlas 7.0+，异步操作）。
// dynamic 映射保持开启，元数据字段默认可过滤。
//
//	@param ctx 上下文
//	@param collection 集合名
//	@param indexName Search 索引名
//	@param field 向量字段名
//	@param dims 向量维度
//	@param metric 距离度量（cosine / euclidean / dot）
func (c *Client) CreateVectorSearchIndex(
	ctx context.Context,
	collection, indexName, field string,
	dims int,
	metric vector.DistanceMetric,
) error {
	if c.cli == nil {
		log.Error(context.Background(), "mongodb client is not initialized")
		return mongoV2.ErrClientDisconnected
	}
	if collection == "" || indexName == "" || field == "" {
		return errors.New("collection, index name and field are required")
	}
	if dims <= 0 {
		return fmt.Errorf("dims must be positive, got %d", dims)
	}
	similarity, ok := atlasSimilarityNames[metric]
	if !ok {
		return fmt.Errorf("unsupported vector metric %q", metric)
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	_, err := c.cli.Database(c.database).Collection(collection).SearchIndexes().CreateOne(ctx, mongoV2.SearchIndexModel{
		Definition: bsonV2.M{
			"mappings": bsonV2.M{
				"dynamic": true,
				"fields": bsonV2.M{
					field: bsonV2.M{
						"type":       "knnVector",
						"dimensions": dims,
						"similarity": similarity,
					},
				},
			},
		},
		Options: optionsV2.SearchIndexes().SetName(indexName),
	})
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to create vector search index: %v", err))
		return err
	}
	return nil
}

// DropVectorSearchIndex 删除集合上的向量 Search 索引。
func (c *Client) DropVectorSearchIndex(ctx context.Context, collection, indexName string) error {
	if c.cli == nil {
		log.Error(context.Background(), "mongodb client is not initialized")
		return mongoV2.ErrClientDisconnected
	}
	if collection == "" || indexName == "" {
		return errors.New("collection and index name are required")
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	if err := c.cli.Database(c.database).Collection(collection).SearchIndexes().DropOne(ctx, indexName); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to drop vector search index: %v", err))
		return err
	}
	return nil
}
