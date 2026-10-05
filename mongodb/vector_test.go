package mongodb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tx7do/go-utils/mapper"

	bsonV2 "go.mongodb.org/mongo-driver/v2/bson"
	mongoV2 "go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/tx7do/go-crud/mongodb/query"
	"github.com/tx7do/go-crud/vector"
)

// 向量检索（Atlas $vectorSearch）测试：管道构造为纯函数可离线断言；
// 客户端执行依赖 Atlas 服务，属集成测试范畴（见 client_test.go 门禁约定）。

type testVectorDoc struct {
	ID       string    `bson:"_id"`
	Content  string    `bson:"content"`
	Embedded []float32 `bson:"embedded"`
}

func TestBuildVectorSearchPipeline(t *testing.T) {
	q := &vector.Query{
		Index:  "doc_vector_index",
		Field:  "embedded",
		Vector: []float32{0.1, 0.2, 0.3},
		TopK:   10,
		Filter: bsonV2.M{"category": "news"},
	}
	pipeline, err := buildVectorSearchPipeline(q, bsonV2.M{"category": "news"})
	require.NoError(t, err)
	require.Len(t, pipeline, 2)

	// $vectorSearch 阶段
	vs := pipeline[0][0].Value.(bsonV2.M)
	assert.Equal(t, "doc_vector_index", vs["index"])
	assert.Equal(t, "embedded", vs["path"])
	assert.Equal(t, []float32{0.1, 0.2, 0.3}, vs["queryVector"])
	assert.Equal(t, 10, vs["limit"])
	assert.Equal(t, q.EffectiveNumCandidates(), vs["numCandidates"])
	assert.Equal(t, bsonV2.M{"category": "news"}, vs["filter"])

	// $project 阶段：原始文档 + 相似度分
	project := pipeline[1][0].Value.(bsonV2.M)
	assert.Equal(t, "$$ROOT", project["doc"])
	meta, ok := project[vectorSearchScoreField].(bsonV2.M)
	require.True(t, ok)
	assert.Equal(t, "vectorSearchScore", meta["$meta"])

	// MinScore 追加 $match 阶段
	q.MinScore = 0.8
	pipeline, err = buildVectorSearchPipeline(q, nil)
	require.NoError(t, err)
	require.Len(t, pipeline, 3)
	match := pipeline[2][0].Value.(bsonV2.M)
	assert.Contains(t, match, vectorSearchScoreField)
}

func TestBuildVectorSearchPipeline_NumCandidates(t *testing.T) {
	// 未指定候选数时按 TopK 放大默认值
	q := &vector.Query{Index: "idx", Field: "embedded", Vector: []float32{1}, TopK: 7}
	pipeline, err := buildVectorSearchPipeline(q, nil)
	require.NoError(t, err)
	vs := pipeline[0][0].Value.(bsonV2.M)
	assert.Equal(t, 70, vs["numCandidates"])

	// 候选数 < TopK 应报错
	q.NumCandidates = 3
	_, err = buildVectorSearchPipeline(q, nil)
	assert.ErrorContains(t, err, "must not be less than topK")
}

func TestBuildVectorSearchPipeline_Validation(t *testing.T) {
	// Atlas 场景 index 必填
	_, err := buildVectorSearchPipeline(&vector.Query{
		Field: "embedded", Vector: []float32{1}, TopK: 1,
	}, nil)
	assert.ErrorContains(t, err, "index is required")

	// 其余校验沿用 vector.Query.Validate
	_, err = buildVectorSearchPipeline(&vector.Query{
		Index: "idx", Field: "", Vector: []float32{1}, TopK: 1,
	}, nil)
	assert.Error(t, err)
	_, err = buildVectorSearchPipeline(&vector.Query{
		Index: "idx", Field: "embedded", Vector: nil, TopK: 1,
	}, nil)
	assert.Error(t, err)
	_, err = buildVectorSearchPipeline(&vector.Query{
		Index: "idx", Field: "embedded", Vector: []float32{1}, TopK: 0,
	}, nil)
	assert.Error(t, err)
}

func TestSearchByVector_Guards(t *testing.T) {
	ctx := context.Background()

	// nil client
	repo := NewRepository[testVectorDoc, testVectorDoc](nil, "", mapper.NewCopierMapper[testVectorDoc, testVectorDoc](), nil)
	_, err := repo.SearchByVector(ctx, nil, &vector.Query{
		Index: "idx", Field: "embedded", Vector: []float32{1}, TopK: 1,
	})
	assert.ErrorContains(t, err, "mongodb database is nil")

	// 空 collection
	repo = NewRepository[testVectorDoc, testVectorDoc](&Client{}, "", mapper.NewCopierMapper[testVectorDoc, testVectorDoc](), nil)
	_, err = repo.SearchByVector(ctx, query.NewQueryBuilder(), &vector.Query{
		Index: "idx", Field: "embedded", Vector: []float32{1}, TopK: 1,
	})
	assert.ErrorContains(t, err, "collection is empty")
}

func TestCreateVectorSearchIndex_Guards(t *testing.T) {
	ctx := context.Background()

	// nil client
	c := &Client{}
	assert.ErrorIs(t, c.CreateVectorSearchIndex(ctx, "docs", "idx", "embedded", 3, vector.MetricCosine), mongoV2.ErrClientDisconnected)
	assert.ErrorIs(t, c.DropVectorSearchIndex(ctx, "docs", "idx"), mongoV2.ErrClientDisconnected)

	// 参数校验（不触网）
	c2 := &Client{cli: &mongoV2.Client{}, timeout: 1}
	assert.Error(t, c2.CreateVectorSearchIndex(ctx, "", "idx", "embedded", 3, vector.MetricCosine))
	assert.Error(t, c2.CreateVectorSearchIndex(ctx, "docs", "", "embedded", 3, vector.MetricCosine))
	assert.Error(t, c2.CreateVectorSearchIndex(ctx, "docs", "idx", "", 3, vector.MetricCosine))
	assert.Error(t, c2.CreateVectorSearchIndex(ctx, "docs", "idx", "embedded", 0, vector.MetricCosine))
	assert.Error(t, c2.CreateVectorSearchIndex(ctx, "docs", "idx", "embedded", 3, "unknown"))
	assert.Error(t, c2.DropVectorSearchIndex(ctx, "", "idx"))
	assert.Error(t, c2.DropVectorSearchIndex(ctx, "docs", ""))
}
