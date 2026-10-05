package qdrant

import (
	"testing"

	qdrant "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tx7do/go-crud/vector"
)

// 向量检索请求构造测试（纯函数，可离线断言）。
// 分数换算语义：Cosine/Dot 原生相似度直接透传；Euclid 距离换算 1/(1+d)。

func TestBuildQueryPoints_Valid(t *testing.T) {
	q := &vector.Query{
		Field:         "embedding",
		Vector:        []float32{0.1, 0.2, 0.3},
		TopK:          5,
		NumCandidates: 42,
	}
	req, err := buildQueryPoints("coll", q, nil)
	require.NoError(t, err)
	assert.Equal(t, "coll", req.CollectionName)
	require.NotNil(t, req.Query)
	// Field 忽略：本模块只创建单一匿名向量的集合，Using 保持未设置。
	assert.Nil(t, req.Using)
	require.NotNil(t, req.Limit)
	assert.Equal(t, uint64(5), *req.Limit)
	assert.Nil(t, req.Filter)
	require.NotNil(t, req.Params)
	require.NotNil(t, req.Params.HnswEf)
	assert.Equal(t, uint64(42), *req.Params.HnswEf)
	assert.Equal(t, true, req.WithPayload.GetEnable())
}

func TestBuildQueryPoints_NoCandidatesNoParams(t *testing.T) {
	q := &vector.Query{
		Field:  "embedding",
		Vector: []float32{0.1, 0.2, 0.3},
		TopK:   5,
	}
	req, err := buildQueryPoints("coll", q, nil)
	require.NoError(t, err)
	assert.Nil(t, req.Params)
}

func TestBuildQueryPoints_FilterPassthrough(t *testing.T) {
	q := &vector.Query{
		Field:  "embedding",
		Vector: []float32{0.1, 0.2, 0.3},
		TopK:   5,
	}
	f := &qdrant.Filter{}
	req, err := buildQueryPoints("coll", q, f)
	require.NoError(t, err)
	assert.Same(t, f, req.Filter)
}

func TestBuildQueryPoints_MinScoreNotForwarded(t *testing.T) {
	// MinScore 在统一分数空间客户端过滤，不透传 ScoreThreshold
	// （其原生方向随度量翻转，见 vector.go 注释）。
	q := &vector.Query{
		Field:    "embedding",
		Vector:   []float32{0.1, 0.2, 0.3},
		TopK:     5,
		MinScore: 0.9,
	}
	req, err := buildQueryPoints("coll", q, nil)
	require.NoError(t, err)
	assert.Nil(t, req.ScoreThreshold)
}

func TestBuildQueryPoints_InvalidQueries(t *testing.T) {
	base := vector.Query{Field: "embedding", Vector: []float32{1}, TopK: 5}

	emptyField := base
	emptyField.Field = ""
	_, err := buildQueryPoints("coll", &emptyField, nil)
	assert.ErrorIs(t, err, ErrInvalidVectorQuery)

	emptyVector := base
	emptyVector.Vector = nil
	_, err = buildQueryPoints("coll", &emptyVector, nil)
	assert.ErrorIs(t, err, ErrInvalidVectorQuery)

	zeroTopK := base
	zeroTopK.TopK = 0
	_, err = buildQueryPoints("coll", &zeroTopK, nil)
	assert.ErrorIs(t, err, ErrInvalidVectorQuery)

	_, err = buildQueryPoints("", &base, nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)
}

func TestQdrantScoreToScore(t *testing.T) {
	// Cosine / Dot：原生即相似度，直接透传。
	assert.Equal(t, 0.25, qdrantScoreToScore(vector.MetricCosine, 0.25))
	assert.Equal(t, float64(-3), qdrantScoreToScore(vector.MetricDotProduct, -3))
	// 未指定度量按 cosine 处理（透传）。
	assert.Equal(t, 0.5, qdrantScoreToScore(vector.DistanceMetric(""), 0.5))
	// Euclid：距离 → 1/(1+d)。
	assert.Equal(t, 0.5, qdrantScoreToScore(vector.MetricEuclidean, 1))
	assert.Equal(t, 1.0, qdrantScoreToScore(vector.MetricEuclidean, 0))
	assert.InDelta(t, 1.0/3.0, qdrantScoreToScore(vector.MetricEuclidean, 2), 1e-9)
}
