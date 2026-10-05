package opensearch

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	opensearchapiV4 "github.com/opensearch-project/opensearch-go/v4/opensearchapi"

	"github.com/tx7do/go-crud/vector"
)

// 向量检索（query.knn）纯单元测试：只验证请求体/参数构造与结果转换，不发网络请求。

func TestBuildKnnVectorMapping(t *testing.T) {
	mapping, settings, err := BuildKnnVectorMapping("embedding", 768, vector.MetricCosine)
	require.NoError(t, err)

	var parsed map[string]any
	require.NoError(t, json.Unmarshal([]byte(mapping), &parsed))
	props := parsed["mappings"].(map[string]any)["properties"].(map[string]any)["embedding"].(map[string]any)
	assert.Equal(t, "knn_vector", props["type"])
	assert.InEpsilon(t, 768.0, props["dimension"], 1e-9)
	method := props["method"].(map[string]any)
	assert.Equal(t, "hnsw", method["name"])
	assert.Equal(t, "cosinesimil", method["space_type"])

	var parsedSettings map[string]any
	require.NoError(t, json.Unmarshal([]byte(settings), &parsedSettings))
	assert.Equal(t, true, parsedSettings["index"].(map[string]any)["knn"])

	// 度量枚举映射
	for metric, want := range map[vector.DistanceMetric]string{
		vector.MetricCosine:     "cosinesimil",
		vector.MetricEuclidean:  "l2",
		vector.MetricDotProduct: "innerproduct",
	} {
		m, _, err := BuildKnnVectorMapping("v", 3, metric)
		require.NoError(t, err)
		assert.Contains(t, m, want)
	}

	// 非法参数
	_, _, err = BuildKnnVectorMapping("", 3, vector.MetricCosine)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, _, err = BuildKnnVectorMapping("v", 0, vector.MetricCosine)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, _, err = BuildKnnVectorMapping("v", 3, "unknown")
	assert.Error(t, err)
}

func TestBuildKnnQueryBody(t *testing.T) {
	q := &vector.Query{
		Field:  "embedding",
		Vector: []float32{0.1, 0.2, 0.3},
		TopK:   10,
		Filter: map[string]any{"term": map[string]any{"tenant_id": "t1"}},
	}
	body, err := buildKnnQueryBody(q)
	require.NoError(t, err)

	assert.Equal(t, 10, body["size"])

	query := body["query"].(map[string]any)
	knn := query["knn"].(map[string]any)
	fieldSpec := knn["embedding"].(map[string]any)
	assert.Equal(t, []float32{0.1, 0.2, 0.3}, fieldSpec["vector"])
	assert.Equal(t, 10, fieldSpec["k"])
	assert.Equal(t, map[string]any{"term": map[string]any{"tenant_id": "t1"}}, fieldSpec["filter"])
	// OS 的 query.knn 没有 num_candidates 概念，不应出现
	_, hasNumCandidates := fieldSpec["num_candidates"]
	assert.False(t, hasNumCandidates)

	// MinScore 映射为顶层 min_score
	q.MinScore = 0.8
	body, err = buildKnnQueryBody(q)
	require.NoError(t, err)
	assert.InDelta(t, 0.8, body["min_score"], 1e-9)

	// 非法查询
	_, err = buildKnnQueryBody(&vector.Query{Field: "", Vector: []float32{1}, TopK: 1})
	assert.ErrorIs(t, err, ErrInvalidVectorQuery)
}

func TestSearchWithKnn_BodyValidation(t *testing.T) {
	// 不依赖网络的参数校验分支：body 已含 query 时应直接报错
	c := &Client{}
	_, err := c.SearchWithKnn(nil, "docs", map[string]any{"query": map[string]any{}},
		&vector.Query{Field: "embedding", Vector: []float32{1}, TopK: 1})
	assert.ErrorIs(t, err, ErrInvalidRequest)

	// 非法向量查询
	_, err = c.SearchWithKnn(nil, "docs", nil,
		&vector.Query{Field: "", Vector: []float32{1}, TopK: 1})
	assert.ErrorIs(t, err, ErrInvalidVectorQuery)
}

func TestToVectorResult(t *testing.T) {
	const respBody = `{"took":1,"timed_out":false,"hits":{"total":{"value":1,"relation":"eq"},"max_score":0.9,"hits":[{"_index":"docs","_id":"1","_score":0.9,"_source":{"name":"alice"}}]}}`

	var sr opensearchapiV4.SearchResp
	require.NoError(t, json.Unmarshal([]byte(respBody), &sr))

	type doc struct {
		Name string `json:"name"`
	}
	res := ToVectorResult[doc](&sr)
	require.NotNil(t, res)
	require.Len(t, res.Hits, 1)
	assert.Equal(t, "alice", res.Hits[0].Value.Name)
	assert.InDelta(t, 0.9, res.Hits[0].Score, 1e-6)
	assert.Equal(t, int64(1), res.Total)

	// 原始文档透传
	rawRes := ToVectorResult[json.RawMessage](&sr)
	require.Len(t, rawRes.Hits, 1)
	assert.JSONEq(t, `{"name":"alice"}`, string(rawRes.Hits[0].Value))

	assert.Nil(t, ToVectorResult[doc](nil))
}
