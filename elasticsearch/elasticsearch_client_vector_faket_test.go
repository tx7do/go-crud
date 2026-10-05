package elasticsearch

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tx7do/go-crud/vector"
)

// 向量检索（kNN）测试：基于 fake transport，全程不碰真实集群。

func TestBuildDenseVectorMapping(t *testing.T) {
	mapping, err := BuildDenseVectorMapping("embedding", 768, vector.MetricCosine)
	require.NoError(t, err)

	var parsed map[string]any
	require.NoError(t, json.Unmarshal([]byte(mapping), &parsed))
	props := parsed["mappings"].(map[string]any)["properties"].(map[string]any)["embedding"].(map[string]any)
	assert.Equal(t, "dense_vector", props["type"])
	assert.InEpsilon(t, 768.0, props["dims"], 1e-9)
	assert.Equal(t, true, props["index"])
	assert.Equal(t, "cosine", props["similarity"])

	// 度量枚举映射
	for metric, want := range map[vector.DistanceMetric]string{
		vector.MetricCosine:     "cosine",
		vector.MetricEuclidean:  "l2_norm",
		vector.MetricDotProduct: "dot_product",
	} {
		m, err := BuildDenseVectorMapping("v", 3, metric)
		require.NoError(t, err)
		assert.Contains(t, m, want)
	}

	// 非法参数
	_, err = BuildDenseVectorMapping("", 3, vector.MetricCosine)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = BuildDenseVectorMapping("v", 0, vector.MetricCosine)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = BuildDenseVectorMapping("v", 3, "unknown")
	assert.Error(t, err)
}

func TestKnnSearch_Fake(t *testing.T) {
	ctx := context.Background()
	c, ft := newFakeESClient(t)
	ft.enqueue(200, searchResultBody)

	q := &vector.Query{
		Field:         "embedding",
		Vector:        []float32{0.1, 0.2, 0.3},
		TopK:          10,
		NumCandidates: 100,
		Filter: map[string]any{
			"bool": map[string]any{"filter": map[string]any{"term": map[string]any{"tenant_id": "t1"}}},
		},
	}
	res, err := c.KnnSearch(ctx, "docs", q)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, 1, len(res.Hits.Hits))

	// 请求路径与 knn 子句
	assert.Equal(t, "/docs/_search", ft.lastPath())
	body := ft.lastRequestBody()
	assert.Contains(t, body, `"knn"`)
	assert.Contains(t, body, `"field":"embedding"`)
	assert.Contains(t, body, `"query_vector":[0.1,0.2,0.3]`)
	assert.Contains(t, body, `"k":10`)
	assert.Contains(t, body, `"num_candidates":100`)
	assert.Contains(t, body, `"tenant_id":"t1"`)

	// NumCandidates 未指定时按 TopK 放大默认值
	ft.enqueue(200, searchResultBody)
	_, err = c.KnnSearch(ctx, "docs", &vector.Query{Field: "embedding", Vector: []float32{1}, TopK: 7})
	require.NoError(t, err)
	assert.Contains(t, ft.lastRequestBody(), `"num_candidates":70`)

	// MinScore 映射为 similarity
	ft.enqueue(200, searchResultBody)
	_, err = c.KnnSearch(ctx, "docs", &vector.Query{Field: "embedding", Vector: []float32{1}, TopK: 7, MinScore: 0.8})
	require.NoError(t, err)
	assert.Contains(t, ft.lastRequestBody(), `"similarity":0.8`)

	// 非法查询
	_, err = c.KnnSearch(ctx, "docs", &vector.Query{Field: "", Vector: []float32{1}, TopK: 1})
	assert.ErrorIs(t, err, ErrInvalidVectorQuery)
}

func TestSearchWithKnn_Fake(t *testing.T) {
	ctx := context.Background()
	c, ft := newFakeESClient(t)
	ft.enqueue(200, searchResultBody)

	// 混合检索：query + knn 同体
	body := map[string]any{
		"query": map[string]any{
			"match": map[string]any{"title": "hello"},
		},
		"size": 20,
	}
	res, err := c.SearchWithKnn(ctx, "docs", body, &vector.Query{Field: "embedding", Vector: []float32{1, 2}, TopK: 5})
	require.NoError(t, err)
	require.NotNil(t, res)

	sent := ft.lastRequestBody()
	assert.Contains(t, sent, `"match"`)
	assert.Contains(t, sent, `"knn"`)
	assert.Contains(t, sent, `"size":20`)

	// body 中已含 knn 时报错，避免静默覆盖
	_, err = c.SearchWithKnn(ctx, "docs", map[string]any{"knn": map[string]any{}},
		&vector.Query{Field: "embedding", Vector: []float32{1}, TopK: 1})
	assert.ErrorIs(t, err, ErrInvalidRequest)

	// 服务器报错透传
	ft.enqueue(500, esErrorBody)
	_, err = c.SearchWithKnn(ctx, "docs", nil, &vector.Query{Field: "embedding", Vector: []float32{1}, TopK: 1})
	assert.ErrorIs(t, err, ErrSearchDocument)
}

func TestCreateVectorIndex_Fake(t *testing.T) {
	ctx := context.Background()
	c, ft := newFakeESClient(t)

	// IndexExists 先返回 404（不存在），Create 返回成功
	ft.enqueue(404, notFoundBody)
	ft.enqueue(200, okESBody)

	require.NoError(t, c.CreateVectorIndex(ctx, "docs", "embedding", 768, vector.MetricCosine))

	// 第二次请求（PUT）的 body 应含 dense_vector mapping
	paths := []string{}
	ft.mu.Lock()
	for _, r := range ft.requests {
		paths = append(paths, r.Method+" "+r.URL.Path)
	}
	createBody := ft.bodies[len(ft.bodies)-1]
	ft.mu.Unlock()
	assert.Contains(t, paths[len(paths)-2], "HEAD")
	assert.Contains(t, paths[len(paths)-1], "PUT")
	assert.True(t, strings.Contains(createBody, "dense_vector"))
}

func TestToVectorResult(t *testing.T) {
	var sr SearchResult
	require.NoError(t, json.Unmarshal([]byte(searchResultBody), &sr))

	type doc struct {
		Name string `json:"name"`
	}
	res := ToVectorResult[doc](&sr)
	require.NotNil(t, res)
	require.Len(t, res.Hits, 1)
	assert.Equal(t, "alice", res.Hits[0].Value.Name)
	assert.InDelta(t, 1.0, res.Hits[0].Score, 1e-9)
	assert.Equal(t, int64(1), res.Total)

	// 原始文档透传
	rawRes := ToVectorResult[json.RawMessage](&sr)
	require.Len(t, rawRes.Hits, 1)
	assert.JSONEq(t, `{"name":"alice"}`, string(rawRes.Hits[0].Value))

	assert.Nil(t, ToVectorResult[doc](nil))
}
