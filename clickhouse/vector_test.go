package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tx7do/go-utils/mapper"

	"github.com/tx7do/go-crud/vector"
)

// 向量检索（距离函数暴力检索）测试：SQL 构造与分数重算均为纯函数，可离线断言；
// 真实执行需 ClickHouse 集成环境（见 repository_test.go 门禁约定）。

type testVectorEntity struct {
	ID        int64     `ch:"id"`
	Embedding []float32 `ch:"embedding"`
}

func TestClickhouseDistanceFunc(t *testing.T) {
	fn, err := clickhouseDistanceFunc(vector.MetricCosine)
	require.NoError(t, err)
	assert.Equal(t, "cosineDistance", fn)

	fn, err = clickhouseDistanceFunc(vector.MetricEuclidean)
	require.NoError(t, err)
	assert.Equal(t, "L2Distance", fn)

	fn, err = clickhouseDistanceFunc(vector.MetricDotProduct)
	require.NoError(t, err)
	assert.Equal(t, "dotProduct", fn)

	// 未指定度量按余弦处理
	fn, err = clickhouseDistanceFunc("")
	require.NoError(t, err)
	assert.Equal(t, "cosineDistance", fn)

	_, err = clickhouseDistanceFunc("unknown")
	assert.Error(t, err)
}

func TestBuildVectorSearchSQL(t *testing.T) {
	q := &vector.Query{
		Field:  "embedding",
		Vector: []float32{0.1, 0.2, 0.3},
		TopK:   10,
	}

	// 无过滤条件
	sql, err := buildVectorSearchSQL("vectors", "", q)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM vectors ORDER BY cosineDistance(`embedding`, [0.1,0.2,0.3]) ASC LIMIT 10", sql)

	// 自动补 WHERE 前缀
	sql, err = buildVectorSearchSQL("vectors", "tenant_id = 't1'", q)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM vectors WHERE tenant_id = 't1' ORDER BY cosineDistance(`embedding`, [0.1,0.2,0.3]) ASC LIMIT 10", sql)

	// 已带 WHERE 前缀则不重复添加
	sql, err = buildVectorSearchSQL("vectors", "WHERE tenant_id = 't1'", q)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM vectors WHERE tenant_id = 't1' ORDER BY cosineDistance(`embedding`, [0.1,0.2,0.3]) ASC LIMIT 10", sql)

	// 各度量的函数与排序方向（内积降序，距离升序）
	for metric, want := range map[vector.DistanceMetric]string{
		vector.MetricCosine:     "ORDER BY cosineDistance(`embedding`, [0.1,0.2,0.3]) ASC",
		vector.MetricEuclidean:  "ORDER BY L2Distance(`embedding`, [0.1,0.2,0.3]) ASC",
		vector.MetricDotProduct: "ORDER BY dotProduct(`embedding`, [0.1,0.2,0.3]) DESC",
	} {
		q.Metric = metric
		sql, err = buildVectorSearchSQL("vectors", "", q)
		require.NoError(t, err)
		assert.Contains(t, sql, want)
	}

	// 非法参数
	q.Metric = "unknown"
	_, err = buildVectorSearchSQL("vectors", "", q)
	assert.Error(t, err)
	q.Metric = vector.MetricCosine
	_, err = buildVectorSearchSQL("", "", q)
	assert.Error(t, err)
	_, err = buildVectorSearchSQL("vectors", "", &vector.Query{Field: "", Vector: []float32{1}, TopK: 0})
	assert.Error(t, err)
}

func TestGoDistance(t *testing.T) {
	a := []float32{1, 0, 0}
	b := []float32{0, 1, 0}

	// 余弦：正交向量 cos=0，距离=1；同向 cos=1，距离=0；反向 cos=-1，距离=2
	d, err := goDistance(vector.MetricCosine, a, b)
	require.NoError(t, err)
	assert.InDelta(t, 1.0, d, 1e-9)
	d, err = goDistance(vector.MetricCosine, a, a)
	require.NoError(t, err)
	assert.InDelta(t, 0.0, d, 1e-9)
	d, err = goDistance(vector.MetricCosine, a, []float32{-1, 0, 0})
	require.NoError(t, err)
	assert.InDelta(t, 2.0, d, 1e-9)

	// 未指定度量按余弦处理
	d, err = goDistance("", a, b)
	require.NoError(t, err)
	assert.InDelta(t, 1.0, d, 1e-9)

	// 欧氏距离
	d, err = goDistance(vector.MetricEuclidean, []float32{0, 0}, []float32{3, 4})
	require.NoError(t, err)
	assert.InDelta(t, 5.0, d, 1e-9)

	// 内积
	d, err = goDistance(vector.MetricDotProduct, []float32{1, 2}, []float32{3, 4})
	require.NoError(t, err)
	assert.InDelta(t, 11.0, d, 1e-9)

	// 维度不一致
	_, err = goDistance(vector.MetricCosine, a, []float32{1, 2})
	assert.Error(t, err)
}

func TestEntityVectorField(t *testing.T) {
	entity := &testVectorEntity{ID: 1, Embedding: []float32{1, 2, 3}}

	// 按 ch 标签匹配
	vec, err := entityVectorField(entity, "embedding")
	require.NoError(t, err)
	assert.Equal(t, []float32{1, 2, 3}, vec)

	// 忽略大小写匹配
	_, err = entityVectorField(entity, "Embedding")
	assert.NoError(t, err)

	// 字段不存在
	_, err = entityVectorField(entity, "missing")
	assert.Error(t, err)

	// nil 实体
	var nilEntity *testVectorEntity
	_, err = entityVectorField(nilEntity, "embedding")
	assert.Error(t, err)
}

func TestSearchByVector_Guards(t *testing.T) {
	// nil client（表名校验在 client 之后，此处一并覆盖空表名分支）
	repo := NewRepository[string, testVectorEntity](nil, nil, "", nil)
	_, err := repo.SearchByVector(nil, "", &vector.Query{
		Field: "embedding", Vector: []float32{1}, TopK: 1,
	})
	assert.ErrorContains(t, err, "clickhouse client is nil")

	// 非法查询
	repo = NewRepository[string, testVectorEntity](&Client{}, mapper.NewCopierMapper[string, testVectorEntity](), "vectors", nil)
	_, err = repo.SearchByVector(nil, "", &vector.Query{
		Field: "", Vector: []float32{1}, TopK: 0,
	})
	assert.ErrorContains(t, err, "invalid vector query")

	// 不支持的度量
	_, err = repo.SearchByVector(nil, "", &vector.Query{
		Field: "embedding", Vector: []float32{1}, TopK: 1, Metric: "unknown",
	})
	assert.ErrorContains(t, err, "unsupported vector metric")

	// 注：完整执行路径需 ClickHouse 集成环境
}
