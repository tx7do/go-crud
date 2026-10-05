package doris

import (
	"context"
	"regexp"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tx7do/go-utils/mapper"
	"github.com/tx7do/go-wind/log"

	"github.com/tx7do/go-crud/vector"
)

// 向量检索（距离函数）测试：SQL 构造为纯函数；执行路径用 sqlmock 全离线覆盖，
// 向量列以 Doris MySQL 协议的 "[1,2,3]" 文本返回，走 Float32Vector 的 Scanner。

type testVectorEntity struct {
	ID        uint                 `db:"id"`
	Name      string               `db:"name"`
	Embedding vector.Float32Vector `db:"embedding"`
}

func TestDorisDistanceFunc(t *testing.T) {
	fn, err := dorisDistanceFunc(vector.MetricCosine)
	require.NoError(t, err)
	assert.Equal(t, "cosine_distance", fn)

	fn, err = dorisDistanceFunc(vector.MetricEuclidean)
	require.NoError(t, err)
	assert.Equal(t, "l2_distance", fn)

	fn, err = dorisDistanceFunc(vector.MetricDotProduct)
	require.NoError(t, err)
	assert.Equal(t, "inner_product", fn)

	// 未指定度量按余弦处理
	fn, err = dorisDistanceFunc("")
	require.NoError(t, err)
	assert.Equal(t, "cosine_distance", fn)

	_, err = dorisDistanceFunc("unknown")
	assert.Error(t, err)
}

func TestBuildVectorSearchSQL(t *testing.T) {
	q := &vector.Query{
		Field:  "embedding",
		Vector: []float32{0.1, 0.2, 0.3},
		TopK:   10,
	}

	// 无过滤条件
	sql, err := buildVectorSearchSQL("nodes", "", q)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM nodes ORDER BY cosine_distance(`embedding`, [0.1,0.2,0.3]) ASC LIMIT 10", sql)

	// 自动补 WHERE 前缀
	sql, err = buildVectorSearchSQL("nodes", "tenant_id = ?", q)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM nodes WHERE tenant_id = ? ORDER BY cosine_distance(`embedding`, [0.1,0.2,0.3]) ASC LIMIT 10", sql)

	// 已带 WHERE 前缀则不重复添加
	sql, err = buildVectorSearchSQL("nodes", "WHERE tenant_id = ?", q)
	require.NoError(t, err)
	assert.NotContains(t, sql, "WHERE WHERE")

	// 各度量的函数与排序方向（内积降序，距离升序）
	for metric, want := range map[vector.DistanceMetric]string{
		vector.MetricCosine:     "ORDER BY cosine_distance(`embedding`, [0.1,0.2,0.3]) ASC",
		vector.MetricEuclidean:  "ORDER BY l2_distance(`embedding`, [0.1,0.2,0.3]) ASC",
		vector.MetricDotProduct: "ORDER BY inner_product(`embedding`, [0.1,0.2,0.3]) DESC",
	} {
		q.Metric = metric
		sql, err = buildVectorSearchSQL("nodes", "", q)
		require.NoError(t, err)
		assert.Contains(t, sql, want)
		assert.Contains(t, sql, "LIMIT 10")
	}

	// 非法参数
	q.Metric = "unknown"
	_, err = buildVectorSearchSQL("nodes", "", q)
	assert.Error(t, err)
	q.Metric = vector.MetricCosine
	_, err = buildVectorSearchSQL("", "", q)
	assert.Error(t, err)
	_, err = buildVectorSearchSQL("nodes", "", &vector.Query{Field: "", Vector: nil, TopK: 0})
	assert.Error(t, err)
}

func TestRepositoryMock_SearchByVector(t *testing.T) {
	// 向量实体测试需自建仓库（newMockRepo 固定使用 NoDeleted 实体）
	client, mock, cleanup := newMockClient(t)
	defer cleanup()
	repo := NewRepository[testVectorEntity, testVectorEntity](
		client, mapper.NewCopierMapper[testVectorEntity, testVectorEntity](), "nodes", log.GetLogger())
	ctx := context.Background()

	// cosine：执行距离排序，分数在 Go 侧按回读向量重算（score = 1 - cos 距离）
	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT * FROM nodes WHERE tenant_id = ? ORDER BY cosine_distance(`embedding`, [0.1,0.2,0.3]) ASC LIMIT 10")).
		WithArgs("t1").
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "embedding"}).
			AddRow(1, "doc-a", "[0.1,0.2,0.3]").    // 与查询向量同向 → 距离 0 → score 1
			AddRow(2, "doc-b", "[-0.1,-0.2,-0.3]")) // 反向 → 距离 2 → score -1

	res, err := repo.SearchByVector(ctx, "tenant_id = ?", &vector.Query{
		Field:  "embedding",
		Vector: []float32{0.1, 0.2, 0.3},
		TopK:   10,
	}, "t1")
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, int64(2), res.Total)
	require.Len(t, res.Hits, 2)
	assert.Equal(t, "doc-a", res.Hits[0].Value.Name)
	assert.InDelta(t, 1.0, res.Hits[0].Score, 1e-9)
	assert.InDelta(t, -1.0, res.Hits[1].Score, 1e-9)
	// 向量列经 Float32Vector.Scanner 以文本格式回读
	assert.Equal(t, vector.Float32Vector{0.1, 0.2, 0.3}, repoMustEmbedding(t, res.Hits[0].Value))

	// 内积：降序取近邻，score = inner_product 本身
	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT * FROM nodes ORDER BY inner_product(`embedding`, [1,2,3]) DESC LIMIT 5")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name", "embedding"}).
			AddRow(3, "doc-c", "[1,2,3]"))
	res, err = repo.SearchByVector(ctx, "", &vector.Query{
		Field:  "embedding",
		Vector: []float32{1, 2, 3},
		TopK:   5,
		Metric: vector.MetricDotProduct,
	})
	require.NoError(t, err)
	require.Len(t, res.Hits, 1)
	assert.InDelta(t, 14.0, res.Hits[0].Score, 1e-9) // 1*1 + 2*2 + 3*3

	// 查询失败透传
	mock.ExpectQuery(regexp.QuoteMeta(
		"SELECT * FROM nodes ORDER BY cosine_distance(`embedding`, [1]) ASC LIMIT 1")).
		WillReturnError(assert.AnError)
	_, err = repo.SearchByVector(ctx, "", &vector.Query{Field: "embedding", Vector: []float32{1}, TopK: 1})
	assert.Error(t, err)
}

// repoMustEmbedding 从返回的 DTO 中取出嵌入向量（DTO 与实体同构的测试场景直接断言）。
func repoMustEmbedding(t *testing.T, value any) vector.Float32Vector {
	t.Helper()
	entity, ok := value.(*testVectorEntity)
	require.True(t, ok, "DTO 应与实体同构以携带向量字段")
	return entity.Embedding
}

func TestRepositoryMock_SearchByVector_Guards(t *testing.T) {
	repo, _, _, cleanup := newMockRepo(t, "nodes")
	defer cleanup()
	ctx := context.Background()

	// 非法查询
	_, err := repo.SearchByVector(ctx, "", &vector.Query{Field: "", Vector: nil, TopK: 0})
	assert.ErrorContains(t, err, "invalid vector query")

	// 不支持的度量
	_, err = repo.SearchByVector(ctx, "", &vector.Query{
		Field: "embedding", Vector: []float32{1}, TopK: 1, Metric: "unknown",
	})
	assert.ErrorContains(t, err, "unsupported vector metric")
}
