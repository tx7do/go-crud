package milvus

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tx7do/go-utils/mapper"
	"github.com/tx7do/go-wind/log"

	"github.com/tx7do/go-crud/milvus/mixin"
	"github.com/tx7do/go-crud/vector"
	"github.com/tx7do/go-crud/viewer"
)

// guardEntity 用于守卫分支测试的普通实体（非 tenant-scoped）。
type guardEntity struct {
	ID  int64
	Emb []float32
}

// TestRepository_Guards 守卫分支：未初始化客户端 / 空集合名 / nil DTO /
// 空表达式。全部在触碰网络前返回。
func TestRepository_Guards(t *testing.T) {
	ctx := context.Background()
	m := mapper.NewCopierMapper[guardEntity, guardEntity]()
	logger := log.GetLogger()

	nilRepo := NewRepository[guardEntity, guardEntity](nil, "coll", m, logger)
	emptyRepo := NewRepository[guardEntity, guardEntity](&Client{}, "coll", m, logger)
	// 本地（惰性连接）客户端 + 空集合名：集合守卫先于网络访问触发。
	localClient, err := NewClient(WithAddress("localhost:19530"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = localClient.Close() })
	noCollRepo := NewRepository[guardEntity, guardEntity](localClient, "", m, logger)

	// 未初始化客户端（nil 与零值 Client）。
	_, err = nilRepo.Create(ctx, &guardEntity{ID: 1, Emb: []float32{1}})
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	_, err = emptyRepo.Get(ctx, 1)
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	_, err = emptyRepo.GetByUUID(ctx, "u")
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	_, err = emptyRepo.QueryByExpr(ctx, "x == 1")
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	_, err = emptyRepo.Count(ctx, "x == 1")
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	_, err = emptyRepo.SearchByVector(ctx, &vector.Query{Field: "Emb", Vector: []float32{1}, TopK: 1})
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	_, err = emptyRepo.DeleteByExpr(ctx, "x == 1")
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	_, err = emptyRepo.DeleteByIDs(ctx, []uint64{1})
	assert.ErrorIs(t, err, ErrClientNotInitialized)

	// 空集合名。
	_, err = noCollRepo.Create(ctx, &guardEntity{ID: 1, Emb: []float32{1}})
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = noCollRepo.Get(ctx, 1)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = noCollRepo.QueryByExpr(ctx, "x == 1")
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = noCollRepo.Count(ctx, "x == 1")
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = noCollRepo.SearchByVector(ctx, &vector.Query{Field: "Emb", Vector: []float32{1}, TopK: 1})
	assert.ErrorIs(t, err, ErrInvalidRequest)

	// 空 id 列表直接返回。
	n, err := noCollRepo.DeleteByIDs(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), n)

	// nil DTO / 空列表（空集合守卫先于空列表短路，仍为 ErrInvalidRequest；
	// 空列表的 nil,nil 短路仅在线上路径可达）。
	_, err = noCollRepo.Create(ctx, nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	out, err := noCollRepo.BatchCreate(ctx, nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	assert.Nil(t, out)

	// 空表达式（守卫先于注入，无法注入出非空谓词的非租户实体直接拒绝）。
	_, err = noCollRepo.QueryByExpr(ctx, "")
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = noCollRepo.Count(ctx, "  ")
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = noCollRepo.DeleteByExpr(ctx, "")
	assert.ErrorIs(t, err, ErrInvalidRequest)

	// 非法向量检索请求。
	_, err = noCollRepo.SearchByVector(ctx, &vector.Query{Field: "Emb", Vector: nil, TopK: 1})
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = noCollRepo.SearchByVector(ctx, &vector.Query{Field: "nope", Vector: []float32{1}, TopK: 1})
	assert.ErrorIs(t, err, ErrInvalidRequest)
}

// ─────────────────────────────────────────────────────────────────────────────
// 集成测试（KRATOS_IT 门禁）：租户隔离全链路。
// ─────────────────────────────────────────────────────────────────────────────

// itVecEntity 集成测试实体：数值主键 + 标量 + 向量字段 + TenantID mixin。
type itVecEntity struct {
	ID    int64
	Title string
	Emb   []float32
	mixin.TenantID
}

// TestIntegration_TenantIsolation 租户隔离全链路：
// 写入强制落租户 → Count/Get/QueryByExpr/SearchByVector 按租户隔离 →
// 越权删除被过滤。
func TestIntegration_TenantIsolation(t *testing.T) {
	c := createTestClient(t)
	defer c.Close()

	const coll = "go_crud_milvus_test_tenant"
	setupCtx := context.Background()
	_ = c.DropCollection(setupCtx, coll)

	m := mapper.NewCopierMapper[itVecEntity, itVecEntity]()
	repo := NewRepository[itVecEntity, itVecEntity](c, coll, m, log.GetLogger())

	require.NoError(t, repo.CreateCollection(setupCtx, 4, vector.MetricCosine))
	t.Cleanup(func() { _ = c.DropCollection(setupCtx, coll) })

	ctx7 := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	ctx8 := viewer.WithContext(context.Background(), testEnforceViewer{tid: 8})

	// 写入：tenant 强制覆盖。各点使用互异的正交向量，保证检索排序可断言。
	mk := func(id int64, title string, emb []float32) *itVecEntity {
		return &itVecEntity{ID: id, Title: title, Emb: emb}
	}
	e101 := []float32{1, 0, 0, 0}
	created7, err := repo.BatchCreate(ctx7, []*itVecEntity{
		mk(101, "a7", e101),
		mk(102, "b7", []float32{0, 1, 0, 0}),
		mk(103, "c7", []float32{0, 0, 1, 0}),
	})
	require.NoError(t, err)
	require.Len(t, created7, 3)
	for _, dto := range created7 {
		require.NotNil(t, dto.TenantID.GetTenantID())
		assert.Equal(t, uint32(7), *dto.TenantID.GetTenantID(), "create must force tenant 7")
	}
	created8, err := repo.BatchCreate(ctx8, []*itVecEntity{
		mk(201, "a8", []float32{0, 0, 0, 1}),
		mk(202, "b8", []float32{0, 0, 0, 1}),
	})
	require.NoError(t, err)
	require.Len(t, created8, 2)

	// Count：各自租户视角下只见自己的行。
	n7, err := repo.Count(ctx7, "")
	require.NoError(t, err)
	assert.Equal(t, int64(3), n7)
	n8, err := repo.Count(ctx8, "")
	require.NoError(t, err)
	assert.Equal(t, int64(2), n8)

	// Get：本租户行可取回；他租户行与不存在同构（ErrPointNotFound）。
	got, err := repo.Get(ctx7, 101)
	require.NoError(t, err)
	assert.Equal(t, "a7", got.Title)
	_, err = repo.Get(ctx7, 201)
	assert.ErrorIs(t, err, ErrPointNotFound)

	// QueryByExpr：注入的租户谓词限定结果集。
	rows7, err := repo.QueryByExpr(ctx7, "")
	require.NoError(t, err)
	assert.Len(t, rows7, 3)
	rows8, err := repo.QueryByExpr(ctx8, "")
	require.NoError(t, err)
	assert.Len(t, rows8, 2)

	// SearchByVector：租户 7 视角检索，命中只含租户 7 的行；
	// 查询向量取自 101 号行 → 首位命中即 101 号行，余弦自匹配分数接近 1，
	// 其余正交行分数接近 0。
	res, err := repo.SearchByVector(ctx7, &vector.Query{
		Field:  "Emb",
		Vector: e101,
		TopK:   10,
	})
	require.NoError(t, err)
	require.Len(t, res.Hits, 3, "tenant-7 view must see exactly its 3 rows")
	for _, hit := range res.Hits {
		tid := hit.Value.TenantID.GetTenantID()
		require.NotNil(t, tid, "hit must carry tenant column")
		assert.Equal(t, uint32(7), *tid, "search hits must be tenant-7 only")
	}
	assert.Equal(t, int64(101), res.Hits[0].Value.ID)
	assert.GreaterOrEqual(t, res.Hits[0].Score, 0.99)
	for _, hit := range res.Hits[1:] {
		assert.LessOrEqual(t, hit.Score, 0.1, "orthogonal vectors must score near zero")
	}

	// 越权删除：租户 7 试图删租户 8 的行 → 客户端先过滤，0 删除、计数不变。
	deleted, err := repo.DeleteByIDs(ctx7, []uint64{201})
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted)
	n8, err = repo.Count(ctx8, "")
	require.NoError(t, err)
	assert.Equal(t, int64(2), n8)

	// 本租户删除：删自己的行生效。
	deleted, err = repo.DeleteByIDs(ctx7, []uint64{101})
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
	n7, err = repo.Count(ctx7, "")
	require.NoError(t, err)
	assert.Equal(t, int64(2), n7)
}
