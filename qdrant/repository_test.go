package qdrant

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tx7do/go-utils/mapper"
	"github.com/tx7do/go-wind/log"

	"github.com/tx7do/go-crud/qdrant/mixin"
	"github.com/tx7do/go-crud/vector"
	"github.com/tx7do/go-crud/viewer"
)

// guardEntity 用于守卫分支测试的普通实体（非 tenant-scoped）。
type guardEntity struct {
	ID  uint64
	Emb []float32
}

// TestRepository_Guards 守卫分支：未初始化客户端 / 空集合名 / nil DTO /
// nil Filter。全部在触碰网络前返回。
func TestRepository_Guards(t *testing.T) {
	ctx := context.Background()
	m := mapper.NewCopierMapper[guardEntity, guardEntity]()
	logger := log.GetLogger()

	nilRepo := NewRepository[guardEntity, guardEntity](nil, "coll", m, logger)
	emptyRepo := NewRepository[guardEntity, guardEntity](&Client{}, "coll", m, logger)
	localClient, err := NewClient(WithHost("localhost"), WithPort(6334))
	require.NoError(t, err)
	t.Cleanup(func() { _ = localClient.Close() })
	noCollRepo := NewRepository[guardEntity, guardEntity](localClient, "", m, logger)

	// 未初始化客户端（nil 与零值 Client）。
	_, err = nilRepo.Create(ctx, &guardEntity{ID: 1, Emb: []float32{1}})
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	_, err = emptyRepo.Get(ctx, 1)
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	_, err = emptyRepo.Count(ctx, nil)
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	_, err = emptyRepo.SearchByVector(ctx, &vector.Query{Field: "e", Vector: []float32{1}, TopK: 1})
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	_, err = emptyRepo.DeleteByFilter(ctx, nil)
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	_, err = emptyRepo.DeleteByIDs(ctx, []uint64{1})
	assert.ErrorIs(t, err, ErrClientNotInitialized)

	// 空集合名。
	_, err = noCollRepo.Create(ctx, &guardEntity{ID: 1, Emb: []float32{1}})
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = noCollRepo.Get(ctx, 1)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = noCollRepo.GetByUUID(ctx, "u")
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = noCollRepo.Count(ctx, nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = noCollRepo.Exists(ctx, nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = noCollRepo.SearchByVector(ctx, &vector.Query{Field: "e", Vector: []float32{1}, TopK: 1})
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = noCollRepo.DeleteByFilter(ctx, nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = noCollRepo.DeleteByIDs(ctx, []uint64{1})
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = noCollRepo.DeleteByUUIDs(ctx, []string{"u"})
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = noCollRepo.BatchCreate(ctx, nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)

	// 未初始化客户端：同一批入口。
	_, err = emptyRepo.GetByUUID(ctx, "u")
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	_, err = emptyRepo.DeleteByUUIDs(ctx, []string{"u"})
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	_, err = emptyRepo.Exists(ctx, nil)
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	_, err = emptyRepo.BatchCreate(ctx, []*guardEntity{})
	assert.ErrorIs(t, err, ErrClientNotInitialized)

	// nil DTO（离线可达的路径：集合名校验先于 DTO 校验，两者均为
	// ErrInvalidRequest；DTO 校验分支见 Create 的 nil 检查）。
	_, err = noCollRepo.Create(ctx, nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)

	// DeleteByFilter 必须显式给出 Filter（客户端守卫先于 Filter 守卫）。
	_, err = emptyRepo.DeleteByFilter(ctx, nil)
	assert.ErrorIs(t, err, ErrClientNotInitialized)
}

// ─────────────────────────────────────────────────────────────────────────────
// 集成测试（KRATOS_IT 门禁）：租户隔离全链路。
// ─────────────────────────────────────────────────────────────────────────────

// itVecEntity 集成测试实体：数值 ID + 向量字段 + TenantID mixin。
type itVecEntity struct {
	ID    uint64
	Title string
	Emb   []float32
	mixin.TenantID
}

// TestIntegration_TenantIsolation 租户隔离全链路：
// 写入强制落租户 → Count/Get/SearchByVector 按租户隔离 → 越权删除被过滤。
func TestIntegration_TenantIsolation(t *testing.T) {
	c := createTestClient(t)
	defer c.Close()

	const coll = "go_crud_qdrant_test_tenant"
	setupCtx := context.Background()
	_ = c.DropCollection(setupCtx, coll)
	require.NoError(t, c.CreateVectorCollection(setupCtx, coll, 4, vector.MetricCosine))
	require.NoError(t, c.CreatePayloadIndex(setupCtx, coll, "tenant_id", PayloadIndexInteger))
	t.Cleanup(func() { _ = c.DropCollection(setupCtx, coll) })

	m := mapper.NewCopierMapper[itVecEntity, itVecEntity]()
	repo := NewRepository[itVecEntity, itVecEntity](c, coll, m, log.GetLogger())

	ctx7 := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	ctx8 := viewer.WithContext(context.Background(), testEnforceViewer{tid: 8})

	// 写入：tenant 强制覆盖（ctx7 下写的三条属于租户 7，ctx8 下两条属于租户 8）。
	// 各点使用互异的正交向量，保证检索排序可断言（自匹配恒为首位）。
	mk := func(id uint64, title string, emb []float32) *itVecEntity {
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

	// Count：各自租户视角下只见自己的点。
	n7, err := repo.Count(ctx7, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(3), n7)
	n8, err := repo.Count(ctx8, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n8)

	// Get：本租户点可取回；他租户点与不存在同构（ErrPointNotFound）。
	got, err := repo.Get(ctx7, 101)
	require.NoError(t, err)
	assert.Equal(t, "a7", got.Title)
	_, err = repo.Get(ctx7, 201)
	assert.ErrorIs(t, err, ErrPointNotFound)

	// SearchByVector：租户 7 视角检索，命中只含租户 7 的点；
	// 查询向量取自 101 号点 → 首位命中即 101 号点，余弦自匹配分数接近 1，
	// 其余正交点分数接近 0。
	res, err := repo.SearchByVector(ctx7, &vector.Query{
		Vector: e101,
		TopK:   10,
	})
	require.NoError(t, err)
	require.Len(t, res.Hits, 3, "tenant-7 view must see exactly its 3 points")
	for _, hit := range res.Hits {
		tid := hit.Value.TenantID.GetTenantID()
		require.NotNil(t, tid, "hit must carry tenant payload")
		assert.Equal(t, uint32(7), *tid, "search hits must be tenant-7 only")
	}
	assert.Equal(t, uint64(101), res.Hits[0].Value.ID)
	assert.GreaterOrEqual(t, res.Hits[0].Score, 0.99)
	for _, hit := range res.Hits[1:] {
		assert.LessOrEqual(t, hit.Score, 0.1, "orthogonal vectors must score near zero")
	}

	// 越权删除：租户 7 试图删租户 8 的点 → 客户端先过滤，0 删除、计数不变。
	deleted, err := repo.DeleteByIDs(ctx7, []uint64{201})
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted)
	n8, err = repo.Count(ctx8, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n8)

	// 本租户删除：删自己的点生效。
	deleted, err = repo.DeleteByIDs(ctx7, []uint64{101})
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
	n7, err = repo.Count(ctx7, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n7)
}
