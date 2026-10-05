package weaviate

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tx7do/go-utils/mapper"
	"github.com/tx7do/go-wind/log"

	wvFilters "github.com/weaviate/weaviate-go-client/v4/weaviate/filters"

	"github.com/tx7do/go-crud/vector"
	"github.com/tx7do/go-crud/viewer"
	"github.com/tx7do/go-crud/weaviate/mixin"
)

// guardEntity 用于守卫分支测试的普通实体（非 tenant-scoped）。
type guardEntity struct {
	UUID  string
	Title string
	Emb   []float32
}

// TestRepository_Guards 守卫分支：未初始化客户端 / 空集合名 / nil DTO /
// nil Filter / 数值 ID 通道。全部在触碰网络前返回。
func TestRepository_Guards(t *testing.T) {
	ctx := context.Background()
	m := mapper.NewCopierMapper[guardEntity, guardEntity]()
	logger := log.GetLogger()

	nilRepo := NewRepository[guardEntity, guardEntity](nil, "Coll", m, logger)
	emptyRepo := NewRepository[guardEntity, guardEntity](&Client{}, "Coll", m, logger)
	for _, repo := range []*Repository[guardEntity, guardEntity]{nilRepo, emptyRepo} {
		_, err := repo.Create(ctx, &guardEntity{Title: "x", Emb: []float32{1}})
		assert.ErrorIs(t, err, ErrClientNotInitialized)
		_, err = repo.BatchCreate(ctx, []*guardEntity{{Title: "x", Emb: []float32{1}}})
		assert.ErrorIs(t, err, ErrClientNotInitialized)
		_, err = repo.GetByUUID(ctx, "u")
		assert.ErrorIs(t, err, ErrClientNotInitialized)
		_, err = repo.Query(ctx, nil, 0)
		assert.ErrorIs(t, err, ErrClientNotInitialized)
		_, err = repo.Count(ctx, nil)
		assert.ErrorIs(t, err, ErrClientNotInitialized)
		_, err = repo.Exists(ctx, nil)
		assert.ErrorIs(t, err, ErrClientNotInitialized)
		_, err = repo.DeleteByUUIDs(ctx, []string{"u"})
		assert.ErrorIs(t, err, ErrClientNotInitialized)
		_, err = repo.DeleteByFilter(ctx, wvFilters.Where())
		assert.ErrorIs(t, err, ErrClientNotInitialized)
		_, err = repo.SearchByVector(ctx, &vector.Query{Vector: []float32{1}, TopK: 1})
		assert.ErrorIs(t, err, ErrClientNotInitialized)
		err = repo.CreateCollection(ctx, vector.MetricCosine)
		assert.ErrorIs(t, err, ErrClientNotInitialized)
	}

	// 注入替身的客户端：请求级守卫。
	ft := &fakeTransport{}
	localClient, err := NewClient(WithHost("localhost:8080"), WithHTTPClient(&http.Client{Transport: ft}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = localClient.Close() })
	noCollRepo := NewRepository[guardEntity, guardEntity](localClient, "", m, logger)

	_, err = noCollRepo.Create(ctx, &guardEntity{Title: "x", Emb: []float32{1}})
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = noCollRepo.GetByUUID(ctx, "u")
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = noCollRepo.Query(ctx, nil, 0)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = noCollRepo.Count(ctx, nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)

	// 数值 ID 通道不存在（对象身份是 UUID 字符串）。
	_, repo2 := newFakeRepo[guardEntity](t)
	_, err = repo2.Get(ctx, 1)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = repo2.DeleteByIDs(ctx, []uint64{1})
	assert.ErrorIs(t, err, ErrInvalidRequest)

	// nil DTO / 空 UUID / 空 Filter。
	_, err = repo2.Create(ctx, nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = repo2.GetByUUID(ctx, "")
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = repo2.DeleteByFilter(ctx, nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = repo2.SearchByVector(ctx, nil)
	assert.ErrorIs(t, err, ErrInvalidVectorQuery)

	// 空批量直接返回（无写请求发出；NewClient 构造期的 /v1/meta 版本探测
	// 不计入）。
	outs, err := repo2.BatchCreate(ctx, nil)
	assert.NoError(t, err)
	assert.Nil(t, outs)
	empty, err := repo2.DeleteByUUIDs(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), empty)
	for _, req := range ft.requests {
		assert.NotEqual(t, "/v1/batch/objects", req.path, "empty batch must not issue writes")
		assert.NotEqual(t, "/v1/batch/delete", req.path, "empty delete must not issue writes")
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 集成测试（KRATOS_IT 门禁）：租户隔离全链路。
// ─────────────────────────────────────────────────────────────────────────────

// itDocEntity 集成测试实体：UUID 通道 + 标量/列表 + 向量字段 + TenantID mixin。
type itDocEntity struct {
	UUID  string    `json:"-"`
	Title string    `json:"title"`
	Age   int64     `json:"age"`
	Tags  []string  `json:"tags"`
	Emb   []float32 `json:"-"`
	mixin.TenantID
}

// TestIntegration_TenantIsolation 租户隔离全链路：
// 写入强制落租户与 UUID 回读 → Count/Query 按租户隔离 → GetByUUID 越权
// 同构未找到 → 越权删除被过滤 → 向量检索租户限定。
func TestIntegration_TenantIsolation(t *testing.T) {
	c := createTestClient(t)
	defer func() { _ = c.Close() }()

	const coll = "GoCrudItDoc"
	setupCtx := context.Background()
	_ = c.DropCollection(setupCtx, coll)

	m := mapper.NewCopierMapper[itDocEntity, itDocEntity]()
	repo := NewRepository[itDocEntity, itDocEntity](c, coll, m, log.GetLogger())

	require.NoError(t, repo.CreateCollection(setupCtx, vector.MetricCosine))
	t.Cleanup(func() { _ = c.DropCollection(setupCtx, coll) })

	ctx7 := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	ctx8 := viewer.WithContext(context.Background(), testEnforceViewer{tid: 8})
	platformCtx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 0, platform: true})

	// 写入：tenant 强制覆盖（ctx7 下写的三条属于租户 7，ctx8 下两条属于
	// 租户 8）。各对象使用互异的正交向量，保证检索排序可断言。
	mk := func(title string, emb []float32) *itDocEntity {
		return &itDocEntity{Title: title, Age: 1, Tags: []string{"t"}, Emb: emb}
	}
	e101 := []float32{1, 0, 0, 0}
	created7, err := repo.BatchCreate(ctx7, []*itDocEntity{
		mk("a7", e101),
		mk("b7", []float32{0, 1, 0, 0}),
		mk("c7", []float32{0, 0, 1, 0}),
	})
	require.NoError(t, err)
	require.Len(t, created7, 3)
	for _, dto := range created7 {
		require.NotEmpty(t, dto.UUID, "created objects must carry back their UUIDs")
		require.NotNil(t, dto.TenantID.GetTenantID())
		assert.Equal(t, uint32(7), *dto.TenantID.GetTenantID(), "create must force tenant 7")
	}
	created8, err := repo.BatchCreate(ctx8, []*itDocEntity{
		mk("a8", []float32{0, 0, 0, 1}),
		mk("b8", []float32{0, 0, 0, 1}),
	})
	require.NoError(t, err)
	require.Len(t, created8, 2)

	// Count：各自租户视角下只见自己的对象。
	n7, err := repo.Count(ctx7, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(3), n7)
	n8, err := repo.Count(ctx8, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n8)

	// Query：租户视角只取回本租户对象。
	rows7, err := repo.Query(ctx7, nil, 0)
	require.NoError(t, err)
	require.Len(t, rows7, 3, "tenant-7 view must see exactly its 3 objects")
	for _, r := range rows7 {
		tid := r.TenantID.GetTenantID()
		require.NotNil(t, tid, "rows must carry tenant payload")
		assert.Equal(t, uint32(7), *tid, "rows must be tenant-7 only")
		assert.NotEmpty(t, r.UUID)
	}

	// GetByUUID：本租户对象可取回；他租户对象与不存在同构。
	got, err := repo.GetByUUID(ctx7, created7[0].UUID)
	require.NoError(t, err)
	assert.Equal(t, "a7", got.Title)
	_, err = repo.GetByUUID(ctx7, created8[0].UUID)
	assert.ErrorIs(t, err, ErrPointNotFound)

	// 向量检索：租户 7 视角检索，命中只含租户 7 的对象；查询向量取自
	// a7 → 首位命中即 a7，cosine 距离换算后自匹配分数接近 1。
	res, err := repo.SearchByVector(ctx7, &vector.Query{
		Vector: e101,
		TopK:   10,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.Hits)
	for _, hit := range res.Hits {
		tid := hit.Value.TenantID.GetTenantID()
		require.NotNil(t, tid, "hits must carry tenant payload")
		assert.Equal(t, uint32(7), *tid, "search hits must be tenant-7 only")
	}
	assert.Equal(t, "a7", res.Hits[0].Value.Title)
	assert.GreaterOrEqual(t, res.Hits[0].Score, 0.99)

	// 越权删除：租户 7 试图删租户 8 的对象 → 0 删除、计数不变。
	deleted, err := repo.DeleteByUUIDs(ctx7, []string{created8[0].UUID})
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted)
	n8, err = repo.Count(ctx8, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n8)

	// 本租户删除：删自己的对象生效。
	deleted, err = repo.DeleteByUUIDs(ctx7, []string{created7[0].UUID})
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
	n7, err = repo.Count(ctx7, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n7)

	// 平台视图全量清场后计数归零。
	rowsAll, err := repo.Query(platformCtx, nil, 0)
	require.NoError(t, err)
	ids := make([]string, 0, len(rowsAll))
	for _, r := range rowsAll {
		ids = append(ids, r.UUID)
	}
	if len(ids) > 0 {
		_, err = repo.DeleteByUUIDs(platformCtx, ids)
		require.NoError(t, err)
	}
	nAll, err := repo.Count(platformCtx, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), nAll)
}
