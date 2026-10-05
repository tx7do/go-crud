package neo4j

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tx7do/go-utils/mapper"
	"github.com/tx7do/go-wind/log"

	"github.com/tx7do/go-crud/neo4j/mixin"
	"github.com/tx7do/go-crud/viewer"
)

// guardEntity 用于守卫分支测试的普通实体（非 tenant-scoped）。
type guardEntity struct {
	UUID string
	Name string
}

// TestRepository_Guards 守卫分支：未初始化客户端 / 空或含反引号标签 /
// nil DTO / 空 element id / 数值 ID 通道 / 空 WHERE 删除。全部在触碰
// 网络前返回。
func TestRepository_Guards(t *testing.T) {
	ctx := context.Background()
	m := mapper.NewCopierMapper[guardEntity, guardEntity]()
	logger := log.GetLogger()

	// 未初始化客户端（nil 与零值 Client）。
	nilRepo := NewRepository[guardEntity, guardEntity](nil, "coll", m, logger)
	emptyClient := &Client{}
	emptyRepo := NewRepository[guardEntity, guardEntity](emptyClient, "coll", m, logger)
	for _, repo := range []*Repository[guardEntity, guardEntity]{nilRepo, emptyRepo} {
		_, err := repo.Create(ctx, &guardEntity{Name: "x"})
		assert.ErrorIs(t, err, ErrClientNotInitialized)
		_, err = repo.BatchCreate(ctx, []*guardEntity{{Name: "x"}})
		assert.ErrorIs(t, err, ErrClientNotInitialized)
		_, err = repo.GetByUUID(ctx, "u")
		assert.ErrorIs(t, err, ErrClientNotInitialized)
		_, err = repo.Query(ctx, nil)
		assert.ErrorIs(t, err, ErrClientNotInitialized)
		_, err = repo.Count(ctx, nil)
		assert.ErrorIs(t, err, ErrClientNotInitialized)
		_, err = repo.Exists(ctx, nil)
		assert.ErrorIs(t, err, ErrClientNotInitialized)
		_, err = repo.DeleteByUUIDs(ctx, []string{"u"})
		assert.ErrorIs(t, err, ErrClientNotInitialized)
		_, err = repo.DeleteByWhere(ctx, &Query{Where: "n.age > 1"})
		assert.ErrorIs(t, err, ErrClientNotInitialized)
	}

	// 注入替身的客户端：空标签 / 含反引号标签（防 Cypher 标签注入）。
	localClient, err := NewClient(WithNeo4jDriver(&fakeDriver{}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = localClient.Close() })
	noLabelRepo := NewRepository[guardEntity, guardEntity](localClient, "", m, logger)
	backtickRepo := NewRepository[guardEntity, guardEntity](localClient, "bad`x", m, logger)
	for _, repo := range []*Repository[guardEntity, guardEntity]{noLabelRepo, backtickRepo} {
		_, err = repo.Create(ctx, &guardEntity{Name: "x"})
		assert.ErrorIs(t, err, ErrInvalidRequest)
		_, err = repo.BatchCreate(ctx, []*guardEntity{{Name: "x"}})
		assert.ErrorIs(t, err, ErrInvalidRequest)
		_, err = repo.GetByUUID(ctx, "u")
		assert.ErrorIs(t, err, ErrInvalidRequest)
		_, err = repo.Query(ctx, nil)
		assert.ErrorIs(t, err, ErrInvalidRequest)
		_, err = repo.Count(ctx, nil)
		assert.ErrorIs(t, err, ErrInvalidRequest)
		_, err = repo.Exists(ctx, nil)
		assert.ErrorIs(t, err, ErrInvalidRequest)
		_, err = repo.DeleteByUUIDs(ctx, []string{"u"})
		assert.ErrorIs(t, err, ErrInvalidRequest)
		_, err = repo.DeleteByWhere(ctx, &Query{Where: "n.age > 1"})
		assert.ErrorIs(t, err, ErrInvalidRequest)
	}

	// 有效仓库上的请求级守卫。
	d, repo := newFakeRepo[guardEntity](t)
	_ = d

	// nil DTO / 空 element id。
	_, err = repo.Create(ctx, nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = repo.GetByUUID(ctx, "")
	assert.ErrorIs(t, err, ErrInvalidRequest)

	// 数值 ID 通道不存在（element id 为字符串）。
	_, err = repo.Get(ctx, 1)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = repo.DeleteByIDs(ctx, []uint64{1})
	assert.ErrorIs(t, err, ErrInvalidRequest)

	// DeleteByWhere 必须显式给出 WHERE（nil 与空串），防误删全标签。
	_, err = repo.DeleteByWhere(ctx, nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = repo.DeleteByWhere(ctx, &Query{})
	assert.ErrorIs(t, err, ErrInvalidRequest)

	// 空批量直接返回（无语句执行）。
	outs, err := repo.BatchCreate(ctx, nil)
	assert.NoError(t, err)
	assert.Nil(t, outs)
	outs, err = repo.BatchCreate(ctx, []*guardEntity{})
	assert.NoError(t, err)
	assert.Nil(t, outs)
	empty, err := repo.DeleteByUUIDs(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), empty)
}

// ─────────────────────────────────────────────────────────────────────────────
// 集成测试（KRATOS_IT 门禁）：租户隔离全链路。
// ─────────────────────────────────────────────────────────────────────────────

// itEntity 集成测试实体：身份通道字段 + 标量/列表（各通道均经真实
// 服务端往返；Neo4j 属性无映射类型，故不含映射字段）+ TenantID mixin。
type itEntity struct {
	UUID string
	Name string
	Age  int64
	Tags []string
	mixin.TenantID
}

// TestIntegration_TenantIsolation 租户隔离全链路：
// 写入强制落租户 → Count/Query 按租户隔离 → GetByUUID 越权同构未找到 →
// 越权删除被预计数拦截。
func TestIntegration_TenantIsolation(t *testing.T) {
	c := createTestClient(t)
	defer func() { _ = c.Close() }()

	const label = "GoCrudItNode"
	m := mapper.NewCopierMapper[itEntity, itEntity]()
	repo := NewRepository[itEntity, itEntity](c, label, m, log.GetLogger())

	platformCtx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 0, platform: true})
	ctx7 := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	ctx8 := viewer.WithContext(context.Background(), testEnforceViewer{tid: 8})

	// 清场：移除历史遗留节点（平台视图全量取回后删除；标签随最后一个
	// 节点消失，无独立 drop 语义）。
	purge := func() {
		rows, err := repo.Query(platformCtx, nil)
		if err != nil {
			return
		}
		ids := make([]string, 0, len(rows))
		for _, r := range rows {
			ids = append(ids, r.UUID)
		}
		if len(ids) > 0 {
			_, _ = repo.DeleteByUUIDs(platformCtx, ids)
		}
	}
	purge()
	t.Cleanup(purge)

	// 写入：tenant 强制覆盖（ctx7 下写的三条属于租户 7，ctx8 下两条属于
	// 租户 8）。标量/列表通道一并经真实服务端往返。
	mk := func(name string, age int64) *itEntity {
		return &itEntity{Name: name, Age: age, Tags: []string{"t"}}
	}
	created7, err := repo.BatchCreate(ctx7, []*itEntity{
		mk("a7", 71),
		mk("b7", 72),
		mk("c7", 73),
	})
	require.NoError(t, err)
	require.Len(t, created7, 3)
	for _, dto := range created7 {
		require.NotEmpty(t, dto.UUID, "created nodes must carry back their element ids")
		require.NotNil(t, dto.TenantID.GetTenantID())
		assert.Equal(t, uint32(7), *dto.TenantID.GetTenantID(), "create must force tenant 7")
		assert.Equal(t, []string{"t"}, dto.Tags, "list channel must round-trip")
	}
	created8, err := repo.BatchCreate(ctx8, []*itEntity{
		mk("a8", 81),
		mk("b8", 82),
	})
	require.NoError(t, err)
	require.Len(t, created8, 2)

	// Count：各自租户视角下只见自己的节点。
	n7, err := repo.Count(ctx7, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(3), n7)
	n8, err := repo.Count(ctx8, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n8)

	// Query：租户视角只取回本租户节点，且属性与 element id 完整还原。
	rows7, err := repo.Query(ctx7, nil)
	require.NoError(t, err)
	require.Len(t, rows7, 3, "tenant-7 view must see exactly its 3 nodes")
	for _, r := range rows7 {
		tid := r.TenantID.GetTenantID()
		require.NotNil(t, tid, "rows must carry tenant payload")
		assert.Equal(t, uint32(7), *tid, "rows must be tenant-7 only")
		assert.Equal(t, []string{"t"}, r.Tags)
		assert.GreaterOrEqual(t, r.Age, int64(71), "age channel present")
		assert.LessOrEqual(t, r.Age, int64(73), "age channel present")
	}

	// GetByUUID：本租户节点可取回；他租户节点与不存在同构。
	got, err := repo.GetByUUID(ctx7, created7[0].UUID)
	require.NoError(t, err)
	assert.Equal(t, "a7", got.Name)
	_, err = repo.GetByUUID(ctx7, created8[0].UUID)
	assert.ErrorIs(t, err, ErrPointNotFound)

	// 越权删除：租户 7 试图删租户 8 的节点 → 预计数为 0，计数不变。
	deleted, err := repo.DeleteByUUIDs(ctx7, []string{created8[0].UUID})
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted)
	n8, err = repo.Count(ctx8, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n8)

	// 本租户删除：删自己的节点生效。
	deleted, err = repo.DeleteByUUIDs(ctx7, []string{created7[0].UUID})
	require.NoError(t, err)
	assert.Equal(t, int64(1), deleted)
	n7, err = repo.Count(ctx7, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n7)

	// 平台视图全量清场后计数归零。
	purge()
	nAll, err := repo.Count(platformCtx, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), nAll)
}
