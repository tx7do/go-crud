package cassandra

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tx7do/go-utils/mapper"
	"github.com/tx7do/go-wind/log"

	"github.com/tx7do/go-crud/cassandra/mixin"
	"github.com/tx7do/go-crud/viewer"
)

// ─────────────────────────────────────────────────────────────────────────────
// 集成测试（KRATOS_IT 门禁）：租户隔离全链路。
// ─────────────────────────────────────────────────────────────────────────────

// requireService skips the test unless integration mode is enabled (KRATOS_IT)
// or in -short mode, to keep hermetic runs green.
func requireService(t *testing.T) {
	t.Helper()
	if os.Getenv("KRATOS_IT") == "" {
		t.Skip("skipping integration test: requires a live server; set KRATOS_IT to enable")
	}
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
}

// createTestITClient 建立到本地 Cassandra 的连接（仅集成模式；容器引导
// 期间 CreateSession 会失败，按 3s 间隔重试至多 2 分钟）。
func createTestITClient(t *testing.T) *Client {
	requireService(t)
	deadline := time.Now().Add(2 * time.Minute)
	for {
		c, err := NewCassandraClient(WithHosts("127.0.0.1"), WithDisableInitialHostLookup(true))
		if err == nil {
			return c
		}
		if time.Now().After(deadline) {
			t.Fatalf("cassandra not reachable at 127.0.0.1:9042: %v", err)
		}
		time.Sleep(3 * time.Second)
	}
}

// itRowEntity 集成测试实体：bigint 主键 + 标量/列表 + TenantID mixin。
type itRowEntity struct {
	ID    int64
	Title string
	Age   int64
	Tags  []string
	mixin.TenantID
}

// TestIntegration_TenantIsolation 租户隔离全链路：
// 写入强制落租户 → Count/Query 按租户隔离 → Get 越权同构未找到 →
// 越权删除被预过滤拦截 → 平台视图全量清场。
func TestIntegration_TenantIsolation(t *testing.T) {
	c := createTestITClient(t)
	defer c.Close()

	ctx := context.Background()
	const table = "go_crud_it.it_rows"

	// DDL（幂等）：keyspace / 简单主键表 / tenant_id 二级索引（租户谓词
	// 的服务端过滤依赖它）。
	require.NoError(t, c.Exec(ctx,
		"CREATE KEYSPACE IF NOT EXISTS go_crud_it WITH replication = {'class':'SimpleStrategy','replication_factor':1}"))
	require.NoError(t, c.Exec(ctx,
		"CREATE TABLE IF NOT EXISTS "+table+" (id bigint PRIMARY KEY, title text, age bigint, tags list<text>, tenant_id bigint)"))
	require.NoError(t, c.Exec(ctx,
		"CREATE INDEX IF NOT EXISTS it_rows_tenant_idx ON "+table+" (tenant_id)"))

	m := mapper.NewCopierMapper[itRowEntity, itRowEntity]()
	repo := NewRepository[itRowEntity, itRowEntity](c, table, m, log.GetLogger())

	ctx7 := viewer.WithContext(ctx, testEnforceViewer{tid: 7})
	ctx8 := viewer.WithContext(ctx, testEnforceViewer{tid: 8})
	platformCtx := viewer.WithContext(ctx, testEnforceViewer{tid: 0, platform: true})

	// 清场：移除历史遗留行。
	purge := func() {
		rows, err := repo.Query(platformCtx, nil)
		if err != nil {
			return
		}
		ids := make([]uint64, 0, len(rows))
		for _, r := range rows {
			ids = append(ids, uint64(r.ID))
		}
		if len(ids) > 0 {
			_, _ = repo.DeleteByIDs(platformCtx, ids)
		}
	}
	purge()
	t.Cleanup(purge)

	// 写入：tenant 强制覆盖（ctx7 下写的三条属于租户 7，ctx8 下两条属于
	// 租户 8）。索引就绪存在传播延迟，写入前轮询至租户计数可用。
	waitForIndex := func() {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for {
			_, err := repo.Count(ctx7, nil)
			if err == nil {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("tenant index not usable within 30s: %v", err)
			}
			time.Sleep(time.Second)
		}
	}
	waitForIndex()

	mk := func(id int64, title string, age int64) *itRowEntity {
		return &itRowEntity{ID: id, Title: title, Age: age, Tags: []string{"t"}}
	}
	created7, err := repo.BatchCreate(ctx7, []*itRowEntity{
		mk(101, "a7", 71),
		mk(102, "b7", 72),
		mk(103, "c7", 73),
	})
	require.NoError(t, err)
	require.Len(t, created7, 3)
	for _, dto := range created7 {
		require.NotNil(t, dto.TenantID.GetTenantID())
		assert.Equal(t, uint32(7), *dto.TenantID.GetTenantID(), "create must force tenant 7")
	}
	created8, err := repo.BatchCreate(ctx8, []*itRowEntity{
		mk(201, "a8", 81),
		mk(202, "b8", 82),
	})
	require.NoError(t, err)
	require.Len(t, created8, 2)

	// Count：各自租户视角下只见自己的行。
	n7, err := repo.Count(ctx7, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(3), n7)
	n8, err := repo.Count(ctx8, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n8)

	// Query：租户视角只取回本租户行，且属性完整还原（list<text>→[]string）。
	rows7, err := repo.Query(ctx7, nil)
	require.NoError(t, err)
	require.Len(t, rows7, 3, "tenant-7 view must see exactly its 3 rows")
	for _, r := range rows7 {
		tid := r.TenantID.GetTenantID()
		require.NotNil(t, tid, "rows must carry tenant payload")
		assert.Equal(t, uint32(7), *tid, "rows must be tenant-7 only")
		assert.Equal(t, []string{"t"}, r.Tags)
	}

	// Get：本租户行可取回；他租户行与不存在同构（ErrPointNotFound）。
	got, err := repo.Get(ctx7, 101)
	require.NoError(t, err)
	assert.Equal(t, "a7", got.Title)
	_, err = repo.Get(ctx7, 201)
	assert.ErrorIs(t, err, ErrPointNotFound)

	// 越权删除：租户 7 试图删租户 8 的行 → 预过滤为 0、计数不变。
	deleted, err := repo.DeleteByIDs(ctx7, []uint64{201})
	require.NoError(t, err)
	assert.Equal(t, int64(0), deleted)
	n8, err = repo.Count(ctx8, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(2), n8)

	// 本租户删除：删自己的行生效。
	deleted, err = repo.DeleteByIDs(ctx7, []uint64{101})
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
