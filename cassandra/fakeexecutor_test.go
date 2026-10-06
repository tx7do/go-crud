package cassandra

import (
	"context"
	"errors"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tx7do/go-utils/mapper"

	"github.com/tx7do/go-crud/cassandra/mixin"
	"github.com/tx7do/go-crud/viewer"
)

// ─────────────────────────────────────────────────────────────────────────────
// fake executor 离线测试：
//
// gocql.Session 是具体类型无法替身，仓库只依赖 sessionExecutor 接口——
// 注入记录型替身后可在离线环境锁定仓库层的全部协议交互语义：语句拼装
// （列清单/占位符/表名限定）、租户强制落列与 WHERE 谓词注入、主键直取的
// 客户端租户校验、删除前的主键预过滤、计数回读、错误包装哨兵。网络相关
// 的集成链路另见 KRATOS_IT 门禁测试。
// ─────────────────────────────────────────────────────────────────────────────

// fcEntity fake executor 测试实体：数值主键 + 标量/列表/映射 + TenantID mixin。
type fcEntity struct {
	ID    int64
	Title string
	Age   int64
	Tags  []string
	mixin.TenantID
}

// fcUuidEntity 字符串主键 + TenantID mixin。
type fcUuidEntity struct {
	UUID  string
	Title string
	mixin.TenantID
}

// fcPlainEntity 非 tenant-scoped 实体（无 mixin）。
type fcPlainEntity struct {
	ID    int64
	Title string
}

// fcCtx7 / fcCtxPlatform 租户 7 与平台视图上下文。
var (
	fcCtx7        = viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	fcCtxPlatform = viewer.WithContext(context.Background(), testEnforceViewer{tid: 0, platform: true})
)

// fakeExecutor 记录全部语句并按预设回放行/错误的会话替身。
type fakeExecutor struct {
	stmts []string         // 全部语句（Exec/Select 逐条；Batch 展开逐条）
	args  [][]any          // 与 stmts 一一对应的绑定参数
	calls int              // 统一调用计数（errs 的键）
	rows  []map[string]any // Select 回放
	errs  map[int]error    // 按调用序号的注入错误
}

func (f *fakeExecutor) record(stmt string, args []any) error {
	f.stmts = append(f.stmts, stmt)
	f.args = append(f.args, args)
	if err, ok := f.errs[f.calls]; ok {
		f.calls++
		return err
	}
	f.calls++
	return nil
}

func (f *fakeExecutor) Exec(_ context.Context, stmt string, args ...any) error {
	return f.record(stmt, args)
}

func (f *fakeExecutor) Select(_ context.Context, stmt string, args ...any) ([]map[string]any, error) {
	if err := f.record(stmt, args); err != nil {
		return nil, err
	}
	return f.rows, nil
}

func (f *fakeExecutor) Batch(_ context.Context, _ gocql.BatchType, stmts []string, argsList [][]any) error {
	for i, stmt := range stmts {
		var args []any
		if i < len(argsList) {
			args = argsList[i]
		}
		if err := f.record(stmt, args); err != nil {
			return err
		}
	}
	return nil
}

// newFakeRepo 构建绑定 fake executor 的仓库（表名固定 ks.rows）。
func newFakeRepo[ENTITY any](t *testing.T) (*fakeExecutor, *Repository[ENTITY, ENTITY]) {
	t.Helper()
	fe := &fakeExecutor{}
	m := mapper.NewCopierMapper[ENTITY, ENTITY]()
	return fe, newRepository[ENTITY, ENTITY](fe, "ks.rows", m)
}

// ─────────────────────────────────────────────────────────────────────────────
// 写入路径。
// ─────────────────────────────────────────────────────────────────────────────

// TestFake_Create_TenantForced 单写：主键/列清单/占位符拼装正确，租户
// 强制落入绑定参数。
func TestFake_Create_TenantForced(t *testing.T) {
	fe, repo := newFakeRepo[fcEntity](t)

	out, err := repo.Create(fcCtx7, &fcEntity{ID: 101, Title: "a7", Age: 42, Tags: []string{"t"}})
	require.NoError(t, err)

	require.Len(t, fe.stmts, 1)
	assert.Equal(t, "INSERT INTO ks.rows (ID, Title, Age, Tags, tenant_id) VALUES (?, ?, ?, ?, ?)", fe.stmts[0])
	assert.Equal(t, int64(101), fe.args[0][0])
	assert.Equal(t, int64(7), fe.args[0][len(fe.args[0])-1], "create must force tenant 7")

	assert.Equal(t, int64(101), out.ID)
	assert.Equal(t, int64(7), out.TenantID.TenantID)
}

// TestFake_Create_PlainEntity 非 tenant 实体：无 tenant_id 列。
func TestFake_Create_PlainEntity(t *testing.T) {
	fe, repo := newFakeRepo[fcPlainEntity](t)

	_, err := repo.Create(fcCtx7, &fcPlainEntity{ID: 1, Title: "x"})
	require.NoError(t, err)
	assert.Equal(t, "INSERT INTO ks.rows (ID, Title) VALUES (?, ?)", fe.stmts[0])
}

// TestFake_Create_EmptyPK 空主键拒绝。
func TestFake_Create_EmptyPK(t *testing.T) {
	_, repo := newFakeRepo[fcEntity](t)

	_, err := repo.Create(fcCtx7, &fcEntity{Title: "x"})
	assert.ErrorIs(t, err, ErrInvalidPointID)
}

// TestFake_BatchCreate_TenantForced 批量写：逐实体强制租户，单次 batch。
func TestFake_BatchCreate_TenantForced(t *testing.T) {
	fe, repo := newFakeRepo[fcEntity](t)

	outs, err := repo.BatchCreate(fcCtx7, []*fcEntity{{ID: 1, Title: "a"}, {ID: 2, Title: "b"}})
	require.NoError(t, err)
	require.Len(t, outs, 2)
	require.Len(t, fe.stmts, 2)
	for i, stmt := range fe.stmts {
		assert.Contains(t, stmt, "INSERT INTO ks.rows")
		assert.Equal(t, int64(7), fe.args[i][len(fe.args[i])-1])
	}
}

// TestFake_BatchCreate_Empty 空批量直接返回。
func TestFake_BatchCreate_Empty(t *testing.T) {
	fe, repo := newFakeRepo[fcEntity](t)

	outs, err := repo.BatchCreate(fcCtx7, nil)
	assert.NoError(t, err)
	assert.Nil(t, outs)
	assert.Empty(t, fe.stmts)
}

// TestFake_Create_MissingViewerFailClosed 缺身份 fail-closed。
func TestFake_Create_MissingViewerFailClosed(t *testing.T) {
	fe, repo := newFakeRepo[fcEntity](t)

	_, err := repo.Create(context.Background(), &fcEntity{ID: 1, Title: "x"})
	assert.ErrorIs(t, err, viewer.ErrMissingViewer)
	assert.Empty(t, fe.stmts)

	_, err = repo.BatchCreate(context.Background(), []*fcEntity{{ID: 1, Title: "x"}})
	assert.ErrorIs(t, err, viewer.ErrMissingViewer)
	assert.Empty(t, fe.stmts)
}

// ─────────────────────────────────────────────────────────────────────────────
// 主键直取路径。
// ─────────────────────────────────────────────────────────────────────────────

// TestFake_Get_TenantMatch 本租户行取回：属性完整还原（bigint→int64、
// text→string、list<text>→[]string）。
func TestFake_Get_TenantMatch(t *testing.T) {
	fe, repo := newFakeRepo[fcEntity](t)
	fe.rows = []map[string]any{
		{"ID": int64(101), "Title": "own", "Age": int64(42), "Tags": []any{"t"}, "tenant_id": int64(7)},
	}

	out, err := repo.Get(fcCtx7, 101)
	require.NoError(t, err)
	assert.Equal(t, "own", out.Title)
	assert.Equal(t, int64(42), out.Age)
	assert.Equal(t, []string{"t"}, out.Tags)
	assert.Equal(t, int64(101), out.ID)

	require.Len(t, fe.stmts, 1)
	assert.Equal(t, "SELECT ID, Title, Age, Tags, tenant_id FROM ks.rows WHERE ID = ?", fe.stmts[0])
	assert.Equal(t, int64(101), fe.args[0][0])
}

// TestFake_Get_TenantMismatch 他租户行与不存在同构（主键直取路径的
// 客户端租户校验）。
func TestFake_Get_TenantMismatch(t *testing.T) {
	fe, repo := newFakeRepo[fcEntity](t)
	fe.rows = []map[string]any{{"ID": int64(201), "Title": "foreign", "tenant_id": int64(8)}}

	_, err := repo.Get(fcCtx7, 201)
	assert.ErrorIs(t, err, ErrPointNotFound)
}

// TestFake_Get_MissingTenantColumn 缺租户列的行视同他租户。
func TestFake_Get_MissingTenantColumn(t *testing.T) {
	fe, repo := newFakeRepo[fcEntity](t)
	fe.rows = []map[string]any{{"ID": int64(1), "Title": "x"}}

	_, err := repo.Get(fcCtx7, 1)
	assert.ErrorIs(t, err, ErrPointNotFound)
}

// TestFake_Get_PlatformSkips 平台视图放行。
func TestFake_Get_PlatformSkips(t *testing.T) {
	fe, repo := newFakeRepo[fcEntity](t)
	fe.rows = []map[string]any{{"ID": int64(1), "Title": "any", "tenant_id": int64(99)}}

	out, err := repo.Get(fcCtxPlatform, 1)
	require.NoError(t, err)
	assert.Equal(t, "any", out.Title)
}

// TestFake_Get_EmptyResult 空结果即未找到。
func TestFake_Get_EmptyResult(t *testing.T) {
	_, repo := newFakeRepo[fcEntity](t)

	_, err := repo.Get(fcCtx7, 1)
	assert.ErrorIs(t, err, ErrPointNotFound)
}

// TestFake_GetByUUID_StringPK 字符串主键通道；数值主键实体走字符串通道
// 报类型不匹配。
func TestFake_GetByUUID_StringPK(t *testing.T) {
	fe, repo := newFakeRepo[fcUuidEntity](t)
	fe.rows = []map[string]any{{"UUID": "u-1", "Title": "own", "tenant_id": int64(7)}}

	out, err := repo.GetByUUID(fcCtx7, "u-1")
	require.NoError(t, err)
	assert.Equal(t, "u-1", out.UUID)
	assert.Equal(t, "SELECT UUID, Title, tenant_id FROM ks.rows WHERE UUID = ?", fe.stmts[0])

	// 数值通道对字符串主键实体不适用。
	_, err = repo.Get(fcCtx7, 1)
	assert.ErrorIs(t, err, ErrInvalidRequest)

	// 空 UUID 拒绝。
	_, err = repo.GetByUUID(fcCtx7, "")
	assert.ErrorIs(t, err, ErrInvalidRequest)
}

// ─────────────────────────────────────────────────────────────────────────────
// Query / Count / Exists。
// ─────────────────────────────────────────────────────────────────────────────

// TestFake_Query_TenantPredicateInjected 租户视图：谓词注入 + 他租户行
// 被客户端校验剔除。
func TestFake_Query_TenantPredicateInjected(t *testing.T) {
	fe, repo := newFakeRepo[fcEntity](t)
	fe.rows = []map[string]any{
		{"ID": int64(1), "Title": "own", "tenant_id": int64(7)},
		{"ID": int64(2), "Title": "foreign", "tenant_id": int64(8)},
	}

	rows, err := repo.Query(fcCtx7, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "own", rows[0].Title)

	assert.Equal(t, "SELECT ID, Title, Age, Tags, tenant_id FROM ks.rows WHERE tenant_id = ?", fe.stmts[0])
	assert.Equal(t, int64(7), fe.args[0][0])
}

// TestFake_Query_CallerWhereMerged 调用方条件与租户谓词合并（括号包裹
// AND，参数按位置追加）。
func TestFake_Query_CallerWhereMerged(t *testing.T) {
	fe, repo := newFakeRepo[fcEntity](t)

	rows, err := repo.Query(fcCtx7, &Query{Where: "Age > ?", Args: []any{18}, AllowFiltering: true})
	require.NoError(t, err)
	assert.Empty(t, rows)

	assert.Equal(t, "SELECT ID, Title, Age, Tags, tenant_id FROM ks.rows WHERE (Age > ?) AND tenant_id = ? ALLOW FILTERING", fe.stmts[0])
	assert.Equal(t, []any{18, int64(7)}, fe.args[0])
}

// TestFake_Query_PlatformNoPredicate 平台视图：无谓词无参数。
func TestFake_Query_PlatformNoPredicate(t *testing.T) {
	fe, repo := newFakeRepo[fcEntity](t)
	fe.rows = []map[string]any{{"ID": int64(1), "Title": "a", "tenant_id": int64(7)}}

	rows, err := repo.Query(fcCtxPlatform, nil)
	require.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, "SELECT ID, Title, Age, Tags, tenant_id FROM ks.rows", fe.stmts[0])
	assert.Empty(t, fe.args[0])
}

// TestFake_Query_MissingViewerFailClosed 缺身份 fail-closed。
func TestFake_Query_MissingViewerFailClosed(t *testing.T) {
	fe, repo := newFakeRepo[fcEntity](t)

	_, err := repo.Query(context.Background(), nil)
	assert.ErrorIs(t, err, viewer.ErrMissingViewer)
	assert.Empty(t, fe.stmts)
}

// TestFake_Count_TenantInjected 计数：别名回读 + 谓词注入。
func TestFake_Count_TenantInjected(t *testing.T) {
	fe, repo := newFakeRepo[fcEntity](t)
	fe.rows = []map[string]any{{"row_count": 42}}

	n, err := repo.Count(fcCtx7, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(42), n)
	assert.Equal(t, "SELECT count(*) AS row_count FROM ks.rows WHERE tenant_id = ?", fe.stmts[0])

	// int64 计数（替身形态）与空结果。
	fe.rows = []map[string]any{{"row_count": int64(3)}}
	n, err = repo.Count(fcCtx7, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(3), n)

	fe.rows = nil
	n, err = repo.Count(fcCtx7, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

// TestFake_Exists 计数为正即存在。
func TestFake_Exists(t *testing.T) {
	fe, repo := newFakeRepo[fcEntity](t)
	fe.rows = []map[string]any{{"row_count": 0}}

	ok, err := repo.Exists(fcCtx7, nil)
	require.NoError(t, err)
	assert.False(t, ok)
}

// ─────────────────────────────────────────────────────────────────────────────
// 删除路径。
// ─────────────────────────────────────────────────────────────────────────────

// TestFake_DeleteByIDs_TenantPrefiltered 数值主键删除：预过滤（IN + 租户
// 谓词）后按过滤结果删除，计数取过滤结果。
func TestFake_DeleteByIDs_TenantPrefiltered(t *testing.T) {
	fe, repo := newFakeRepo[fcEntity](t)
	fe.rows = []map[string]any{{"ID": int64(101)}}

	n, err := repo.DeleteByIDs(fcCtx7, []uint64{101, 102})
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	require.Len(t, fe.stmts, 2)
	assert.Equal(t, "SELECT ID FROM ks.rows WHERE (ID IN (?, ?)) AND tenant_id = ? ALLOW FILTERING", fe.stmts[0])
	assert.Equal(t, []any{int64(101), int64(102), int64(7)}, fe.args[0])
	assert.Equal(t, "DELETE FROM ks.rows WHERE ID IN (?)", fe.stmts[1])
	assert.Equal(t, []any{int64(101)}, fe.args[1])
}

// TestFake_DeleteByIDs_CrossTenantZero 越权删除：预过滤为空 → 不发删除。
func TestFake_DeleteByIDs_CrossTenantZero(t *testing.T) {
	fe, repo := newFakeRepo[fcEntity](t)

	n, err := repo.DeleteByIDs(fcCtx7, []uint64{201})
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
	assert.Len(t, fe.stmts, 1, "pre-filter only; the delete must not be issued")
}

// TestFake_DeleteByUUIDs 字符串主键删除同语义。
func TestFake_DeleteByUUIDs(t *testing.T) {
	fe, repo := newFakeRepo[fcUuidEntity](t)
	fe.rows = []map[string]any{{"UUID": "u-1"}}

	n, err := repo.DeleteByUUIDs(fcCtxPlatform, []string{"u-1"})
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	assert.Equal(t, "SELECT UUID FROM ks.rows WHERE UUID IN (?) ALLOW FILTERING", fe.stmts[0], "no tenant injection on platform view keeps the fragment unwrapped")
}

// TestFake_DeleteByWhere 原生 WHERE 删除：同一条件先查后删。
func TestFake_DeleteByWhere(t *testing.T) {
	fe, repo := newFakeRepo[fcEntity](t)
	fe.rows = []map[string]any{{"ID": int64(1)}, {"ID": int64(2)}}

	n, err := repo.DeleteByWhere(fcCtx7, &Query{Where: "expired = ?", Args: []any{true}})
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)

	require.Len(t, fe.stmts, 2)
	assert.Equal(t, "SELECT ID FROM ks.rows WHERE (expired = ?) AND tenant_id = ?", fe.stmts[0])
	assert.Equal(t, "DELETE FROM ks.rows WHERE ID IN (?, ?)", fe.stmts[1])
}

// TestFake_DeleteByWhere_EmptyRejected 空 WHERE 拒绝（防误删全表）。
func TestFake_DeleteByWhere_EmptyRejected(t *testing.T) {
	fe, repo := newFakeRepo[fcEntity](t)

	_, err := repo.DeleteByWhere(fcCtx7, nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = repo.DeleteByWhere(fcCtx7, &Query{})
	assert.ErrorIs(t, err, ErrInvalidRequest)
	assert.Empty(t, fe.stmts)
}

// TestFake_Delete_MissingViewerFailClosed 缺身份 fail-closed（删除路径）。
func TestFake_Delete_MissingViewerFailClosed(t *testing.T) {
	fe, repo := newFakeRepo[fcEntity](t)

	_, err := repo.DeleteByIDs(context.Background(), []uint64{1})
	assert.ErrorIs(t, err, viewer.ErrMissingViewer)
	_, err = repo.DeleteByWhere(context.Background(), &Query{Where: "x = ?", Args: []any{1}})
	assert.ErrorIs(t, err, viewer.ErrMissingViewer)
	assert.Empty(t, fe.stmts)
}

// ─────────────────────────────────────────────────────────────────────────────
// 错误包装与守卫。
// ─────────────────────────────────────────────────────────────────────────────

// TestFake_ExecErrors 各路径的哨兵包装（每用例前重置计数，使错误序号在
// 操作内部从 0 起算）。
func TestFake_ExecErrors(t *testing.T) {
	fe, repo := newFakeRepo[fcEntity](t)
	boom := errors.New("boom")
	flush := func() {
		fe.stmts = nil
		fe.args = nil
		fe.calls = 0
		fe.rows = nil
	}

	flush()
	fe.errs = map[int]error{0: boom}
	_, err := repo.Create(fcCtx7, &fcEntity{ID: 1, Title: "x"})
	assert.ErrorIs(t, err, ErrInsertFailed)

	flush()
	fe.errs = map[int]error{0: boom}
	_, err = repo.Get(fcCtx7, 1)
	assert.ErrorIs(t, err, ErrQueryFailed)

	flush()
	fe.errs = map[int]error{0: boom}
	_, err = repo.Query(fcCtx7, nil)
	assert.ErrorIs(t, err, ErrQueryFailed)

	flush()
	fe.errs = map[int]error{0: boom}
	_, err = repo.Count(fcCtx7, nil)
	assert.ErrorIs(t, err, ErrCountFailed)

	flush()
	fe.errs = map[int]error{0: boom} // 预过滤失败
	_, err = repo.DeleteByIDs(fcCtx7, []uint64{1})
	assert.ErrorIs(t, err, ErrQueryFailed)

	flush()
	fe.rows = []map[string]any{{"ID": int64(1)}}
	fe.errs = map[int]error{1: boom} // 预过滤成功、删除失败
	_, err = repo.DeleteByIDs(fcCtx7, []uint64{1})
	assert.ErrorIs(t, err, ErrDeleteFailed)

	flush()
	fe.rows = []map[string]any{{"ID": int64(1)}}
	fe.errs = map[int]error{1: boom} // DeleteByWhere 同路径（预选成功、删除失败）
	_, err = repo.DeleteByWhere(fcCtx7, &Query{Where: "expired = ?", Args: []any{true}})
	assert.ErrorIs(t, err, ErrDeleteFailed)
}

// TestRepository_Guards nil 执行器 / 非法表名 / 数值字符串混用等守卫。
func TestRepository_Guards(t *testing.T) {
	ctx := context.Background()
	m := mapper.NewCopierMapper[fcEntity, fcEntity]()

	nilRepo := newRepository[fcEntity, fcEntity](nil, "ks.rows", m)
	_, err := nilRepo.Create(ctx, &fcEntity{ID: 1, Title: "x"})
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	_, err = nilRepo.Get(ctx, 1)
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	_, err = nilRepo.Count(ctx, nil)
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	_, err = nilRepo.DeleteByUUIDs(ctx, []string{"u"})
	assert.ErrorIs(t, err, ErrClientNotInitialized)

	fe := &fakeExecutor{}
	badTable := newRepository[fcEntity, fcEntity](fe, "bad`table", m)
	_, err = badTable.Create(ctx, &fcEntity{ID: 1, Title: "x"})
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = badTable.Query(ctx, nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)

	// nil DTO / 非法列名（注入面拒绝，触网前报错）。
	okRepo := newRepository[fcEntity, fcEntity](fe, "ks.rows", m)
	_, err = okRepo.Create(ctx, nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)

	badColRepo := newRepository[mixinEntity, mixinEntity](fe, "ks.rows", mapper.NewCopierMapper[mixinEntity, mixinEntity]())
	_, err = badColRepo.Create(ctx, &mixinEntity{ID: 1, BadCol: "x"})
	assert.ErrorIs(t, err, ErrInvalidRequest)
	assert.Empty(t, fe.stmts)
}

// mixinEntity 非法列名的测试实体（bad)col 含括号）。
type mixinEntity struct {
	ID       int64
	BadCol   string `cql:"name:bad)col"`
	LastSeen string `cql:"-"`
}
