package neo4j

import (
	"context"
	"errors"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tx7do/go-utils/mapper"
	"github.com/tx7do/go-wind/log"

	"github.com/tx7do/go-crud/neo4j/mixin"
	"github.com/tx7do/go-crud/viewer"
)

// ─────────────────────────────────────────────────────────────────────────────
// fake driver 离线测试：
//
// neo4j-go-driver 的 DriverWithContext / SessionWithContext /
// ResultWithContext 均为接口，Record / Node 为可导出构造的结构体别名，
// 注入替身后可在离线环境锁定仓库层的全部协议交互语义——建点属性组装
// （含租户强制落属性）、element id 回读通道、WHERE 谓词注入与行过滤、
// 删除前预计数与越权拦截、count 回读、错误包装哨兵。网络相关的集成链路
// 另见 KRATOS_IT 门禁测试。
// ─────────────────────────────────────────────────────────────────────────────

// fcEntity fake driver 测试实体：身份通道字段 + 标量 + TenantID mixin。
type fcEntity struct {
	UUID string
	Name string
	Age  int64
	mixin.TenantID
}

// fcPlainEntity 非 tenant-scoped 实体（无 mixin）。
type fcPlainEntity struct {
	UUID string
	Name string
}

// fcCtx7 / fcCtxPlatform 租户 7 与平台视图上下文。
var (
	fcCtx7        = viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	fcCtxPlatform = viewer.WithContext(context.Background(), testEnforceViewer{tid: 0, platform: true})
)

// fakeDriver 记录全部 Run 调用并按预设回放记录/错误的驱动替身。
// 嵌入 DriverWithContext 接口：未覆盖的方法一旦被调用即 panic，测试仅
// 触达覆盖面。
type fakeDriver struct {
	neo4j.DriverWithContext

	connErr error

	// 捕获面：全部自动提交语句（含预计数/删除等复合路径的多次调用）。
	cyphers []string
	paramss []map[string]any

	// 回放面：记录表对所有 Run 调用统一回放（预计数+删除等复合路径的
	// 各次调用共用同一表；计数调用只读 count(n) 列，其余调用忽略记录）。
	records    []*neo4j.Record
	collectErr error
	runErrs    map[int]error
}

func (d *fakeDriver) NewSession(_ context.Context, _ neo4j.SessionConfig) neo4j.SessionWithContext {
	return &fakeSession{drv: d}
}

func (d *fakeDriver) VerifyConnectivity(context.Context) error { return d.connErr }

func (d *fakeDriver) Close(context.Context) error { return nil }

// fakeSession 会话替身：Run 记录调用并回放，Close 幂等。
type fakeSession struct {
	neo4j.SessionWithContext
	drv *fakeDriver
}

func (s *fakeSession) Run(_ context.Context, cypher string, params map[string]any, _ ...func(*neo4j.TransactionConfig)) (neo4j.ResultWithContext, error) {
	d := s.drv
	idx := len(d.cyphers)
	d.cyphers = append(d.cyphers, cypher)
	d.paramss = append(d.paramss, params)
	if err, ok := d.runErrs[idx]; ok {
		return nil, err
	}
	return &fakeResult{records: d.records, err: d.collectErr}, nil
}

func (s *fakeSession) Close(context.Context) error { return nil }

// fakeResult 结果替身：Collect 回放预设记录或错误。
type fakeResult struct {
	neo4j.ResultWithContext
	records []*neo4j.Record
	err     error
}

func (r *fakeResult) Collect(context.Context) ([]*neo4j.Record, error) { return r.records, r.err }

// nodeRecord 构造 "n" 列的节点记录。
func nodeRecord(elementID string, props map[string]any) *neo4j.Record {
	return &neo4j.Record{Keys: []string{"n"}, Values: []any{neo4j.Node{ElementId: elementID, Props: props}}}
}

// countRecord 构造 count(n) 聚合记录。
func countRecord(n int64) *neo4j.Record {
	return &neo4j.Record{Keys: []string{"count(n)"}, Values: []any{n}}
}

// elementRecord 构造 elementId(n) 回读记录。
func elementRecord(id string) *neo4j.Record {
	return &neo4j.Record{Keys: []string{"elementId(n)"}, Values: []any{id}}
}

// newFakeRepo 构建绑定 fake driver 的仓库（标签固定 coll）。
func newFakeRepo[ENTITY any](t *testing.T) (*fakeDriver, *Repository[ENTITY, ENTITY]) {
	t.Helper()
	d := &fakeDriver{}
	c, err := NewClient(WithNeo4jDriver(d))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	m := mapper.NewCopierMapper[ENTITY, ENTITY]()
	return d, NewRepository[ENTITY, ENTITY](c, "coll", m, log.GetLogger())
}

// ─────────────────────────────────────────────────────────────────────────────
// 建点路径。
// ─────────────────────────────────────────────────────────────────────────────

// TestFake_Create_TenantForced 建点：属性经 $props 整体传递，租户被强制
// 落入属性表（值 7），element id 回读到身份通道字段。
func TestFake_Create_TenantForced(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)
	d.records = []*neo4j.Record{elementRecord("4:0:1")}

	out, err := repo.Create(fcCtx7, &fcEntity{Name: "x", Age: 42})
	require.NoError(t, err)

	require.Len(t, d.cyphers, 1)
	assert.Equal(t, "CREATE (n:`coll`) SET n = $props RETURN elementId(n)", d.cyphers[0])
	assert.Equal(t, map[string]any{"Name": "x", "Age": int64(42), "tenant_id": int64(7)}, d.paramss[0]["props"])
	assert.Equal(t, int64(7), out.TenantID.TenantID, "create must force tenant 7")
	assert.Equal(t, "4:0:1", out.UUID, "element id must round-trip into the uuid channel")
}

// TestFake_Create_PlainEntity 非 tenant 实体：属性表无 tenant_id。
func TestFake_Create_PlainEntity(t *testing.T) {
	d, repo := newFakeRepo[fcPlainEntity](t)
	d.records = []*neo4j.Record{elementRecord("4:0:2")}

	out, err := repo.Create(fcCtx7, &fcPlainEntity{Name: "x"})
	require.NoError(t, err)
	require.Len(t, d.cyphers, 1)
	assert.Equal(t, map[string]any{"Name": "x"}, d.paramss[0]["props"], "plain entity has no tenant property")
	assert.Equal(t, "4:0:2", out.UUID)
}

// TestFake_BatchCreate_TenantForced 批量建点：UNWIND 单事务路径，逐实体
// 强制租户，element id 按行序回填。
func TestFake_BatchCreate_TenantForced(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)
	d.records = []*neo4j.Record{elementRecord("e0"), elementRecord("e1")}

	outs, err := repo.BatchCreate(fcCtx7, []*fcEntity{{Name: "a"}, {Name: "b"}})
	require.NoError(t, err)
	require.Len(t, outs, 2)

	require.Len(t, d.cyphers, 1)
	assert.Equal(t, "UNWIND $rows AS row CREATE (n:`coll`) SET n = row RETURN elementId(n)", d.cyphers[0])
	rows, ok := d.paramss[0]["rows"].([]any)
	require.True(t, ok)
	require.Len(t, rows, 2)
	assert.Equal(t, map[string]any{"Name": "a", "Age": int64(0), "tenant_id": int64(7)}, rows[0])
	assert.Equal(t, map[string]any{"Name": "b", "Age": int64(0), "tenant_id": int64(7)}, rows[1])
	assert.Equal(t, "e0", outs[0].UUID)
	assert.Equal(t, "e1", outs[1].UUID)
	assert.Equal(t, int64(7), outs[0].TenantID.TenantID)
	assert.Equal(t, int64(7), outs[1].TenantID.TenantID)
}

// TestFake_BatchCreate_NilEntriesSkipped nil 条目跳过。
func TestFake_BatchCreate_NilEntriesSkipped(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)
	d.records = []*neo4j.Record{elementRecord("e0")}

	outs, err := repo.BatchCreate(fcCtx7, []*fcEntity{nil, {Name: "a"}})
	require.NoError(t, err)
	require.Len(t, outs, 1)
	rows, ok := d.paramss[0]["rows"].([]any)
	require.True(t, ok)
	assert.Len(t, rows, 1, "nil entries must be skipped from the batch")
}

// TestFake_BatchCreate_Empty 空批量直接返回（无语句执行）。
func TestFake_BatchCreate_Empty(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)

	outs, err := repo.BatchCreate(fcCtx7, nil)
	assert.NoError(t, err)
	assert.Nil(t, outs)
	assert.Empty(t, d.cyphers)

	outs, err = repo.BatchCreate(fcCtx7, []*fcEntity{})
	assert.NoError(t, err)
	assert.Nil(t, outs)
	assert.Empty(t, d.cyphers)
}

// ─────────────────────────────────────────────────────────────────────────────
// 直取路径（GetByUUID）。
// ─────────────────────────────────────────────────────────────────────────────

// TestFake_GetByUUID_TenantMatch 本租户节点取回：element id 条件与租户
// 谓词合并，节点属性与 element id 还原到 DTO。
func TestFake_GetByUUID_TenantMatch(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)
	d.records = []*neo4j.Record{nodeRecord("4:0:9", map[string]any{"tenant_id": int64(7), "Name": "own", "Age": int64(42)})}

	out, err := repo.GetByUUID(fcCtx7, "u1")
	require.NoError(t, err)

	require.Len(t, d.cyphers, 1)
	assert.Equal(t, "MATCH (n:`coll`) WHERE (elementId(n) = $eid) AND n.`tenant_id` = $__tid RETURN n", d.cyphers[0])
	assert.Equal(t, "u1", d.paramss[0]["eid"])
	assert.Equal(t, int64(7), d.paramss[0]["__tid"])
	assert.Equal(t, "own", out.Name)
	assert.Equal(t, int64(42), out.Age)
	assert.Equal(t, "4:0:9", out.UUID)
}

// TestFake_GetByUUID_TenantMismatch 他租户节点与不存在同构（纵深防御：
// 谓词注入失效时客户端校验兜底）。
func TestFake_GetByUUID_TenantMismatch(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)
	d.records = []*neo4j.Record{nodeRecord("4:0:9", map[string]any{"tenant_id": int64(8), "Name": "foreign"})}

	_, err := repo.GetByUUID(fcCtx7, "u1")
	assert.ErrorIs(t, err, ErrPointNotFound)
}

// TestFake_GetByUUID_MissingTenantProperty 缺租户属性的节点视同他租户。
func TestFake_GetByUUID_MissingTenantProperty(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)
	d.records = []*neo4j.Record{nodeRecord("4:0:9", map[string]any{"Name": "x"})}

	_, err := repo.GetByUUID(fcCtx7, "u1")
	assert.ErrorIs(t, err, ErrPointNotFound)
}

// TestFake_GetByUUID_EmptyResult 空结果即未找到。
func TestFake_GetByUUID_EmptyResult(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)

	_, err := repo.GetByUUID(fcCtx7, "u1")
	assert.ErrorIs(t, err, ErrPointNotFound)
	assert.Len(t, d.cyphers, 1, "query still issued with injected predicate")
	assert.Equal(t, int64(7), d.paramss[0]["__tid"])
}

// TestFake_GetByUUID_PlatformNoPredicate 平台视图：无谓词、无保留参数。
func TestFake_GetByUUID_PlatformNoPredicate(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)
	d.records = []*neo4j.Record{nodeRecord("4:0:9", map[string]any{"tenant_id": int64(8), "Name": "any"})}

	out, err := repo.GetByUUID(fcCtxPlatform, "u1")
	require.NoError(t, err)
	assert.Equal(t, "any", out.Name)
	assert.Equal(t, "MATCH (n:`coll`) WHERE elementId(n) = $eid RETURN n", d.cyphers[0])
	assert.NotContains(t, d.paramss[0], "__tid")
}

// TestFake_GetByUUID_PlainEntity 非 tenant 实体：无谓词注入。
func TestFake_GetByUUID_PlainEntity(t *testing.T) {
	d, repo := newFakeRepo[fcPlainEntity](t)
	d.records = []*neo4j.Record{nodeRecord("4:0:9", map[string]any{"Name": "x"})}

	out, err := repo.GetByUUID(fcCtx7, "u1")
	require.NoError(t, err)
	assert.Equal(t, "x", out.Name)
	assert.Equal(t, "MATCH (n:`coll`) WHERE elementId(n) = $eid RETURN n", d.cyphers[0])
	assert.NotContains(t, d.paramss[0], "__tid")
}

// ─────────────────────────────────────────────────────────────────────────────
// 列表路径（Query）。
// ─────────────────────────────────────────────────────────────────────────────

// TestFake_Query_TenantFiltersForeignRows 租户视图：谓词注入，他租户行
// 被客户端校验剔除（纵深防御）。
func TestFake_Query_TenantFiltersForeignRows(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)
	d.records = []*neo4j.Record{
		nodeRecord("e0", map[string]any{"tenant_id": int64(7), "Name": "own"}),
		nodeRecord("e1", map[string]any{"tenant_id": int64(8), "Name": "foreign"}),
	}

	rows, err := repo.Query(fcCtx7, nil)
	require.NoError(t, err)
	require.Len(t, rows, 1, "tenant-7 view must see only its own rows")
	assert.Equal(t, "own", rows[0].Name)

	assert.Equal(t, "MATCH (n:`coll`) WHERE n.`tenant_id` = $__tid RETURN n", d.cyphers[0])
	assert.Equal(t, int64(7), d.paramss[0]["__tid"])
}

// TestFake_Query_CallerWhereMerged 调用方条件与租户谓词合并（括号包裹
// AND），参数表合并传递。
func TestFake_Query_CallerWhereMerged(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)
	caller := map[string]any{"nm": "x"}

	rows, err := repo.Query(fcCtx7, &Query{Where: "n.Name = $nm", Params: caller})
	require.NoError(t, err)
	require.Empty(t, rows)

	assert.Equal(t, "MATCH (n:`coll`) WHERE (n.Name = $nm) AND n.`tenant_id` = $__tid RETURN n", d.cyphers[0])
	assert.Equal(t, "x", d.paramss[0]["nm"])
	assert.Equal(t, int64(7), d.paramss[0]["__tid"])
	assert.Len(t, caller, 1, "caller param map must not be mutated in place")
	assert.NotContains(t, caller, "__tid")
}

// TestFake_Query_PlatformSeesAll 平台视图：无谓词，全量返回。
func TestFake_Query_PlatformSeesAll(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)
	d.records = []*neo4j.Record{
		nodeRecord("e0", map[string]any{"tenant_id": int64(7), "Name": "a"}),
		nodeRecord("e1", map[string]any{"tenant_id": int64(8), "Name": "b"}),
	}

	rows, err := repo.Query(fcCtxPlatform, nil)
	require.NoError(t, err)
	assert.Len(t, rows, 2)
	assert.Equal(t, "MATCH (n:`coll`) RETURN n", d.cyphers[0])
	assert.NotContains(t, d.paramss[0], "__tid")
}

// TestFake_Query_PlainEntityNoPredicate 非 tenant 实体：无谓词注入。
func TestFake_Query_PlainEntityNoPredicate(t *testing.T) {
	d, repo := newFakeRepo[fcPlainEntity](t)
	d.records = []*neo4j.Record{nodeRecord("e0", map[string]any{"Name": "a"})}

	rows, err := repo.Query(fcCtx7, nil)
	require.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Equal(t, "MATCH (n:`coll`) RETURN n", d.cyphers[0])
	assert.NotContains(t, d.paramss[0], "__tid")
}

// TestFake_Query_NonNodeRecordSkipped 非节点记录（类型不符）剔除。
func TestFake_Query_NonNodeRecordSkipped(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)
	d.records = []*neo4j.Record{
		{Keys: []string{"n"}, Values: []any{int64(1)}},
		nodeRecord("e0", map[string]any{"tenant_id": int64(7), "Name": "a"}),
	}

	rows, err := repo.Query(fcCtx7, nil)
	require.NoError(t, err)
	assert.Len(t, rows, 1, "non-node records must be skipped")
	assert.Equal(t, "a", rows[0].Name)
}

// TestFake_Query_MissingViewerFailClosed 缺身份 fail-closed。
func TestFake_Query_MissingViewerFailClosed(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)

	_, err := repo.Query(context.Background(), nil)
	assert.ErrorIs(t, err, viewer.ErrMissingViewer)
	assert.Empty(t, d.cyphers, "no statement may be issued without a viewer")
}

// ─────────────────────────────────────────────────────────────────────────────
// 计数路径（Count / Exists）。
// ─────────────────────────────────────────────────────────────────────────────

// TestFake_Count_TenantPredicate 租户视图计数：谓词注入 + count 回读。
func TestFake_Count_TenantPredicate(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)
	d.records = []*neo4j.Record{countRecord(42)}

	n, err := repo.Count(fcCtx7, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(42), n)
	assert.Equal(t, "MATCH (n:`coll`) WHERE n.`tenant_id` = $__tid RETURN count(n)", d.cyphers[0])
	assert.Equal(t, int64(7), d.paramss[0]["__tid"])
}

// TestFake_Count_PlatformNoPredicate 平台视图无谓词。
func TestFake_Count_PlatformNoPredicate(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)
	d.records = []*neo4j.Record{countRecord(42)}

	n, err := repo.Count(fcCtxPlatform, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(42), n)
	assert.Equal(t, "MATCH (n:`coll`) RETURN count(n)", d.cyphers[0])
	assert.NotContains(t, d.paramss[0], "__tid")
}

// TestFake_Count_PlainEntityNoPredicate 非 tenant 实体无谓词。
func TestFake_Count_PlainEntityNoPredicate(t *testing.T) {
	d, repo := newFakeRepo[fcPlainEntity](t)
	d.records = []*neo4j.Record{countRecord(3)}

	n, err := repo.Count(fcCtx7, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(3), n)
	assert.Equal(t, "MATCH (n:`coll`) RETURN count(n)", d.cyphers[0])
	assert.NotContains(t, d.paramss[0], "__tid")
}

// TestFake_Count_EmptyOrNonIntRecord 空记录或非整数 count 值 → 0。
func TestFake_Count_EmptyOrNonIntRecord(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)

	n, err := repo.Count(fcCtx7, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)

	d.records = []*neo4j.Record{{Keys: []string{"count(n)"}, Values: []any{"not-an-int"}}}
	n, err = repo.Count(fcCtx7, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

// TestFake_Exists 计数为正即存在。
func TestFake_Exists(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)
	d.records = []*neo4j.Record{countRecord(0)}

	ok, err := repo.Exists(fcCtx7, nil)
	require.NoError(t, err)
	assert.False(t, ok)
}

// ─────────────────────────────────────────────────────────────────────────────
// 删除路径。
// ─────────────────────────────────────────────────────────────────────────────

// TestFake_DeleteByUUIDs_TenantScoped 按 element id 删除：预计数与删除
// 同条件（均含租户谓词），只删本租户节点。
func TestFake_DeleteByUUIDs_TenantScoped(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)
	d.records = []*neo4j.Record{countRecord(1)}

	n, err := repo.DeleteByUUIDs(fcCtx7, []string{"u1"})
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	require.Len(t, d.cyphers, 2)
	assert.Equal(t, "MATCH (n:`coll`) WHERE (elementId(n) IN $eids) AND n.`tenant_id` = $__tid RETURN count(n)", d.cyphers[0])
	assert.Equal(t, "MATCH (n:`coll`) WHERE (elementId(n) IN $eids) AND n.`tenant_id` = $__tid DETACH DELETE n", d.cyphers[1])
	assert.Equal(t, []string{"u1"}, d.paramss[1]["eids"])
	assert.Equal(t, int64(7), d.paramss[1]["__tid"])
}

// TestFake_DeleteByUUIDs_CrossTenantPrecountZero 越权删除：预计数为 0
// → 不发删除语句。
func TestFake_DeleteByUUIDs_CrossTenantPrecountZero(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)
	d.records = []*neo4j.Record{countRecord(0)}

	n, err := repo.DeleteByUUIDs(fcCtx7, []string{"u1"})
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
	assert.Len(t, d.cyphers, 1, "pre-count only; the delete statement must not be issued")
}

// TestFake_DeleteByUUIDs_PlatformNoPredicate 平台视图无谓词。
func TestFake_DeleteByUUIDs_PlatformNoPredicate(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)
	d.records = []*neo4j.Record{countRecord(1)}

	n, err := repo.DeleteByUUIDs(fcCtxPlatform, []string{"u1"})
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	assert.Equal(t, "MATCH (n:`coll`) WHERE elementId(n) IN $eids RETURN count(n)", d.cyphers[0])
	assert.Equal(t, "MATCH (n:`coll`) WHERE elementId(n) IN $eids DETACH DELETE n", d.cyphers[1])
	assert.NotContains(t, d.paramss[0], "__tid")
}

// TestFake_DeleteByUUIDs_EmptyList 空列表直接返回。
func TestFake_DeleteByUUIDs_EmptyList(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)

	n, err := repo.DeleteByUUIDs(fcCtx7, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
	assert.Empty(t, d.cyphers)
}

// TestFake_DeleteByWhere_TenantMerged 原生 WHERE 删除：条件与租户谓词
// 合并，预计数与删除同条件。
func TestFake_DeleteByWhere_TenantMerged(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)
	d.records = []*neo4j.Record{countRecord(1)}
	caller := map[string]any{"age": int64(3)}

	n, err := repo.DeleteByWhere(fcCtx7, &Query{Where: "n.age > $age", Params: caller})
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	require.Len(t, d.cyphers, 2)
	const merged = "MATCH (n:`coll`) WHERE (n.age > $age) AND n.`tenant_id` = $__tid "
	assert.Equal(t, merged+"RETURN count(n)", d.cyphers[0])
	assert.Equal(t, merged+"DETACH DELETE n", d.cyphers[1])
	assert.Equal(t, int64(3), d.paramss[1]["age"])
	assert.Equal(t, int64(7), d.paramss[1]["__tid"])
	assert.Len(t, caller, 1, "caller param map must not be mutated in place")
	assert.NotContains(t, caller, "__tid")
}

// TestFake_DeleteByWhere_PlainEntityNoPredicate 非 tenant 实体无谓词。
func TestFake_DeleteByWhere_PlainEntityNoPredicate(t *testing.T) {
	d, repo := newFakeRepo[fcPlainEntity](t)
	d.records = []*neo4j.Record{countRecord(1)}

	n, err := repo.DeleteByWhere(fcCtx7, &Query{Where: "n.age > $age", Params: map[string]any{"age": int64(3)}})
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	assert.Equal(t, "MATCH (n:`coll`) WHERE n.age > $age RETURN count(n)", d.cyphers[0])
	assert.Equal(t, "MATCH (n:`coll`) WHERE n.age > $age DETACH DELETE n", d.cyphers[1])
	assert.NotContains(t, d.paramss[0], "__tid")
}

// TestFake_DeleteByWhere_MissingViewerFailClosed 缺身份 fail-closed。
func TestFake_DeleteByWhere_MissingViewerFailClosed(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)

	_, err := repo.DeleteByWhere(context.Background(), &Query{Where: "n.age > 1"})
	assert.ErrorIs(t, err, viewer.ErrMissingViewer)
	assert.Empty(t, d.cyphers)
}

// ─────────────────────────────────────────────────────────────────────────────
// 错误包装。
// ─────────────────────────────────────────────────────────────────────────────

// TestFake_RunErrors 自动提交语句失败的哨兵包装（预计数失败/删除失败分开）。
// 每用例前清空捕获面，使 runErrs 的序号在每个操作内部从 0 起算。
func TestFake_RunErrors(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)
	boom := errors.New("boom")
	flush := func() {
		d.cyphers = nil
		d.paramss = nil
	}

	flush()
	d.runErrs = map[int]error{0: boom}
	_, err := repo.Create(fcCtx7, &fcEntity{Name: "x"})
	assert.ErrorIs(t, err, ErrInsertFailed)

	flush()
	d.runErrs = map[int]error{0: boom}
	_, err = repo.BatchCreate(fcCtx7, []*fcEntity{{Name: "x"}})
	assert.ErrorIs(t, err, ErrInsertFailed)

	flush()
	d.runErrs = map[int]error{0: boom}
	_, err = repo.GetByUUID(fcCtx7, "u1")
	assert.ErrorIs(t, err, ErrQueryFailed)

	flush()
	d.runErrs = map[int]error{0: boom}
	_, err = repo.Query(fcCtx7, nil)
	assert.ErrorIs(t, err, ErrQueryFailed)

	flush()
	d.runErrs = map[int]error{0: boom}
	_, err = repo.Count(fcCtx7, nil)
	assert.ErrorIs(t, err, ErrCountFailed)

	flush()
	d.runErrs = map[int]error{0: boom} // 预计数失败
	_, err = repo.DeleteByUUIDs(fcCtx7, []string{"u1"})
	assert.ErrorIs(t, err, ErrCountFailed)

	flush()
	d.records = []*neo4j.Record{countRecord(1)}
	d.runErrs = map[int]error{1: boom} // 预计数成功、删除失败
	_, err = repo.DeleteByUUIDs(fcCtx7, []string{"u1"})
	assert.ErrorIs(t, err, ErrDeleteFailed)

	flush()
	d.runErrs = map[int]error{1: boom} // DeleteByWhere 同路径
	_, err = repo.DeleteByWhere(fcCtx7, &Query{Where: "n.age > 1"})
	assert.ErrorIs(t, err, ErrDeleteFailed)
}

// TestFake_CollectError Collect 失败按读取失败包装。
func TestFake_CollectError(t *testing.T) {
	d, repo := newFakeRepo[fcEntity](t)
	d.collectErr = errors.New("collect boom")

	_, err := repo.Query(fcCtx7, nil)
	assert.ErrorIs(t, err, ErrQueryFailed)
	_, err = repo.Count(fcCtx7, nil)
	assert.ErrorIs(t, err, ErrCountFailed)
}
