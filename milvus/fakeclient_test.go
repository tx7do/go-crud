package milvus

import (
	"context"
	"errors"
	"testing"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tx7do/go-utils/mapper"
	"github.com/tx7do/go-wind/log"

	"github.com/tx7do/go-crud/milvus/mixin"
	"github.com/tx7do/go-crud/vector"
	"github.com/tx7do/go-crud/viewer"
)

// ─────────────────────────────────────────────────────────────────────────────
// fake client 离线测试：
//
// milvus SDK 的 client.Client 是接口，注入替身后可在离线环境锁定仓库层的
// 全部协议交互语义——写入列组装（含租户强制落列）、按主键直取的租户校验、
// 表达式注入与行过滤、count(*)/回退计数、删除前的租户过滤、向量检索的
// 请求映射（字段/度量/TopK/注入）与命中过滤（租户剔除、MinScore、L2 分数
// 换算）。网络相关的集成链路另见 KRATOS_IT 门禁测试。
// ─────────────────────────────────────────────────────────────────────────────

// fcEntity fake client 测试实体：数值主键 + 标量 + 向量字段 + TenantID mixin。
type fcEntity struct {
	ID    int64
	Title string
	Emb   []float32
	mixin.TenantID
}

// fcPlainEntity 非 tenant-scoped 实体（无 mixin）。
type fcPlainEntity struct {
	ID    int64
	Title string
	Emb   []float32
}

// fcUuidEntity VarChar 主键 + TenantID mixin（UUID 主键路径的租户过滤）。
type fcUuidEntity struct {
	UUID  string
	Title string
	Emb   []float32
	mixin.TenantID
}

// fcCtx7 / fcCtxPlatform 租户 7 与平台视图上下文。
var (
	fcCtx7        = viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	fcCtxPlatform = viewer.WithContext(context.Background(), testEnforceViewer{tid: 0, platform: true})
)

// fakeMilvusClient 记录调用参数并按预设回放结果的 milvus 客户端替身。
// 嵌入 client.Client 接口：未覆盖的方法一旦被调用即 panic，测试仅触达覆盖面。
type fakeMilvusClient struct {
	client.Client

	// 捕获面。
	hasCollName     string
	dropCalls       int
	upsertCols      [][]entity.Column
	deletePkCalls   int
	deletePkCol     entity.Column
	deleteExpr      string
	queryExpr       string
	queryOutput     []string
	pkQueryOutput   []string
	searchExpr      string
	searchOutput    []string
	searchField     string
	searchMetric    entity.MetricType
	searchTopK      int
	searchSp        entity.SearchParam
	searchVectors   []entity.Vector
	createSchema    *entity.Schema
	createShards    int32
	indexField      string
	indexObj        entity.Index
	indexMetricType string
	loadCalls       int
	hasColl         bool

	// 回放面（按各查询方法的输出字段集区分）。
	pkRows      client.ResultSet      // QueryByPks 回放（主键/租户判定取行）
	pkQueryErr  error                 // QueryByPks 回放错误
	queryRows   client.ResultSet      // Query（非 count(*) 输出）回放
	queryErr    error                 // Query（非 count(*) 输出）回放错误
	countRows   client.ResultSet      // Query（count(*) 聚合）回放
	countErr    error                 // Query（count(*) 聚合）回放错误
	searchRes   []client.SearchResult // Search 回放
	searchFail  error                 // Search 回放错误
	upsertErr   error
	deletePkErr error
	deleteErr   error
	createErr   error
	indexErr    error
	loadErr     error
}

func (f *fakeMilvusClient) HasCollection(_ context.Context, collName string) (bool, error) {
	f.hasCollName = collName
	return f.hasColl, nil
}

func (f *fakeMilvusClient) DropCollection(_ context.Context, _ string, _ ...client.DropCollectionOption) error {
	f.dropCalls++
	return nil
}

func (f *fakeMilvusClient) CreateCollection(_ context.Context, collSchema *entity.Schema, shardNum int32, _ ...client.CreateCollectionOption) error {
	f.createSchema = collSchema
	f.createShards = shardNum
	return f.createErr
}

func (f *fakeMilvusClient) CreateIndex(_ context.Context, _ string, fieldName string, idx entity.Index, _ bool, _ ...client.IndexOption) error {
	f.indexField = fieldName
	f.indexObj = idx
	if auto, ok := idx.(*entity.IndexAUTOINDEX); ok {
		f.indexMetricType = auto.Params()["metric_type"]
	}
	return f.indexErr
}

func (f *fakeMilvusClient) LoadCollection(_ context.Context, _ string, _ bool, _ ...client.LoadCollectionOption) error {
	f.loadCalls++
	return f.loadErr
}

func (f *fakeMilvusClient) Upsert(_ context.Context, _ string, _ string, columns ...entity.Column) (entity.Column, error) {
	f.upsertCols = append(f.upsertCols, columns)
	return nil, f.upsertErr
}

func (f *fakeMilvusClient) DeleteByPks(_ context.Context, _ string, _ string, ids entity.Column) error {
	f.deletePkCalls++
	f.deletePkCol = ids
	return f.deletePkErr
}

func (f *fakeMilvusClient) Delete(_ context.Context, _ string, _ string, expr string) error {
	f.deleteExpr = expr
	return f.deleteErr
}

func (f *fakeMilvusClient) QueryByPks(_ context.Context, _ string, _ []string, _ entity.Column, outputFields []string, _ ...client.SearchQueryOptionFunc) (client.ResultSet, error) {
	f.pkQueryOutput = outputFields
	return f.pkRows, f.pkQueryErr
}

func (f *fakeMilvusClient) Query(_ context.Context, _ string, _ []string, expr string, outputFields []string, _ ...client.SearchQueryOptionFunc) (client.ResultSet, error) {
	f.queryExpr = expr
	f.queryOutput = outputFields
	if len(outputFields) > 0 && outputFields[0] == "count(*)" {
		return f.countRows, f.countErr
	}
	return f.queryRows, f.queryErr
}

func (f *fakeMilvusClient) Search(
	_ context.Context, _ string, _ []string, expr string, outputFields []string,
	vectors []entity.Vector, vectorField string, metricType entity.MetricType,
	topK int, sp entity.SearchParam, _ ...client.SearchQueryOptionFunc,
) ([]client.SearchResult, error) {
	f.searchExpr = expr
	f.searchOutput = outputFields
	f.searchField = vectorField
	f.searchMetric = metricType
	f.searchTopK = topK
	f.searchSp = sp
	f.searchVectors = vectors
	return f.searchRes, f.searchFail
}

func (f *fakeMilvusClient) CheckHealth(_ context.Context) (*entity.MilvusState, error) {
	return &entity.MilvusState{IsHealthy: true}, nil
}

func (f *fakeMilvusClient) Close() error { return nil }

// newFakeRepo 构建绑定 fake client 的仓库（集合名固定 coll）。
func newFakeRepo[ENTITY any](t *testing.T) (*fakeMilvusClient, *Repository[ENTITY, ENTITY]) {
	t.Helper()
	fc := &fakeMilvusClient{}
	c, err := NewClient(WithMilvusClient(fc))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	m := mapper.NewCopierMapper[ENTITY, ENTITY]()
	return fc, NewRepository[ENTITY, ENTITY](c, "coll", m, log.GetLogger())
}

// fcColsByName 按列名索引捕获的列集。
func fcColsByName(t *testing.T, cols []entity.Column) map[string]entity.Column {
	t.Helper()
	out := make(map[string]entity.Column, len(cols))
	for _, c := range cols {
		if c != nil {
			out[c.Name()] = c
		}
	}
	return out
}

// ─── 客户端探活与集合存在性 ─────────────────────────────────────────────────

func TestFakeClient_CheckConnect(t *testing.T) {
	fc := &fakeMilvusClient{}
	c, err := NewClient(WithMilvusClient(fc))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	assert.True(t, c.CheckConnect())
}

func TestFakeClient_HasAndDropCollection(t *testing.T) {
	fc, repo := newFakeRepo[fcEntity](t)
	ctx := context.Background()

	fc.hasColl = true
	ok, err := repo.HasCollection(ctx)
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, "coll", fc.hasCollName, "集合名须透传绑定名")

	fc.hasColl = false
	ok, err = repo.HasCollection(ctx)
	require.NoError(t, err)
	assert.False(t, ok)

	require.NoError(t, repo.DropCollection(ctx))
	assert.Equal(t, 1, fc.dropCalls)
}

// ─── 集合创建：schema / AUTOINDEX / 加载 ────────────────────────────────────

func TestFakeClient_CreateCollection_SchemaAndIndex(t *testing.T) {
	fc, repo := newFakeRepo[fcEntity](t)

	// 度量显式 euclidean → L2。
	require.NoError(t, repo.CreateCollection(context.Background(), 4, vector.MetricEuclidean))

	require.NotNil(t, fc.createSchema)
	assert.Equal(t, "coll", fc.createSchema.CollectionName)
	assert.Equal(t, int32(entity.DefaultShardNumber), fc.createShards)

	// schema 映射细节已在 schema_test 锁定，此处核对经仓库路径的产出一致。
	pk := findField(t, fc.createSchema, "ID")
	require.NotNil(t, pk)
	assert.True(t, pk.PrimaryKey)
	assert.False(t, pk.AutoID)
	assert.Equal(t, entity.FieldTypeInt64, pk.DataType)
	tenant := findField(t, fc.createSchema, "tenant_id")
	require.NotNil(t, tenant)
	assert.True(t, tenant.IsPartitionKey)
	emb := findField(t, fc.createSchema, "Emb")
	require.NotNil(t, emb)
	assert.Equal(t, entity.FieldTypeFloatVector, emb.DataType)
	assert.Equal(t, "4", emb.TypeParams[entity.TypeParamDim])

	// 向量字段建 AUTOINDEX 索引并加载集合。
	assert.Equal(t, "Emb", fc.indexField)
	require.NotNil(t, fc.indexObj)
	auto, ok := fc.indexObj.(*entity.IndexAUTOINDEX)
	require.True(t, ok, "索引对象须为 AUTOINDEX")
	assert.Equal(t, entity.AUTOINDEX, auto.IndexType())
	assert.Equal(t, string(entity.L2), fc.indexMetricType)
	assert.Equal(t, 1, fc.loadCalls)
}

func TestFakeClient_CreateCollection_DefaultMetric(t *testing.T) {
	// 度量未指定 → cosine（与建索引约定一致）。
	fc, repo := newFakeRepo[fcEntity](t)
	require.NoError(t, repo.CreateCollection(context.Background(), 4, ""))
	assert.Equal(t, string(entity.COSINE), fc.indexMetricType)
}

// ─── 写入：租户强制落列 ─────────────────────────────────────────────────────

func TestFakeClient_Create_TenantForced(t *testing.T) {
	fc, repo := newFakeRepo[fcEntity](t)

	created, err := repo.Create(fcCtx7, &fcEntity{ID: 101, Title: "a7", Emb: []float32{1, 0, 0, 0}})
	require.NoError(t, err)

	// EnforceOnScopedInstance 强制覆盖的 tenant_id 随列写入。
	tid := created.TenantID.GetTenantID()
	require.NotNil(t, tid)
	assert.Equal(t, uint32(7), *tid)

	// 单次 Upsert；列集与 schema 字段一一对应（四列）。
	require.Len(t, fc.upsertCols, 1)
	cols := fcColsByName(t, fc.upsertCols[0])
	require.Len(t, cols, 4)

	idc, ok := cols["ID"].(*entity.ColumnInt64)
	require.True(t, ok)
	assert.Equal(t, []int64{101}, idc.Data())
	tenantCol, ok := cols["tenant_id"].(*entity.ColumnInt64)
	require.True(t, ok)
	assert.Equal(t, []int64{7}, tenantCol.Data(), "租户列须为强制覆盖后的 7")
	// VarChar 字段经 coerceStringColumnsToVarChar 矫正为 ColumnVarChar：
	// SDK AnyToColumns 对 VarChar 字段产出 ColumnString（entity/rows.go），
	// 服务端要求 VarChar 字段收取 VarChar 列，写入路径统一矫正。
	titleCol, ok := cols["Title"].(*entity.ColumnVarChar)
	require.True(t, ok)
	assert.Equal(t, []string{"a7"}, titleCol.Data())
	vecCol, ok := cols["Emb"].(*entity.ColumnFloatVector)
	require.True(t, ok)
	assert.Equal(t, 4, vecCol.Dim())
	assert.Equal(t, 1, vecCol.Len())
}

func TestFakeClient_BatchCreate_SingleUpsert(t *testing.T) {
	fc, repo := newFakeRepo[fcEntity](t)

	created, err := repo.BatchCreate(fcCtx7, []*fcEntity{
		{ID: 101, Title: "a7", Emb: []float32{1, 0, 0, 0}},
		{ID: 102, Title: "b7", Emb: []float32{0, 1, 0, 0}},
	})
	require.NoError(t, err)
	require.Len(t, created, 2)
	for _, dto := range created {
		tid := dto.TenantID.GetTenantID()
		require.NotNil(t, tid)
		assert.Equal(t, uint32(7), *tid)
	}

	// 批量实体合并为单次 Upsert，各列承载两行。
	require.Len(t, fc.upsertCols, 1)
	for _, c := range fc.upsertCols[0] {
		assert.Equal(t, 2, c.Len(), "批量列须承载全部行")
	}
	tenantCol, ok := fcColsByName(t, fc.upsertCols[0])["tenant_id"].(*entity.ColumnInt64)
	require.True(t, ok)
	assert.Equal(t, []int64{7, 7}, tenantCol.Data(), "全部行租户均强制覆盖为 7")
}

// ─── 按主键直取：客户端租户校验 ─────────────────────────────────────────────

func TestFakeClient_Get_TenantCheck(t *testing.T) {
	fc, repo := newFakeRepo[fcEntity](t)

	// 本租户行可取回；输出字段集不含向量字段。
	fc.pkRows = client.ResultSet{
		entity.NewColumnInt64("ID", []int64{101}),
		entity.NewColumnVarChar("Title", []string{"a7"}),
		entity.NewColumnInt64("tenant_id", []int64{7}),
	}
	dto, err := repo.Get(fcCtx7, 101)
	require.NoError(t, err)
	assert.Equal(t, int64(101), dto.ID)
	assert.Equal(t, "a7", dto.Title)
	assert.Equal(t, []string{"ID", "Title", "tenant_id"}, fc.pkQueryOutput, "输出字段集须排除向量字段")

	// 他租户行与不存在同构（ErrPointNotFound）。
	fc.pkRows = client.ResultSet{
		entity.NewColumnInt64("ID", []int64{201}),
		entity.NewColumnInt64("tenant_id", []int64{8}),
	}
	_, err = repo.Get(fcCtx7, 201)
	assert.ErrorIs(t, err, ErrPointNotFound)

	// tenant 列缺失的行视同他租户。
	fc.pkRows = client.ResultSet{entity.NewColumnInt64("ID", []int64{201})}
	_, err = repo.Get(fcCtx7, 201)
	assert.ErrorIs(t, err, ErrPointNotFound)

	// 平台视图：不注入不校验，按原样取回。
	fc.pkRows = client.ResultSet{
		entity.NewColumnInt64("ID", []int64{201}),
		entity.NewColumnVarChar("Title", []string{"a8"}),
		entity.NewColumnInt64("tenant_id", []int64{8}),
	}
	dto, err = repo.Get(fcCtxPlatform, 201)
	require.NoError(t, err)
	assert.Equal(t, int64(201), dto.ID)

	// 缺 ViewerContext → fail-closed。
	_, err = repo.Get(context.Background(), 201)
	assert.ErrorIs(t, err, viewer.ErrMissingViewer)
}

func TestFakeClient_NonScopedGet(t *testing.T) {
	fc, repo := newFakeRepo[fcPlainEntity](t)

	// 非 tenant-scoped：无 tenant 列照常取回，输出字段集亦无 tenant_id。
	fc.pkRows = client.ResultSet{
		entity.NewColumnInt64("ID", []int64{1}),
		entity.NewColumnVarChar("Title", []string{"x"}),
	}
	dto, err := repo.Get(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, "x", dto.Title)
	assert.Equal(t, []string{"ID", "Title"}, fc.pkQueryOutput)
}

// ─── 表达式查询：注入与行过滤 ───────────────────────────────────────────────

func TestFakeClient_QueryByExpr_TenantInjectAndFilter(t *testing.T) {
	fc, repo := newFakeRepo[fcEntity](t)
	fc.queryRows = client.ResultSet{
		entity.NewColumnInt64("ID", []int64{101, 201}),
		entity.NewColumnVarChar("Title", []string{"a7", "a8"}),
		entity.NewColumnInt64("tenant_id", []int64{7, 8}),
	}

	// 租户谓词与调用方表达式 AND 合并；他租户行剔除。
	rows, err := repo.QueryByExpr(fcCtx7, "status == 1")
	require.NoError(t, err)
	assert.Equal(t, "(status == 1) and tenant_id == 7", fc.queryExpr)
	require.Len(t, rows, 1)
	assert.Equal(t, int64(101), rows[0].ID)
	assert.Equal(t, "a7", rows[0].Title)

	// 空表达式 → 仅注入的租户谓词。
	rows, err = repo.QueryByExpr(fcCtx7, "")
	require.NoError(t, err)
	assert.Equal(t, "tenant_id == 7", fc.queryExpr)
	require.Len(t, rows, 1)

	// 平台视图：表达式原样透传，全部行可见。
	rows, err = repo.QueryByExpr(fcCtxPlatform, "status == 1")
	require.NoError(t, err)
	assert.Equal(t, "status == 1", fc.queryExpr)
	require.Len(t, rows, 2)

	// 缺 ViewerContext → fail-closed。
	_, err = repo.QueryByExpr(context.Background(), "status == 1")
	assert.ErrorIs(t, err, viewer.ErrMissingViewer)
}

func TestFakeClient_NonScopedQueryByExpr(t *testing.T) {
	fc, repo := newFakeRepo[fcPlainEntity](t)
	fc.queryRows = client.ResultSet{
		entity.NewColumnInt64("ID", []int64{1, 2}),
		entity.NewColumnVarChar("Title", []string{"x", "y"}),
	}

	// 非 tenant-scoped：不注入，全部行可见。
	rows, err := repo.QueryByExpr(context.Background(), "status == 1")
	require.NoError(t, err)
	assert.Equal(t, "status == 1", fc.queryExpr)
	require.Len(t, rows, 2)

	// 空表达式且无注入面 → 拒绝。
	_, err = repo.QueryByExpr(context.Background(), "")
	assert.ErrorIs(t, err, ErrInvalidRequest)
}

// ─── 计数与存在性 ───────────────────────────────────────────────────────────

func TestFakeClient_Count_AggregateAndFallback(t *testing.T) {
	// count(*) 聚合路径：表达式已注入租户谓词。
	fc, repo := newFakeRepo[fcEntity](t)
	fc.countRows = client.ResultSet{entity.NewColumnInt64("count(*)", []int64{42})}
	n, err := repo.Count(fcCtx7, "")
	require.NoError(t, err)
	assert.Equal(t, int64(42), n)
	assert.Equal(t, "tenant_id == 7", fc.queryExpr)
	assert.Equal(t, []string{"count(*)"}, fc.queryOutput)

	// 回退路径：count(*) 不支持 → 按主键列取行计数。
	fc2, repo2 := newFakeRepo[fcEntity](t)
	fc2.countErr = errors.New("count(*) unsupported")
	fc2.queryRows = client.ResultSet{entity.NewColumnInt64("ID", []int64{1, 2})}
	n, err = repo2.Count(fcCtx7, "")
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
	assert.Equal(t, []string{"ID"}, fc2.queryOutput)

	// 双路径皆败 → ErrCountFailed。
	fc3, repo3 := newFakeRepo[fcEntity](t)
	fc3.countErr = errors.New("no count")
	fc3.queryErr = errors.New("no query")
	_, err = repo3.Count(fcCtx7, "")
	assert.ErrorIs(t, err, ErrCountFailed)
}

func TestFakeClient_Exists(t *testing.T) {
	fc, repo := newFakeRepo[fcEntity](t)

	fc.countRows = client.ResultSet{entity.NewColumnInt64("count(*)", []int64{1})}
	exists, err := repo.Exists(fcCtx7, "")
	require.NoError(t, err)
	assert.True(t, exists)

	fc.countRows = client.ResultSet{entity.NewColumnInt64("count(*)", []int64{0})}
	exists, err = repo.Exists(fcCtx7, "")
	require.NoError(t, err)
	assert.False(t, exists)

	// 计数失败向上传播。
	fc2, repo2 := newFakeRepo[fcEntity](t)
	fc2.countErr = errors.New("no count")
	fc2.queryErr = errors.New("no query")
	exists, err = repo2.Exists(fcCtx7, "")
	assert.False(t, exists)
	assert.ErrorIs(t, err, ErrCountFailed)
}

// ─── 删除：先过滤后删 ───────────────────────────────────────────────────────

func TestFakeClient_DeleteByIDs_TenantFilter(t *testing.T) {
	// 剔除他租户行后仅删本租户点。
	fc, repo := newFakeRepo[fcEntity](t)
	fc.pkRows = client.ResultSet{
		entity.NewColumnInt64("ID", []int64{101, 201}),
		entity.NewColumnInt64("tenant_id", []int64{7, 8}),
	}
	n, err := repo.DeleteByIDs(fcCtx7, []uint64{101, 201})
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	assert.Equal(t, 1, fc.deletePkCalls)
	dc, ok := fc.deletePkCol.(*entity.ColumnInt64)
	require.True(t, ok)
	assert.Equal(t, []int64{101}, dc.Data(), "删除列仅含本租户主键")

	// 全为他租户行 → 0 删除、不触 DeleteByPks。
	fc2, repo2 := newFakeRepo[fcEntity](t)
	fc2.pkRows = client.ResultSet{
		entity.NewColumnInt64("ID", []int64{201}),
		entity.NewColumnInt64("tenant_id", []int64{8}),
	}
	n, err = repo2.DeleteByIDs(fcCtx7, []uint64{201})
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
	assert.Equal(t, 0, fc2.deletePkCalls)

	// 平台视图：不过滤，全量删除。
	fc3, repo3 := newFakeRepo[fcEntity](t)
	fc3.pkRows = client.ResultSet{
		entity.NewColumnInt64("ID", []int64{101, 201}),
		entity.NewColumnInt64("tenant_id", []int64{7, 8}),
	}
	n, err = repo3.DeleteByIDs(fcCtxPlatform, []uint64{101, 201})
	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
	dc3, ok := fc3.deletePkCol.(*entity.ColumnInt64)
	require.True(t, ok)
	assert.Equal(t, []int64{101, 201}, dc3.Data())

	// 非 tenant-scoped：无过滤，直接删除。
	fc4, repo4 := newFakeRepo[fcPlainEntity](t)
	fc4.pkRows = client.ResultSet{entity.NewColumnInt64("ID", []int64{1})}
	n, err = repo4.DeleteByIDs(context.Background(), []uint64{1})
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
}

func TestFakeClient_DeleteByUUIDs_TenantFilterAndQuoteGuard(t *testing.T) {
	fc, repo := newFakeRepo[fcUuidEntity](t)
	fc.pkRows = client.ResultSet{
		entity.NewColumnVarChar("UUID", []string{"u1", "u2"}),
		entity.NewColumnInt64("tenant_id", []int64{7, 8}),
	}

	// VarChar 主键路径：剔除他租户行。
	n, err := repo.DeleteByUUIDs(fcCtx7, []string{"u1", "u2"})
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)
	dc, ok := fc.deletePkCol.(*entity.ColumnVarChar)
	require.True(t, ok)
	assert.Equal(t, []string{"u1"}, dc.Data(), "删除列仅含本租户主键")

	// 值含引号/反斜杠 → 注入面拒绝，且不触任何查询/删除。
	_, err = repo.DeleteByUUIDs(fcCtx7, []string{"a\"b"})
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = repo.DeleteByUUIDs(fcCtx7, []string{"a\\b"})
	assert.ErrorIs(t, err, ErrInvalidRequest)
	assert.Equal(t, 1, fc.deletePkCalls, "引号拒绝须先于取行查询")
}

func TestFakeClient_DeleteByExpr(t *testing.T) {
	fc, repo := newFakeRepo[fcEntity](t)
	fc.countRows = client.ResultSet{entity.NewColumnInt64("count(*)", []int64{3})}

	// 注入后的表达式同时用于预计数与删除。
	n, err := repo.DeleteByExpr(fcCtx7, "status == 1")
	require.NoError(t, err)
	assert.Equal(t, int64(3), n)
	assert.Equal(t, "(status == 1) and tenant_id == 7", fc.deleteExpr)
	assert.Equal(t, "(status == 1) and tenant_id == 7", fc.queryExpr)

	// 非 scoped 实体 + 空表达式 → 拒绝（无注入面）。
	_, repo2 := newFakeRepo[fcPlainEntity](t)
	_, err = repo2.DeleteByExpr(context.Background(), "")
	assert.ErrorIs(t, err, ErrInvalidRequest)
}

// ─── 向量检索：请求映射与命中过滤 ───────────────────────────────────────────

func TestFakeClient_SearchByVector_FiltersAndMapping(t *testing.T) {
	fc, repo := newFakeRepo[fcEntity](t)
	fc.searchRes = []client.SearchResult{{
		ResultCount: 3,
		IDs:         entity.NewColumnInt64("ID", []int64{101, 102, 201}),
		Fields: client.ResultSet{
			entity.NewColumnInt64("tenant_id", []int64{7, 7, 8}),
			entity.NewColumnVarChar("Title", []string{"a7", "b7", "a8"}),
		},
		// 行 0：本租户、高分；行 1：本租户、低分（MinScore 剔除）；
		// 行 2：他租户（租户校验剔除）。
		Scores: []float32{0.9, 0.2, 0.95},
	}}

	res, err := repo.SearchByVector(fcCtx7, &vector.Query{
		Field:    "Emb",
		Vector:   []float32{1, 0, 0, 0},
		TopK:     5,
		MinScore: 0.5,
	})
	require.NoError(t, err)

	// 请求映射：注入租户、输出字段排除向量、字段/度量/TopK/检索参数透传。
	assert.Equal(t, "tenant_id == 7", fc.searchExpr)
	assert.Equal(t, []string{"ID", "Title", "tenant_id"}, fc.searchOutput)
	assert.Equal(t, "Emb", fc.searchField)
	assert.Equal(t, entity.COSINE, fc.searchMetric)
	assert.Equal(t, 5, fc.searchTopK)
	require.NotNil(t, fc.searchSp, "检索参数须透传 AUTOINDEX 检索参数")
	require.Len(t, fc.searchVectors, 1)
	fv, ok := fc.searchVectors[0].(entity.FloatVector)
	require.True(t, ok)
	assert.Equal(t, []float32{1, 0, 0, 0}, []float32(fv))

	// 命中过滤：仅行 0 存留（他租户与低分均被剔除）。
	require.Len(t, res.Hits, 1)
	assert.Equal(t, int64(101), res.Hits[0].Value.ID)
	assert.Equal(t, "a7", res.Hits[0].Value.Title)
	assert.InDelta(t, 0.9, res.Hits[0].Score, 1e-6)
	assert.Equal(t, int64(1), res.Total)
}

func TestFakeClient_SearchByVector_L2ScoreConversion(t *testing.T) {
	// L2 距离 → 1/(1+d)；MinScore 0 不过滤。
	fc, repo := newFakeRepo[fcEntity](t)
	fc.searchRes = []client.SearchResult{{
		IDs: entity.NewColumnInt64("ID", []int64{301, 302}),
		Fields: client.ResultSet{
			entity.NewColumnInt64("tenant_id", []int64{7, 7}),
			entity.NewColumnVarChar("Title", []string{"c7", "d7"}),
		},
		Scores: []float32{2.0, 0.0},
	}}

	res, err := repo.SearchByVector(fcCtx7, &vector.Query{
		Field:  "Emb",
		Vector: []float32{1, 0, 0, 0},
		TopK:   5,
		Metric: vector.MetricEuclidean,
	})
	require.NoError(t, err)
	assert.Equal(t, entity.L2, fc.searchMetric)
	require.Len(t, res.Hits, 2)
	assert.InDelta(t, 1.0/3.0, res.Hits[0].Score, 1e-9)
	assert.Equal(t, 1.0, res.Hits[1].Score)
}

func TestFakeClient_SearchByVector_PartialErrorsAndScoreBounds(t *testing.T) {
	// 多结果段：错误段跳过、无分数行跳过、有效段命中。
	fc, repo := newFakeRepo[fcEntity](t)
	fc.searchRes = []client.SearchResult{
		{Err: errors.New("partial failure")},
		{
			IDs:    entity.NewColumnInt64("ID", []int64{401}),
			Fields: client.ResultSet{entity.NewColumnInt64("tenant_id", []int64{7})},
			Scores: nil,
		},
		{
			IDs: entity.NewColumnInt64("ID", []int64{402}),
			Fields: client.ResultSet{
				entity.NewColumnInt64("tenant_id", []int64{7}),
				entity.NewColumnVarChar("Title", []string{"e7"}),
			},
			Scores: []float32{0.8},
		},
	}

	res, err := repo.SearchByVector(fcCtx7, &vector.Query{
		Field:         "Emb",
		Vector:        []float32{1, 0, 0, 0},
		TopK:          5,
		NumCandidates: 7,
	})
	require.NoError(t, err)
	require.Len(t, res.Hits, 1)
	assert.Equal(t, int64(402), res.Hits[0].Value.ID)
	assert.Equal(t, "e7", res.Hits[0].Value.Title)
	assert.InDelta(t, 0.8, res.Hits[0].Score, 1e-6)
}

func TestFakeClient_NonScopedSearchByVector(t *testing.T) {
	// 非 tenant-scoped：无租户列照常命中。
	fc, repo := newFakeRepo[fcPlainEntity](t)
	fc.searchRes = []client.SearchResult{{
		IDs: entity.NewColumnInt64("ID", []int64{1, 2}),
		Fields: client.ResultSet{
			entity.NewColumnVarChar("Title", []string{"x", "y"}),
		},
		Scores: []float32{0.5, 0.5},
	}}
	res, err := repo.SearchByVector(context.Background(), &vector.Query{
		Field:  "Emb",
		Vector: []float32{1, 0, 0, 0},
		TopK:   5,
	})
	require.NoError(t, err)
	require.Len(t, res.Hits, 2)
}

func TestFakeClient_SearchByVector_FilterExprAndDotMetric(t *testing.T) {
	// 引擎原生 Filter 表达式与租户谓词 AND 合并；dot 度量分数透传。
	fc, repo := newFakeRepo[fcEntity](t)
	fc.searchRes = []client.SearchResult{{
		IDs: entity.NewColumnInt64("ID", []int64{501}),
		Fields: client.ResultSet{
			entity.NewColumnInt64("tenant_id", []int64{7}),
			entity.NewColumnVarChar("Title", []string{"f7"}),
		},
		Scores: []float32{-3},
	}}
	res, err := repo.SearchByVector(fcCtx7, &vector.Query{
		Field:  "Emb",
		Vector: []float32{1, 0, 0, 0},
		TopK:   5,
		Metric: vector.MetricDotProduct,
		Filter: "cat == 5",
	})
	require.NoError(t, err)
	assert.Equal(t, "(cat == 5) and tenant_id == 7", fc.searchExpr)
	assert.Equal(t, entity.IP, fc.searchMetric)
	require.Len(t, res.Hits, 1)
	assert.Equal(t, -3.0, res.Hits[0].Score)
}

func TestFakeClient_SearchByVector_Rejections(t *testing.T) {
	_, repo := newFakeRepo[fcEntity](t)

	// 字段不在 schema → 拒绝。
	_, err := repo.SearchByVector(fcCtx7, &vector.Query{Field: "nope", Vector: []float32{1}, TopK: 5})
	assert.ErrorIs(t, err, ErrInvalidVectorQuery)

	// 非法度量 → 拒绝。
	_, err = repo.SearchByVector(fcCtx7, &vector.Query{Field: "Emb", Vector: []float32{1}, TopK: 5, Metric: "bogus"})
	assert.ErrorIs(t, err, ErrInvalidVectorQuery)

	// 请求非法（空向量/TopK）→ 拒绝。
	_, err = repo.SearchByVector(fcCtx7, &vector.Query{Field: "Emb", Vector: nil, TopK: 5})
	assert.ErrorIs(t, err, ErrInvalidVectorQuery)
	_, err = repo.SearchByVector(fcCtx7, &vector.Query{Field: "Emb", Vector: []float32{1}, TopK: 0})
	assert.ErrorIs(t, err, ErrInvalidVectorQuery)
}

// ─── 错误包装与短路 ─────────────────────────────────────────────────────────

// TestFakeClient_UpsertErrorWraps 写入路径错误包装：Upsert 失败 →
// ErrInsertFailed（混合维度批次由服务端校验，客户端不拦截）。
func TestFakeClient_UpsertErrorWraps(t *testing.T) {
	fc, repo := newFakeRepo[fcEntity](t)
	fc.upsertErr = errors.New("rpc boom")
	_, err := repo.Create(fcCtx7, &fcEntity{ID: 1, Emb: []float32{1, 0, 0, 0}})
	assert.ErrorIs(t, err, ErrInsertFailed)
}

// TestFakeClient_MissingViewerFailsClosed 写入路径缺 ViewerContext → fail-closed。
func TestFakeClient_MissingViewerFailsClosed(t *testing.T) {
	_, repo := newFakeRepo[fcEntity](t)
	_, err := repo.Create(context.Background(), &fcEntity{ID: 1, Emb: []float32{1, 0, 0, 0}})
	assert.ErrorIs(t, err, viewer.ErrMissingViewer)
	_, err = repo.BatchCreate(context.Background(), []*fcEntity{{ID: 1, Emb: []float32{1, 0, 0, 0}}})
	assert.ErrorIs(t, err, viewer.ErrMissingViewer)
}

// TestFakeClient_QueryErrorWraps 读取路径错误包装：
// QueryByPks/Query/Search 失败 → 对应哨兵。
func TestFakeClient_QueryErrorWraps(t *testing.T) {
	fc, repo := newFakeRepo[fcEntity](t)
	fc.pkQueryErr = errors.New("rpc boom")
	_, err := repo.Get(fcCtx7, 1)
	assert.ErrorIs(t, err, ErrQueryFailed)

	fc2, repo2 := newFakeRepo[fcEntity](t)
	fc2.queryErr = errors.New("rpc boom")
	_, err = repo2.QueryByExpr(fcCtx7, "x == 1")
	assert.ErrorIs(t, err, ErrQueryFailed)

	fc3, repo3 := newFakeRepo[fcEntity](t)
	fc3.searchFail = errors.New("rpc boom")
	_, err = repo3.SearchByVector(fcCtx7, &vector.Query{Field: "Emb", Vector: []float32{1, 0, 0, 0}, TopK: 5})
	assert.ErrorIs(t, err, ErrVectorSearchFailed)
}

// TestFakeClient_Get_EmptyResult 空结果集 → ErrPointNotFound。
func TestFakeClient_Get_EmptyResult(t *testing.T) {
	fc, repo := newFakeRepo[fcEntity](t)
	fc.pkRows = client.ResultSet{}
	_, err := repo.Get(fcCtx7, 1)
	assert.ErrorIs(t, err, ErrPointNotFound)
}

// TestFakeClient_DeleteErrorWraps 删除路径错误包装：
// 取行查询失败 → ErrQueryFailed；DeleteByPks/Delete 失败 → ErrDeleteFailed。
func TestFakeClient_DeleteErrorWraps(t *testing.T) {
	fc, repo := newFakeRepo[fcEntity](t)
	fc.pkQueryErr = errors.New("rpc boom")
	_, err := repo.DeleteByIDs(fcCtx7, []uint64{1})
	assert.ErrorIs(t, err, ErrQueryFailed)

	fc2, repo2 := newFakeRepo[fcEntity](t)
	fc2.pkRows = client.ResultSet{
		entity.NewColumnInt64("ID", []int64{1}),
		entity.NewColumnInt64("tenant_id", []int64{7}),
	}
	fc2.deletePkErr = errors.New("rpc boom")
	_, err = repo2.DeleteByIDs(fcCtx7, []uint64{1})
	assert.ErrorIs(t, err, ErrDeleteFailed)

	fc3, repo3 := newFakeRepo[fcEntity](t)
	fc3.countRows = client.ResultSet{entity.NewColumnInt64("count(*)", []int64{1})}
	fc3.deleteErr = errors.New("rpc boom")
	_, err = repo3.DeleteByExpr(fcCtx7, "x == 1")
	assert.ErrorIs(t, err, ErrDeleteFailed)
}

// TestFakeClient_DeleteShortCircuits 空主键列表 → 直接 0 删除。
func TestFakeClient_DeleteShortCircuits(t *testing.T) {
	_, repo := newFakeRepo[fcEntity](t)
	n, err := repo.DeleteByIDs(fcCtx7, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
	n, err = repo.DeleteByUUIDs(fcCtx7, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

// TestFakeClient_DeleteByIDs_MissingPkColumn 服务端省略主键列 →
// 行不可判属被跳过 → 0 删除、不触 DeleteByPks（nil 列防护）。
func TestFakeClient_DeleteByIDs_MissingPkColumn(t *testing.T) {
	fc, repo := newFakeRepo[fcEntity](t)
	fc.pkRows = client.ResultSet{entity.NewColumnInt64("tenant_id", []int64{7})}
	n, err := repo.DeleteByIDs(fcCtx7, []uint64{1})
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
	assert.Equal(t, 0, fc.deletePkCalls)

	fc2, repo2 := newFakeRepo[fcUuidEntity](t)
	fc2.pkRows = client.ResultSet{entity.NewColumnInt64("tenant_id", []int64{7})}
	n, err = repo2.DeleteByUUIDs(fcCtx7, []string{"u1"})
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
	assert.Equal(t, 0, fc2.deletePkCalls)
}

// TestFakeClient_Count_MissingAggregateColumn count(*) 列缺失 → 回退主键计数。
func TestFakeClient_Count_MissingAggregateColumn(t *testing.T) {
	fc, repo := newFakeRepo[fcEntity](t)
	fc.countRows = client.ResultSet{} // 无 count(*) 列
	fc.queryRows = client.ResultSet{entity.NewColumnInt64("ID", []int64{1, 2, 3})}
	n, err := repo.Count(fcCtx7, "")
	require.NoError(t, err)
	assert.Equal(t, int64(3), n)
	assert.Equal(t, []string{"ID"}, fc.queryOutput)
}

// TestFakeClient_CreateCollection_ErrorWraps 集合创建各阶段失败 → ErrInsertFailed。
func TestFakeClient_CreateCollection_ErrorWraps(t *testing.T) {
	fc, repo := newFakeRepo[fcEntity](t)
	fc.createErr = errors.New("rpc boom")
	assert.ErrorIs(t, repo.CreateCollection(context.Background(), 4, vector.MetricCosine), ErrInsertFailed)

	fc2, repo2 := newFakeRepo[fcEntity](t)
	fc2.indexErr = errors.New("rpc boom")
	assert.ErrorIs(t, repo2.CreateCollection(context.Background(), 4, vector.MetricCosine), ErrInsertFailed)

	fc3, repo3 := newFakeRepo[fcEntity](t)
	fc3.loadErr = errors.New("rpc boom")
	assert.ErrorIs(t, repo3.CreateCollection(context.Background(), 4, vector.MetricCosine), ErrInsertFailed)
}
