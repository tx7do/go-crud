package weaviate

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tx7do/go-utils/mapper"
	"github.com/tx7do/go-wind/log"

	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"

	"github.com/tx7do/go-crud/vector"
	"github.com/tx7do/go-crud/viewer"
	"github.com/tx7do/go-crud/weaviate/mixin"
)

// ─────────────────────────────────────────────────────────────────────────────
// fake transport 离线测试：
//
// weaviate 官方客户端是 HTTP 客户端，经 Config.ConnectionClient 注入记录型
// RoundTripper 后可在离线环境锁定仓库层的全部协议交互语义——对象写入
// （class/properties/vector/tenant 三通道组装、UUID 回读）、按 UUID 直取的
// 租户校验、GraphQL Get/Aggregate 的租户条件注入与行过滤、批量删除的条件
// 合并与计数回读、向量检索的距离换算与 MinScore 过滤、错误包装哨兵。
// 网络相关的集成链路另见 KRATOS_IT 门禁测试。
// ─────────────────────────────────────────────────────────────────────────────

// fcEntity fake transport 测试实体：UUID 通道 + 标量/列表 + TenantID mixin。
type fcEntity struct {
	UUID  string    `json:"-"`
	Title string    `json:"title"`
	Age   int64     `json:"age"`
	Tags  []string  `json:"tags"`
	Emb   []float32 `json:"-"`
	mixin.TenantID
}

// fcPlainEntity 非 tenant-scoped 实体（无 mixin）。
type fcPlainEntity struct {
	UUID  string    `json:"-"`
	Title string    `json:"title"`
	Emb   []float32 `json:"-"`
}

// fcCtx7 / fcCtxPlatform 租户 7 与平台视图上下文。
var (
	fcCtx7        = viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	fcCtxPlatform = viewer.WithContext(context.Background(), testEnforceViewer{tid: 0, platform: true})
)

// capturedRequest 记录的单个请求。
type capturedRequest struct {
	method, path string
	body         []byte
}

// fakeTransport 记录全部请求并按预设回放响应的传输替身。
type fakeTransport struct {
	requests []capturedRequest

	// 回放面（空响应体回退为 "{}"；状态码默认 200）。
	createRsp     string // POST /v1/objects
	createStatus  int
	schemaRsp     string // GET/POST/DELETE /v1/schema...
	schemaStatus  int
	batchRsp      string // POST /v1/batch/objects
	batchStatus   int
	graphqlRsp    string // POST /v1/graphql
	graphqlStatus int
	deleteRsp     string // POST /v1/batch/delete
	deleteStatus  int
	objectRsp     string // GET /v1/objects/{id}
	objectStatus  int
	readyFail     bool // /.well-known/ready 返回 503
}

func (t *fakeTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
	}
	// /v1/meta 是 SDK 的版本探测（后台异步触发），属管道行为而非 DAL
	// 语义，不进捕获面以免测试断言受调度时序影响。
	if req.URL.Path != "/v1/meta" {
		t.requests = append(t.requests, capturedRequest{method: req.Method, path: req.URL.Path, body: body})
	}

	status, rsp := t.reply(req.Method, req.URL.Path)
	return &http.Response{
		StatusCode: status,
		Body:       io.NopCloser(strings.NewReader(rsp)),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Request:    req,
	}, nil
}

// reply 按路径分发回放面（其余路径回 {}）。
func (t *fakeTransport) reply(method, path string) (int, string) {
	def := func(rsp string, status int) (int, string) {
		if rsp == "" {
			rsp = "{}"
		}
		if status == 0 {
			status = http.StatusOK
		}
		return status, rsp
	}
	switch {
	case path == "/v1/.well-known/ready":
		if t.readyFail {
			return http.StatusServiceUnavailable, `{"error":[{"message":"not ready"}]}`
		}
		return http.StatusOK, "1"
	case path == "/v1/meta":
		return http.StatusOK, `{"version":"1.27.0"}`
	case method == http.MethodPost && path == "/v1/objects":
		return def(t.createRsp, t.createStatus)
	case method == http.MethodPost && path == "/v1/batch/objects":
		return def(t.batchRsp, t.batchStatus)
	case method == http.MethodPost && path == "/v1/graphql":
		return def(t.graphqlRsp, t.graphqlStatus)
	// weaviate 1.27+ 的批量删除端点：DELETE /v1/batch/objects（携带
	// BatchDelete 匹配体）；SDK 的 ObjectsBatchDeleter 亦按此发送。
	case strings.HasPrefix(path, "/v1/schema"):
		return def(t.schemaRsp, t.schemaStatus)
	case method == http.MethodDelete && path == "/v1/batch/objects":
		return def(t.deleteRsp, t.deleteStatus)
	case method == http.MethodGet && strings.HasPrefix(path, "/v1/objects/"):
		if t.objectRsp == "" {
			return http.StatusNotFound, `{"error":[{"message":"not found"}]}`
		}
		return def(t.objectRsp, t.objectStatus)
	default:
		return def("", 0)
	}
}

// lastBody 最后一条请求的解码体（JSON 失败返回 nil）。
func (t *fakeTransport) lastBody(t2 *testing.T) map[string]any {
	t2.Helper()
	require.NotEmpty(t2, t.requests)
	var m map[string]any
	require.NoError(t2, json.Unmarshal(t.requests[len(t.requests)-1].body, &m))
	return m
}

// reqByPath 按路径取最近的一条请求（首用 API 前的 /v1/meta 版本探测
// 不计入，按下标断言会错位）。
func (t *fakeTransport) reqByPath(t2 *testing.T, path string) capturedRequest {
	t2.Helper()
	for i := len(t.requests) - 1; i >= 0; i-- {
		if t.requests[i].path == path {
			return t.requests[i]
		}
	}
	t2.Fatalf("no request to %s captured", path)
	return capturedRequest{}
}

// graphqlQuery 解出 GraphQL 请求的查询文本（信封为 {"query": "..."}）。
func graphqlQuery(t *testing.T, ft *fakeTransport) string {
	t.Helper()
	var env struct {
		Query string `json:"query"`
	}
	require.NoError(t, json.Unmarshal(ft.reqByPath(t, "/v1/graphql").body, &env))
	return env.Query
}

// newFakeRepo 构建绑定 fake transport 的仓库（集合名固定 Coll）。
func newFakeRepo[ENTITY any](t *testing.T) (*fakeTransport, *Repository[ENTITY, ENTITY]) {
	t.Helper()
	ft := &fakeTransport{}
	c, err := NewClient(
		WithHost("localhost:8080"),
		WithHTTPClient(&http.Client{Transport: ft}),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	m := mapper.NewCopierMapper[ENTITY, ENTITY]()
	return ft, NewRepository[ENTITY, ENTITY](c, "Coll", m, log.GetLogger())
}

// ─────────────────────────────────────────────────────────────────────────────
// 写入路径。
// ─────────────────────────────────────────────────────────────────────────────

// TestFake_Create_TenantForced 单写：class/properties/vector 三通道组装，
// 租户强制落入属性表，服务端返回的 UUID 回读到 ID 通道字段。
func TestFake_Create_TenantForced(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.createRsp = `{"class":"Coll","id":"u-1","properties":{}}`

	out, err := repo.Create(fcCtx7, &fcEntity{Title: "x", Age: 42, Tags: []string{"a"}, Emb: []float32{0.1, 0.2}})
	require.NoError(t, err)

	require.Len(t, ft.requests, 1)
	assert.Equal(t, http.MethodPost, ft.requests[0].method)
	assert.Equal(t, "/v1/objects", ft.requests[0].path)

	body := ft.lastBody(t)
	assert.Equal(t, "Coll", body["class"])
	props, ok := body["properties"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "x", props["title"])
	assert.Equal(t, float64(42), props["age"])
	assert.Equal(t, float64(7), props["tenant_id"], "create must force tenant 7")
	assert.NotContains(t, props, "UUID", "id channel must not enter properties")
	assert.NotContains(t, props, "uuid")
	vec, ok := body["vector"].([]any)
	require.True(t, ok)
	assert.NotEmpty(t, vec, "vector channel must carry the vector field")

	assert.Equal(t, "u-1", out.UUID, "server-returned UUID must round-trip")
}

// TestFake_Create_PlainEntity 非 tenant 实体：属性表无 tenant_id。
func TestFake_Create_PlainEntity(t *testing.T) {
	ft, repo := newFakeRepo[fcPlainEntity](t)
	ft.createRsp = `{"class":"Coll","id":"u-1"}`

	_, err := repo.Create(fcCtx7, &fcPlainEntity{Title: "x", Emb: []float32{0.1}})
	require.NoError(t, err)
	props := ft.lastBody(t)["properties"].(map[string]any)
	assert.Equal(t, "x", props["title"])
	assert.NotContains(t, props, "tenant_id")
}

// TestFake_Create_Error 服务端错误包装为 ErrInsertFailed。
func TestFake_Create_Error(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.createStatus = http.StatusInternalServerError
	ft.createRsp = `{"error":[{"message":"boom"}]}`

	_, err := repo.Create(fcCtx7, &fcEntity{Title: "x", Age: 1, Emb: []float32{0.1}})
	assert.ErrorIs(t, err, ErrInsertFailed)
}

// TestFake_BatchCreate_TenantForced 批量写：单次 batch，逐对象强制租户，
// UUID 按行序回读。
func TestFake_BatchCreate_TenantForced(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.batchRsp = `[
		{"class":"Coll","id":"u-1","result":{"status":"SUCCESS"}},
		{"class":"Coll","id":"u-2","result":{"status":"SUCCESS"}}
	]`

	outs, err := repo.BatchCreate(fcCtx7, []*fcEntity{{Title: "a", Emb: []float32{0.1}}, {Title: "b", Emb: []float32{0.2}}})
	require.NoError(t, err)
	require.Len(t, outs, 2)
	assert.Equal(t, "u-1", outs[0].UUID)
	assert.Equal(t, "u-2", outs[1].UUID)

	require.Len(t, ft.requests, 1)
	assert.Equal(t, "/v1/batch/objects", ft.requests[0].path)
	body := ft.lastBody(t)
	objs, ok := body["objects"].([]any)
	require.True(t, ok)
	require.Len(t, objs, 2)
	o0 := objs[0].(map[string]any)
	assert.Equal(t, "Coll", o0["class"])
	assert.Equal(t, float64(7), o0["properties"].(map[string]any)["tenant_id"])
}

// TestFake_BatchCreate_RowError 行级错误包装为 ErrInsertFailed。
func TestFake_BatchCreate_RowError(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.batchRsp = `[{"class":"Coll","id":"u-1","result":{"status":"FAILED","errors":{"error":[{"message":"bad row"}]}}}]`

	_, err := repo.BatchCreate(fcCtx7, []*fcEntity{{Title: "a", Emb: []float32{0.1}}})
	require.ErrorIs(t, err, ErrInsertFailed)
	assert.Contains(t, err.Error(), "bad row")
}

// TestFake_BatchCreate_Empty 空批量直接返回（无请求发出）。
func TestFake_BatchCreate_Empty(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)

	outs, err := repo.BatchCreate(fcCtx7, nil)
	assert.NoError(t, err)
	assert.Nil(t, outs)
	assert.Empty(t, ft.requests)
}

// ─────────────────────────────────────────────────────────────────────────────
// 直取路径（GetByUUID）。
// ─────────────────────────────────────────────────────────────────────────────

// TestFake_GetByUUID_TenantMatch 本租户对象取回：属性与 UUID 还原到 DTO。
func TestFake_GetByUUID_TenantMatch(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.objectRsp = `{"class":"Coll","id":"u-1","properties":{"title":"own","age":42,"tenant_id":7}}`

	out, err := repo.GetByUUID(fcCtx7, "u-1")
	require.NoError(t, err)
	assert.Equal(t, "own", out.Title)
	assert.Equal(t, int64(42), out.Age)
	assert.Equal(t, "u-1", out.UUID)

	require.Len(t, ft.requests, 1)
	assert.Equal(t, "/v1/objects/Coll/u-1", ft.requests[0].path, "by-id fetch is class-scoped")
}

// TestFake_GetByUUID_TenantMismatch 他租户对象与不存在同构（纵深防御：
// 直取路径无服务端条件，客户端校验兜底）。
func TestFake_GetByUUID_TenantMismatch(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.objectRsp = `{"class":"Coll","id":"u-1","properties":{"title":"foreign","tenant_id":8}}`

	_, err := repo.GetByUUID(fcCtx7, "u-1")
	assert.ErrorIs(t, err, ErrPointNotFound)
}

// TestFake_GetByUUID_MissingTenantProperty 缺租户属性的对象视同他租户。
func TestFake_GetByUUID_MissingTenantProperty(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.objectRsp = `{"class":"Coll","id":"u-1","properties":{"title":"x"}}`

	_, err := repo.GetByUUID(fcCtx7, "u-1")
	assert.ErrorIs(t, err, ErrPointNotFound)
}

// TestFake_GetByUUID_NotFound 不存在 → ErrPointNotFound。
func TestFake_GetByUUID_NotFound(t *testing.T) {
	_, repo := newFakeRepo[fcEntity](t)

	_, err := repo.GetByUUID(fcCtx7, "u-1")
	assert.ErrorIs(t, err, ErrPointNotFound)
}

// TestFake_GetByUUID_PlatformSkips 平台视图放行（不做租户校验）。
func TestFake_GetByUUID_PlatformSkips(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.objectRsp = `{"class":"Coll","id":"u-1","properties":{"title":"any","tenant_id":99}}`

	out, err := repo.GetByUUID(fcCtxPlatform, "u-1")
	require.NoError(t, err)
	assert.Equal(t, "any", out.Title)
}

// TestFake_GetByUUID_MissingViewerFailClosed 缺身份 fail-closed。
func TestFake_GetByUUID_MissingViewerFailClosed(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.objectRsp = `{"class":"Coll","id":"u-1","properties":{"tenant_id":7}}`

	_, err := repo.GetByUUID(context.Background(), "u-1")
	assert.ErrorIs(t, err, viewer.ErrMissingViewer)
}

// ─────────────────────────────────────────────────────────────────────────────
// GraphQL 路径（Query / Count / SearchByVector）。
// ─────────────────────────────────────────────────────────────────────────────

// TestFake_Query_TenantFiltersForeignRows 租户视图：条件注入 + 他租户行
// 被客户端校验剔除。
func TestFake_Query_TenantFiltersForeignRows(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.graphqlRsp = `{"data":{"Get":{"Coll":[
		{"Title":"own","tenant_id":7,"_additional":{"id":"u-1"}},
		{"Title":"foreign","tenant_id":8,"_additional":{"id":"u-2"}}
	]}}}`

	rows, err := repo.Query(fcCtx7, nil, 0)
	require.NoError(t, err)
	require.Len(t, rows, 1, "tenant-7 view must see only its own rows")
	assert.Equal(t, "own", rows[0].Title)
	assert.Equal(t, "u-1", rows[0].UUID)

	q := graphqlQuery(t, ft)
	assert.Contains(t, q, `path: ["tenant_id"]`, "tenant condition must be injected")
	assert.Contains(t, q, "Coll")
	assert.NotContains(t, q, "nearVector", "plain query must not carry vector argument")
}

// TestFake_Query_CallerFilterMerged 调用方条件与租户条件 And 合并。
func TestFake_Query_CallerFilterMerged(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.graphqlRsp = `{"data":{"Get":{"Coll":[]}}}`

	caller := filters.Where().WithPath([]string{"Title"}).WithOperator(filters.Equal).WithValueText("x")
	rows, err := repo.Query(fcCtx7, caller, 10)
	require.NoError(t, err)
	assert.Empty(t, rows)

	q := graphqlQuery(t, ft)
	assert.Contains(t, q, "And", "caller filter and tenant condition must merge under And")
	assert.Contains(t, q, "title")
	assert.Contains(t, q, "tenant_id")
}

// TestFake_Query_PlatformNoTenantCondition 平台视图不注入租户条件。
func TestFake_Query_PlatformNoTenantCondition(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.graphqlRsp = `{"data":{"Get":{"Coll":[
		{"Title":"a","tenant_id":7,"_additional":{"id":"u-1"}}
	]}}}`

	rows, err := repo.Query(fcCtxPlatform, nil, 0)
	require.NoError(t, err)
	assert.Len(t, rows, 1)
	// 平台视图不注入租户条件（where 子句缺席；字段列表中的 tenant_id
	// 属性名照常出现）。
	assert.NotContains(t, graphqlQuery(t, ft), "where:")
}

// TestFake_Query_MissingViewerFailClosed 缺身份 fail-closed。
func TestFake_Query_MissingViewerFailClosed(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)

	_, err := repo.Query(context.Background(), nil, 0)
	assert.ErrorIs(t, err, viewer.ErrMissingViewer)
	assert.Empty(t, ft.requests)
}

// TestFake_Query_GraphQLError GraphQL 错误列表包装为 ErrQueryFailed。
func TestFake_Query_GraphQLError(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.graphqlRsp = `{"errors":[{"message":"bad query"}]}`

	_, err := repo.Query(fcCtx7, nil, 0)
	assert.ErrorIs(t, err, ErrQueryFailed)
}

// TestFake_Count_TenantInjected 计数：Aggregate meta.count 回读 + 租户
// 条件注入。
func TestFake_Count_TenantInjected(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.graphqlRsp = `{"data":{"Aggregate":{"Coll":[{"meta":{"count":42}}]}}}`

	n, err := repo.Count(fcCtx7, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(42), n)

	q := graphqlQuery(t, ft)
	assert.Contains(t, q, "Aggregate")
	assert.Contains(t, q, "count")
	assert.Contains(t, q, `path: ["tenant_id"]`)
}

// TestFake_Count_GraphQLError 计数失败包装为 ErrCountFailed。
func TestFake_Count_GraphQLError(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.graphqlRsp = `{"errors":[{"message":"bad aggregate"}]}`

	_, err := repo.Count(fcCtx7, nil)
	assert.ErrorIs(t, err, ErrCountFailed)
}

// TestFake_Exists 计数为正即存在。
func TestFake_Exists(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.graphqlRsp = `{"data":{"Aggregate":{"Coll":[{"meta":{"count":0}}]}}}`

	ok, err := repo.Exists(fcCtx7, nil)
	require.NoError(t, err)
	assert.False(t, ok)
}

// TestFake_SearchByVector_ScoreConversion 向量检索：cosine 距离换算为
// 相似度分（1-d），MinScore 在统一分数空间客户端过滤。
func TestFake_SearchByVector_ScoreConversion(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.graphqlRsp = `{"data":{"Get":{"Coll":[
		{"Title":"near","tenant_id":7,"_additional":{"id":"u-1","distance":0.1}},
		{"Title":"far","tenant_id":7,"_additional":{"id":"u-2","distance":0.9}}
	]}}}`

	res, err := repo.SearchByVector(fcCtx7, &vector.Query{
		Vector:   []float32{0.1, 0.2, 0.3, 0.4},
		TopK:     10,
		MinScore: 0.5,
	})
	require.NoError(t, err)
	require.Len(t, res.Hits, 1, "far hit must be filtered by MinScore in unified score space")
	assert.InDelta(t, 0.9, res.Hits[0].Score, 1e-6, "cosine distance 0.1 must convert to similarity 0.9")
	assert.Equal(t, "near", res.Hits[0].Value.Title)
	assert.Equal(t, int64(1), res.Total)

	q := graphqlQuery(t, ft)
	assert.Contains(t, q, "nearVector")
	assert.Contains(t, q, `path: ["tenant_id"]`)
}

// TestFake_SearchByVector_InvalidQuery 空向量 / 非正 TopK 拒绝。
func TestFake_SearchByVector_InvalidQuery(t *testing.T) {
	_, repo := newFakeRepo[fcEntity](t)

	_, err := repo.SearchByVector(fcCtx7, &vector.Query{TopK: 1})
	assert.ErrorIs(t, err, ErrInvalidVectorQuery)
	_, err = repo.SearchByVector(fcCtx7, &vector.Query{Vector: []float32{1}})
	assert.ErrorIs(t, err, ErrInvalidVectorQuery)
}

// TestFake_SearchByVector_TenantMismatchSkipped 他租户命中静默剔除。
func TestFake_SearchByVector_TenantMismatchSkipped(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.graphqlRsp = `{"data":{"Get":{"Coll":[
		{"Title":"near","tenant_id":8,"_additional":{"id":"u-1","distance":0.1}}
	]}}}`

	res, err := repo.SearchByVector(fcCtx7, &vector.Query{Vector: []float32{1, 0}, TopK: 5})
	require.NoError(t, err)
	assert.Empty(t, res.Hits, "foreign-tenant hits must be silently dropped")
}

// ─────────────────────────────────────────────────────────────────────────────
// 删除路径。
// ─────────────────────────────────────────────────────────────────────────────

// TestFake_DeleteByUUIDs_TenantScoped 按 UUID 批量删除：id ContainsAny
// 条件与租户条件合并，计数取响应成功数。
func TestFake_DeleteByUUIDs_TenantScoped(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.deleteRsp = `{"results":{"matches":1,"successful":1,"failed":0,"limit":10000,"objects":[]}}`

	n, err := repo.DeleteByUUIDs(fcCtx7, []string{"u-1"})
	require.NoError(t, err)
	assert.Equal(t, int64(1), n)

	req := ft.reqByPath(t, "/v1/batch/objects")
	body := string(req.body)
	assert.Contains(t, body, `"path":["id"]`)
	assert.Contains(t, body, "ContainsAny")
	assert.Contains(t, body, `"path":["tenant_id"]`)
}

// TestFake_DeleteByUUIDs_EmptyList 空列表直接返回（无删除请求发出）。
func TestFake_DeleteByUUIDs_EmptyList(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)

	n, err := repo.DeleteByUUIDs(fcCtx7, nil)
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
	for _, req := range ft.requests {
		assert.NotEqual(t, "/v1/batch/objects", req.path)
	}
}

// TestFake_DeleteByFilter_TenantMerged 按条件删除：条件与租户条件合并。
func TestFake_DeleteByFilter_TenantMerged(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.deleteRsp = `{"results":{"matches":3,"successful":3,"failed":0,"limit":10000,"objects":[]}}`

	caller := filters.Where().WithPath([]string{"expired"}).WithOperator(filters.Equal).WithValueBoolean(true)
	n, err := repo.DeleteByFilter(fcCtx7, caller)
	require.NoError(t, err)
	assert.Equal(t, int64(3), n)

	body := string(ft.reqByPath(t, "/v1/batch/objects").body)
	assert.Contains(t, body, `"path":["expired"]`)
	assert.Contains(t, body, `"path":["tenant_id"]`)
}

// TestFake_DeleteByFilter_NilRejected 必须显式给出条件。
func TestFake_DeleteByFilter_NilRejected(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)

	_, err := repo.DeleteByFilter(fcCtx7, nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	for _, req := range ft.requests {
		assert.NotEqual(t, "/v1/batch/objects", req.path)
	}
}

// TestFake_DeleteErrors 删除失败包装为 ErrDeleteFailed。
func TestFake_DeleteErrors(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.deleteStatus = http.StatusInternalServerError
	ft.deleteRsp = `{"error":[{"message":"boom"}]}`

	_, err := repo.DeleteByUUIDs(fcCtx7, []string{"u-1"})
	assert.ErrorIs(t, err, ErrDeleteFailed)
}

// TestFake_NilDTO nil DTO 拒绝。
func TestFake_NilDTO(t *testing.T) {
	_, repo := newFakeRepo[fcEntity](t)

	_, err := repo.Create(fcCtx7, nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)
}

// ─────────────────────────────────────────────────────────────────────────────
// schema 管理与畸形响应补充面。
// ─────────────────────────────────────────────────────────────────────────────

// TestFake_HasCollection 集合存在性：SDK 仅按状态码判定——200 即存在
// （响应体不参与），非 200 即不存在且无错误。
func TestFake_HasCollection(t *testing.T) {
	ft, holder := newFakeRepo[fcEntity](t)

	ok, err := holder.client.HasCollection(context.Background(), "Coll")
	require.NoError(t, err)
	assert.True(t, ok, "200 means the class exists")

	ft.schemaStatus = http.StatusNotFound
	ok, err = holder.client.HasCollection(context.Background(), "Coll")
	require.NoError(t, err)
	assert.False(t, ok, "non-200 means the class is absent")
}

// TestFake_DropCollection 集合删除：成功返回 nil，失败包装 ErrDeleteFailed。
func TestFake_DropCollection(t *testing.T) {
	ft, holder := newFakeRepo[fcEntity](t)

	require.NoError(t, holder.client.DropCollection(context.Background(), "Coll"))
	assert.Equal(t, http.MethodDelete, ft.reqByPath(t, "/v1/schema/Coll").method)

	ft.schemaStatus = http.StatusInternalServerError
	err := holder.client.DropCollection(context.Background(), "Coll")
	assert.ErrorIs(t, err, ErrDeleteFailed)
}

// TestFake_CreateCollection 建集合：成功 / 服务端错误 / 非法类名。
func TestFake_CreateCollection(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)

	require.NoError(t, repo.CreateCollection(fcCtxPlatform, vector.MetricCosine))
	req := ft.reqByPath(t, "/v1/schema")
	assert.Equal(t, http.MethodPost, req.method)
	assert.Contains(t, string(req.body), `"class":"Coll"`)
	assert.Contains(t, string(req.body), `"distance":"cosine"`)

	ft.schemaStatus = http.StatusInternalServerError
	assert.ErrorIs(t, repo.CreateCollection(fcCtxPlatform, ""), ErrInsertFailed)

	// 非法类名在触碰网络前拒绝（client 尚未发过 schema 请求以外的路径）。
	lower, err := NewClient(WithHost("localhost:8080"), WithHTTPClient(&http.Client{Transport: &fakeTransport{}}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = lower.Close() })
	badRepo := NewRepository[fcEntity, fcEntity](lower, "coll", mapper.NewCopierMapper[fcEntity, fcEntity](), log.GetLogger())
	assert.ErrorIs(t, badRepo.CreateCollection(fcCtxPlatform, ""), ErrInvalidRequest)
}

// TestFake_Create_NoVectorField 无向量字段的实体在触碰网络前拒绝。
func TestFake_Create_NoVectorField(t *testing.T) {
	// fcPlainEntity 含 Emb 向量字段；此处用无向量实体另建仓库。
	type plainNoVec struct {
		UUID  string `json:"-"`
		Title string `json:"title"`
	}
	c, err := NewClient(WithHost("localhost:8080"), WithHTTPClient(&http.Client{Transport: &fakeTransport{}}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	r2 := NewRepository[plainNoVec, plainNoVec](c, "Coll", mapper.NewCopierMapper[plainNoVec, plainNoVec](), log.GetLogger())

	_, err = r2.Create(fcCtx7, &plainNoVec{Title: "x"})
	assert.ErrorIs(t, err, ErrInvalidRequest)
}

// TestFake_BatchCreate_EdgeCases 全 nil 条目 → 空批量；批量响应短于条目
// → 余下 UUID 留空；无向量实体拒绝。
func TestFake_BatchCreate_EdgeCases(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)

	outs, err := repo.BatchCreate(fcCtx7, []*fcEntity{nil, nil})
	assert.NoError(t, err)
	assert.Nil(t, outs)

	ft.batchRsp = `[{"class":"Coll","id":"u-1","result":{"status":"SUCCESS"}}]`
	outs, err = repo.BatchCreate(fcCtx7, []*fcEntity{{Title: "a", Emb: []float32{1}}, {Title: "b", Emb: []float32{2}}})
	require.NoError(t, err)
	require.Len(t, outs, 2)
	assert.Equal(t, "u-1", outs[0].UUID)
	assert.Empty(t, outs[1].UUID, "short batch response leaves remaining channels empty")
}

// TestFake_GetByUUID_ServerError 服务端错误包装为 ErrQueryFailed。
func TestFake_GetByUUID_ServerError(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.objectStatus = http.StatusInternalServerError
	ft.objectRsp = `{"error":[{"message":"boom"}]}`

	_, err := repo.GetByUUID(fcCtx7, "u-1")
	assert.ErrorIs(t, err, ErrQueryFailed)
}

// TestFake_Query_MalformedPayloads 非映射命中剔除；空 Data 不 panic。
func TestFake_Query_MalformedPayloads(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.graphqlRsp = `{"data":{"Get":{"Coll":[
		1,
		{"Title":"x","tenant_id":7,"_additional":{"id":"u-1"}}
	]}}}`

	rows, err := repo.Query(fcCtx7, nil, 0)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "x", rows[0].Title)

	ft.graphqlRsp = `{"data":{}}`
	rows, err = repo.Query(fcCtx7, nil, 0)
	require.NoError(t, err)
	assert.Empty(t, rows)
}

// TestFake_Count_EmptyAggregate 空聚合行 / 缺 meta → 0。
func TestFake_Count_EmptyAggregate(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)

	for _, rsp := range []string{
		`{"data":{"Aggregate":{"Coll":[]}}}`,
		`{"data":{"Aggregate":{"Coll":[{}]}}}`,
		`{}`,
	} {
		ft.graphqlRsp = rsp
		n, err := repo.Count(fcCtx7, nil)
		require.NoError(t, err)
		assert.Equal(t, int64(0), n)
	}
}

// TestFake_Delete_NilResults 响应缺 results → 计数 0。
func TestFake_Delete_NilResults(t *testing.T) {
	ft, repo := newFakeRepo[fcEntity](t)
	ft.deleteRsp = `{}`

	n, err := repo.DeleteByUUIDs(fcCtx7, []string{"u-1"})
	require.NoError(t, err)
	assert.Equal(t, int64(0), n)
}

// TestJsonNumberToFloat64 JSON 数值宽容转换矩阵。
func TestJsonNumberToFloat64(t *testing.T) {
	assert.InDelta(t, 1.5, jsonNumberToFloat64(float64(1.5)), 1e-9)
	assert.InDelta(t, 1.5, jsonNumberToFloat64(float32(1.5)), 1e-6)
	assert.InDelta(t, 42, jsonNumberToFloat64(int64(42)), 1e-9)
	assert.InDelta(t, 42, jsonNumberToFloat64(42), 1e-9)
	assert.InDelta(t, 2.5, jsonNumberToFloat64(json.Number("2.5")), 1e-9)
	assert.InDelta(t, 0, jsonNumberToFloat64("not-a-number"), 1e-9)
	assert.InDelta(t, 0, jsonNumberToFloat64(nil), 1e-9)
}
