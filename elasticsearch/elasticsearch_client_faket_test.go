package elasticsearch

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

// paginationV1PagingRequestES builds a page/size paging request fixture.
func paginationV1PagingRequestES(page, size uint32) *paginationV1.PagingRequest {
	return &paginationV1.PagingRequest{Page: &page, PageSize: &size}
}

// mustSourceName extracts the "name" field from a hit _source document.
func mustSourceName(raw json.RawMessage) string {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return ""
	}
	name, _ := m["name"].(string)
	return name
}

// ---------------------------------------------------------------------------
// Fake transport: every request is answered from a scripted queue, so the
// whole client surface runs without a cluster (mirrors the opensearch
// opensearch_client_faket_test.go pattern).
// ---------------------------------------------------------------------------

type fakeESResponse struct {
	status int
	body   string
}

type fakeESTransport struct {
	mu        sync.Mutex
	responses []fakeESResponse
	requests  []*http.Request
	err       error // when set, RoundTrip fails instead of answering
}

func (f *fakeESTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, r)
	if f.err != nil {
		return nil, f.err
	}
	if len(f.responses) == 0 {
		return nil, errors.New("fake transport: no scripted response for " + r.Method + " " + r.URL.Path)
	}
	fr := f.responses[0]
	f.responses = f.responses[1:]
	return &http.Response{
		StatusCode: fr.status,
		Body:       io.NopCloser(strings.NewReader(fr.body)),
		Header: http.Header{
			"Content-Type":      []string{"application/json"},
			"X-Elastic-Product": []string{"Elasticsearch"},
		},
		Request: r,
	}, nil
}

func (f *fakeESTransport) enqueue(status int, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.responses = append(f.responses, fakeESResponse{status: status, body: body})
}

func (f *fakeESTransport) lastMethod() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) == 0 {
		return ""
	}
	return f.requests[len(f.requests)-1].Method
}

const (
	okESBody         = `{"acknowledged":true}`
	searchResultBody = `{"took":1,"timed_out":false,"hits":{"total":{"value":1,"relation":"eq"},"hits":[{"_index":"users","_id":"1","_score":1,"_source":{"name":"alice"}}]}}`
	esErrorBody      = `{"error":{"type":"illegal_argument_exception","reason":"boom"},"status":500}`
	notFoundBody     = `{"error":{"type":"document_missing_exception","reason":"nope"},"status":404}`
)

func newFakeESClient(t *testing.T) (*Client, *fakeESTransport) {
	t.Helper()
	ft := &fakeESTransport{}
	c, err := NewElasticsearchClient(
		WithAddresses("http://fake.local:9200"),
		WithTransport(ft),
		WithDisableRetry(true),
		WithMaxRetries(1),
	)
	require.NoError(t, err)
	return c, ft
}

func TestCheckConnectStatus_Fake(t *testing.T) {
	ctx := context.Background()

	// nil embedded client reports false without touching the network
	bare := &Client{}
	assert.False(t, bare.CheckConnectStatus())

	// happy path parses the server version
	c, ft := newFakeESClient(t)
	ft.enqueue(200, `{"version":{"number":"9.5.0"},"cluster_name":"t"}`)
	assert.True(t, c.CheckConnectStatus())

	// server-side error reports false
	ft.enqueue(500, esErrorBody)
	assert.False(t, c.CheckConnectStatus())

	// transport level failure reports false
	ft2 := &fakeESTransport{err: errors.New("dial fail")}
	c2, err := NewElasticsearchClient(WithAddresses("http://fake.local:9200"), WithTransport(ft2))
	require.NoError(t, err)
	assert.False(t, c2.CheckConnectStatus())
	_ = ctx
}

func TestIndexLifecycle_Fake(t *testing.T) {
	ctx := context.Background()
	c, ft := newFakeESClient(t)

	// create on a fresh index: HEAD 404 then PUT ok
	ft.enqueue(404, "")
	ft.enqueue(200, okESBody)
	assert.NoError(t, c.CreateIndex(ctx, "users", UserMapping, ""))

	// create on an existing index fails fast
	ft.enqueue(200, "")
	assert.ErrorIs(t, c.CreateIndex(ctx, "users", UserMapping, ""), ErrIndexAlreadyExists)

	// create failure maps to ErrCreateIndex
	ft.enqueue(404, "")
	ft.enqueue(500, esErrorBody)
	assert.ErrorIs(t, c.CreateIndex(ctx, "users", UserMapping, ""), ErrCreateIndex)

	// delete on an existing index
	ft.enqueue(200, "")
	ft.enqueue(200, okESBody)
	assert.NoError(t, c.DeleteIndex(ctx, "users"))

	// delete on a missing index
	ft.enqueue(404, "")
	assert.ErrorIs(t, c.DeleteIndex(ctx, "users"), ErrIndexNotFound)

	// delete failure maps to ErrDeleteIndex
	ft.enqueue(200, "")
	ft.enqueue(500, esErrorBody)
	assert.ErrorIs(t, c.DeleteIndex(ctx, "users"), ErrDeleteIndex)

	// index existence probe itself
	ft.enqueue(200, "")
	ok, err := c.IndexExists(ctx, "users")
	assert.NoError(t, err)
	assert.True(t, ok)
}

func TestDocumentCRUD_Fake(t *testing.T) {
	ctx := context.Background()
	c, ft := newFakeESClient(t)

	// insert with explicit id and with auto id
	ft.enqueue(200, okESBody)
	assert.NoError(t, c.InsertDocument(ctx, "users", "1", map[string]any{"name": "alice"}))
	ft.enqueue(200, okESBody)
	assert.NoError(t, c.InsertDocument(ctx, "users", "", map[string]any{"name": "bob"}))

	// insert conflict
	ft.enqueue(409, notFoundBody)
	assert.ErrorIs(t, c.InsertDocument(ctx, "users", "1", map[string]any{}), ErrDocumentConflict)

	// insert failure
	ft.enqueue(500, esErrorBody)
	assert.ErrorIs(t, c.InsertDocument(ctx, "users", "1", map[string]any{}), ErrInsertDocument)

	// get document decodes the whole response (including _source)
	ft.enqueue(200, `{"_index":"users","_id":"1","found":true,"_source":{"name":"alice"}}`)
	var out map[string]any
	assert.NoError(t, c.GetDocument(ctx, "users", "1", []string{"name"}, &out))
	assert.Equal(t, true, out["found"])
	if src, ok := out["_source"].(map[string]any); ok {
		assert.Equal(t, "alice", src["name"])
	} else {
		t.Fatal("_source must decode into the output map")
	}

	// get missing document
	ft.enqueue(404, notFoundBody)
	assert.ErrorIs(t, c.GetDocument(ctx, "users", "404", nil, &out), ErrDocumentNotFound)

	// get failure
	ft.enqueue(500, esErrorBody)
	assert.ErrorIs(t, c.GetDocument(ctx, "users", "1", nil, &out), ErrGetDocument)

	// update document (result ignored by the wrapper)
	ft.enqueue(200, okESBody)
	assert.NoError(t, c.UpdateDocument(ctx, "users", "1", map[string]any{"doc": map[string]any{"age": 1}}))

	// delete document
	ft.enqueue(200, okESBody)
	assert.NoError(t, c.DeleteDocument(ctx, "users", "1"))

	// update by query / delete by query
	ft.enqueue(200, okESBody)
	assert.NoError(t, c.UpdateByQuery(ctx, "users", `{"query":{"match_all":{}}}`))
	ft.enqueue(500, esErrorBody)
	assert.ErrorIs(t, c.UpdateByQuery(ctx, "users", `{}`), ErrUpdateDocument)

	ft.enqueue(200, okESBody)
	assert.NoError(t, c.DeleteByQuery(ctx, "users", `{"query":{"match_all":{}}}`))
	ft.enqueue(500, esErrorBody)
	assert.ErrorIs(t, c.DeleteByQuery(ctx, "users", `{}`), ErrDeleteDocument)
}

func bulkOKBody(n int) string {
	var b strings.Builder
	b.WriteString(`{"took":1,"errors":false,"items":[`)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`{"index":{"_id":"x","status":201}}`)
	}
	b.WriteString("]}")
	return b.String()
}

func TestBatchInsertDocument_Fake(t *testing.T) {
	ctx := context.Background()
	c, ft := newFakeESClient(t)

	// empty input short-circuits
	assert.NoError(t, c.BatchInsertDocument(ctx, "users", nil, nil))

	// ids/dataSet length mismatch
	assert.Error(t, c.BatchInsertDocument(ctx, "users", []any{map[string]any{}}, []string{"1", "2"}))

	// success
	ft.enqueue(200, bulkOKBody(2))
	assert.NoError(t, c.BatchInsertDocument(ctx, "users", []any{map[string]any{"a": 1}, map[string]any{"b": 2}}, []string{"1", "2"}))

	// partial failure surfaces as PartialFailureError
	ft.enqueue(200, `{"took":1,"errors":true,"items":[{"index":{"_id":"1","status":500,"error":{"type":"t","reason":"r"}}},{"index":{"_id":"2","status":201}}]}`)
	err := c.BatchInsertDocument(ctx, "users", []any{map[string]any{}, map[string]any{}}, []string{"1", "2"})
	var pfe *PartialFailureError
	assert.ErrorAs(t, err, &pfe)
	assert.Equal(t, 2, pfe.Total)
	assert.Equal(t, 1, pfe.Failed)

	// API level error
	ft.enqueue(500, esErrorBody)
	assert.ErrorIs(t, c.BatchInsertDocument(ctx, "users", []any{map[string]any{}}, nil), ErrBatchInsertDocument)

	// unmarshalable input fails without a network call
	assert.Error(t, c.BatchInsertDocument(ctx, "users", []any{make(chan int)}, nil))

	// oversized batches are split into chunks
	dataSet := make([]any, 1001)
	for i := range dataSet {
		dataSet[i] = map[string]any{"i": i}
	}
	ids := make([]string, 1001)
	for i := range ids {
		ids[i] = "id-" + string(rune('a'+i%26)) + "-" + intToFix(i)
	}
	ft.enqueue(200, bulkOKBody(1000))
	ft.enqueue(200, bulkOKBody(1))
	assert.NoError(t, c.BatchInsertDocument(ctx, "users", dataSet, ids))
}

func intToFix(i int) string {
	if i < 10 {
		return "0" + string(rune('0'+i))
	}
	return string(rune('0'+i/10)) + string(rune('0'+i%10))
}

func TestBatchUpdateDocument_Fake(t *testing.T) {
	ctx := context.Background()
	c, ft := newFakeESClient(t)

	assert.NoError(t, c.BatchUpdateDocument(ctx, "users", nil, nil))
	assert.Error(t, c.BatchUpdateDocument(ctx, "users", []any{map[string]any{}}, []string{"1", "2"}))

	// items without ids are skipped entirely
	err := c.BatchUpdateDocument(ctx, "users", []any{map[string]any{}, map[string]any{}}, nil)
	if err == nil || !strings.Contains(err.Error(), "no valid documents to update") {
		t.Fatalf("expected no-valid-documents error, got %v", err)
	}

	ft.enqueue(200, bulkOKBody(1))
	assert.NoError(t, c.BatchUpdateDocument(ctx, "users", []any{map[string]any{"age": 2}}, []string{"1"}))

	ft.enqueue(200, `{"took":1,"errors":true,"items":[{"update":{"_id":"1","status":404,"error":{"type":"d","reason":"missing"}}}]}`)
	var pfe *PartialFailureError
	assert.ErrorAs(t, c.BatchUpdateDocument(ctx, "users", []any{map[string]any{}}, []string{"1"}), &pfe)

	ft.enqueue(500, esErrorBody)
	assert.ErrorIs(t, c.BatchUpdateDocument(ctx, "users", []any{map[string]any{}}, []string{"1"}), ErrBatchInsertDocument)
}

func TestBatchDeleteDocument_Fake(t *testing.T) {
	ctx := context.Background()
	c, ft := newFakeESClient(t)

	assert.NoError(t, c.BatchDeleteDocument(ctx, "users", nil))

	// only blank ids -> nothing to send
	err := c.BatchDeleteDocument(ctx, "users", []string{"", ""})
	if err == nil || !strings.Contains(err.Error(), "no valid ids to delete") {
		t.Fatalf("expected no-valid-ids error, got %v", err)
	}

	ft.enqueue(200, bulkOKBody(1))
	assert.NoError(t, c.BatchDeleteDocument(ctx, "users", []string{"1"}))

	ft.enqueue(200, `{"took":1,"errors":true,"items":[{"delete":{"_id":"1","status":404,"error":{"type":"d","reason":"missing"}}}]}`)
	var pfe *PartialFailureError
	assert.ErrorAs(t, c.BatchDeleteDocument(ctx, "users", []string{"1"}), &pfe)

	ft.enqueue(500, esErrorBody)
	assert.ErrorIs(t, c.BatchDeleteDocument(ctx, "users", []string{"1"}), ErrBatchInsertDocument)
}

func TestMultiGet_Fake(t *testing.T) {
	ctx := context.Background()
	c, ft := newFakeESClient(t)

	// empty and blank-only inputs short-circuit
	res, err := c.MultiGet(ctx, "users", nil, nil)
	assert.NoError(t, err)
	assert.Nil(t, res)
	res, err = c.MultiGet(ctx, "users", []string{"", ""}, nil)
	assert.NoError(t, err)
	assert.Nil(t, res)

	ft.enqueue(200, `{"docs":[{"_index":"users","_id":"1","found":true,"_source":{"name":"alice"}},{"_index":"users","_id":"2","found":false}]}`)
	res, err = c.MultiGet(ctx, "users", []string{"1", "2"}, []string{"name"})
	assert.NoError(t, err)
	require.Len(t, res, 2)
	assert.JSONEq(t, `{"name":"alice"}`, string(res[0]))
	assert.Nil(t, res[1])

	// blank ids keep their slot nil while found docs align by request order
	ft.enqueue(200, `{"docs":[{"_index":"users","_id":"3","found":true,"_source":{"name":"carol"}}]}`)
	res, err = c.MultiGet(ctx, "users", []string{"", "3"}, nil)
	assert.NoError(t, err)
	assert.Nil(t, res[0])
	assert.JSONEq(t, `{"name":"carol"}`, string(res[1]))

	// server error maps to ErrGetDocument
	ft.enqueue(500, esErrorBody)
	_, err = c.MultiGet(ctx, "users", []string{"1"}, nil)
	assert.ErrorIs(t, err, ErrGetDocument)
}

func TestSearchVariants_Fake(t *testing.T) {
	ctx := context.Background()
	c, ft := newFakeESClient(t)

	// Search with paging + query string
	ft.enqueue(200, searchResultBody)
	sr, err := c.Search(ctx, "users", paginationV1PagingRequestES(1, 10))
	require.NoError(t, err)
	assert.Equal(t, 1, sr.Hits.Total.Value)
	require.Len(t, sr.Hits.Hits, 1)
	assert.Equal(t, "alice", mustSourceName(sr.Hits.Hits[0].Source))

	// search error
	ft.enqueue(500, esErrorBody)
	_, err = c.Search(ctx, "users", paginationV1PagingRequestES(1, 10))
	assert.ErrorIs(t, err, ErrSearchDocument)

	// SearchWithBody
	ft.enqueue(200, searchResultBody)
	sr, err = c.SearchWithBody(ctx, "users", map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
	assert.NoError(t, err)
	assert.NotNil(t, sr)

	// SearchWithHighlight
	ft.enqueue(200, searchResultBody)
	sr, err = c.SearchWithHighlight(ctx, "users",
		map[string]any{"match_all": map[string]any{}},
		map[string]any{"fields": []string{"name"}},
		[]string{"name"},
		map[string]bool{"name": true},
		0, 10,
	)
	assert.NoError(t, err)
	assert.NotNil(t, sr)

	// Count
	ft.enqueue(200, `{"count":7}`)
	n, err := c.Count(ctx, "users", map[string]any{"query": map[string]any{"match_all": map[string]any{}}})
	assert.NoError(t, err)
	assert.EqualValues(t, 7, n)
	ft.enqueue(200, `{"count":0}`)
	n, err = c.Count(ctx, "users", nil)
	assert.NoError(t, err)
	assert.Zero(t, n)
	_, err = c.Count(ctx, "", nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)

	// scroll APIs
	_, err = c.SearchScroll(ctx, "", "1m")
	assert.ErrorIs(t, err, ErrInvalidRequest)
	assert.ErrorIs(t, c.ClearScroll(ctx, ""), ErrInvalidRequest)

	ft.enqueue(200, searchResultBody)
	sr, err = c.SearchScroll(ctx, "scroll-1", "1m")
	assert.NoError(t, err)
	assert.NotNil(t, sr)

	ft.enqueue(200, okESBody)
	assert.NoError(t, c.ClearScroll(ctx, "scroll-1"))

	// scroll error path
	ft.enqueue(500, esErrorBody)
	_, err = c.SearchScroll(ctx, "scroll-1", "1m")
	assert.ErrorIs(t, err, ErrSearchDocument)

	// SearchBySQL / SearchBySQLTo
	ft.enqueue(200, `{"columns":["name"],"rows":[["alice"]]}`)
	sqlRes, err := c.SearchBySQL(ctx, "SELECT name FROM users")
	assert.NoError(t, err)
	assert.Equal(t, []string{"name"}, sqlRes.Columns)

	var sqlOut SQLResult
	ft.enqueue(200, `{"columns":["name"],"rows":[["alice"]]}`)
	assert.NoError(t, c.SearchBySQLTo(ctx, "SELECT name FROM users", &sqlOut))
	assert.Equal(t, []string{"name"}, sqlOut.Columns)

	ft.enqueue(500, esErrorBody)
	_, err = c.SearchBySQL(ctx, "SELECT 1")
	assert.ErrorIs(t, err, ErrSearchDocument)
	ft.enqueue(500, esErrorBody)
	assert.ErrorIs(t, c.SearchBySQLTo(ctx, "SELECT 1", &sqlOut), ErrSearchDocument)
}

func TestIndexAdmin_Fake(t *testing.T) {
	ctx := context.Background()
	c, ft := newFakeESClient(t)

	// templates
	ft.enqueue(200, okESBody)
	assert.NoError(t, c.CreateIndexTemplate(ctx, "tpl", `{}`))
	ft.enqueue(500, esErrorBody)
	assert.ErrorIs(t, c.CreateIndexTemplate(ctx, "tpl", `{}`), ErrCreateIndex)
	ft.enqueue(200, okESBody)
	ok, err := c.ExistsIndexTemplate(ctx, "tpl")
	assert.NoError(t, err)
	assert.True(t, ok)

	// ILM policies
	ft.enqueue(200, okESBody)
	assert.NoError(t, c.CreateILMPolicy(ctx, "pol", `{}`))
	ft.enqueue(500, esErrorBody)
	assert.ErrorIs(t, c.CreateILMPolicy(ctx, "pol", `{}`), ErrCreateILMPolicy)
	ft.enqueue(200, okESBody)
	assert.NoError(t, c.DeleteILMPolicy(ctx, "pol"))
	ft.enqueue(500, esErrorBody)
	assert.ErrorIs(t, c.DeleteILMPolicy(ctx, "pol"), ErrDeleteIndex)
	ft.enqueue(200, `{"pol":{"version":1}}`)
	pol, err := c.GetILMPolicy(ctx, "pol")
	assert.NoError(t, err)
	assert.NotNil(t, pol)

	// aliases
	ft.enqueue(200, okESBody)
	assert.NoError(t, c.CreateAlias(ctx, "users-alias", "users", `{}`))
	ft.enqueue(500, esErrorBody)
	assert.ErrorIs(t, c.CreateAlias(ctx, "users-alias", "users", `{}`), ErrCreateIndex)
	ft.enqueue(200, okESBody)
	assert.NoError(t, c.DeleteAlias(ctx, "users-alias", "users"))
	ft.enqueue(500, esErrorBody)
	assert.ErrorIs(t, c.DeleteAlias(ctx, "users-alias", "users"), ErrDeleteIndex)
	ft.enqueue(200, `{"users":{"aliases":{"users-alias":{}}}}`)
	aliases, err := c.GetAlias(ctx, "users-alias")
	assert.NoError(t, err)
	assert.NotNil(t, aliases)
	ft.enqueue(200, "")
	exists, err := c.ExistsAlias(ctx, "users-alias")
	assert.NoError(t, err)
	assert.True(t, exists)

	// mapping and settings
	ft.enqueue(200, `{"users":{"mappings":{}}}`)
	m, err := c.GetMapping(ctx, "users")
	assert.NoError(t, err)
	assert.NotNil(t, m)
	ft.enqueue(500, esErrorBody)
	_, err = c.GetMapping(ctx, "users")
	assert.ErrorIs(t, err, ErrGetDocument)
	ft.enqueue(200, okESBody)
	assert.NoError(t, c.PutMapping(ctx, "users", UserMapping))
	ft.enqueue(500, esErrorBody)
	assert.ErrorIs(t, c.PutMapping(ctx, "users", UserMapping), ErrCreateIndex)
	ft.enqueue(200, `{"users":{"settings":{}}}`)
	s, err := c.GetSettings(ctx, "users")
	assert.NoError(t, err)
	assert.NotNil(t, s)
	ft.enqueue(200, okESBody)
	assert.NoError(t, c.PutSettings(ctx, "users", `{"number_of_replicas":1}`))

	// index state operations
	for name, call := range map[string]func() error{
		"open":    func() error { return c.OpenIndex(ctx, "users") },
		"close":   func() error { return c.CloseIndex(ctx, "users") },
		"refresh": func() error { return c.RefreshIndex(ctx, "users") },
		"flush":   func() error { return c.FlushIndex(ctx, "users") },
	} {
		ft.enqueue(200, okESBody)
		assert.NoError(t, call(), name)
		ft.enqueue(500, esErrorBody)
		assert.ErrorIs(t, call(), ErrCreateIndex, name)
	}

	// cluster level
	ft.enqueue(200, `{"status":"green"}`)
	h, err := c.ClusterHealth(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "green", h["status"])
	ft.enqueue(500, esErrorBody)
	_, err = c.ClusterHealth(ctx)
	assert.ErrorIs(t, err, ErrRequestFailed)

	// ClusterInfo 缺省 target 必须兜底为 "_all"（esapi 拒绝空 target），
	// 请求应正常打到服务端并返回集群信息。
	ft.enqueue(200, `{"cluster_name":"es","version":{"number":"8.14.0"}}`)
	info, err := c.ClusterInfo(ctx)
	assert.NoError(t, err)
	assert.Equal(t, "es", info["cluster_name"])
	ft.enqueue(500, esErrorBody)
	_, err = c.ClusterInfo(ctx)
	assert.ErrorIs(t, err, ErrRequestFailed)

	// snapshots
	ft.enqueue(200, okESBody)
	assert.NoError(t, c.CreateSnapshotRepository(ctx, "repo", `{"type":"fs"}`))
	ft.enqueue(500, esErrorBody)
	assert.ErrorIs(t, c.CreateSnapshotRepository(ctx, "repo", `{}`), ErrRequestFailed)
	ft.enqueue(200, `{"repo":{"type":"fs"}}`)
	repo, err := c.GetSnapshotRepository(ctx, "repo")
	assert.NoError(t, err)
	assert.NotNil(t, repo)
	ft.enqueue(200, okESBody)
	assert.NoError(t, c.DeleteSnapshotRepository(ctx, "repo"))

	ft.enqueue(200, okESBody)
	assert.NoError(t, c.CreateSnapshot(ctx, "repo", "snap", `{}`))
	ft.enqueue(500, esErrorBody)
	assert.ErrorIs(t, c.CreateSnapshot(ctx, "repo", "snap", `{}`), ErrRequestFailed)
	ft.enqueue(200, `{"snapshots":[]}`)
	snap, err := c.GetSnapshot(ctx, "repo", "snap")
	assert.NoError(t, err)
	assert.NotNil(t, snap)
	ft.enqueue(200, okESBody)
	assert.NoError(t, c.DeleteSnapshot(ctx, "repo", "snap"))
}
