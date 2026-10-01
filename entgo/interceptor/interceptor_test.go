package interceptor

import (
	"context"
	"errors"
	"strings"
	"testing"

	"entgo.io/ent"
	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"

	"github.com/tx7do/go-crud/viewer"
)

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

// fakePredicate mirrors the generated predicate.X types: a named function type
// over *sql.Selector (the reflection code must accept named predicate types,
// not only the raw func(*sql.Selector)).
type fakePredicate func(*sql.Selector)

// fakeQuery is a minimal query builder with the Where(...predicates) shape the
// reflection injection targets. ent.Query is `any` in ent v0.14, so no other
// methods are required to pass it around as ent.Query.
type fakeQuery struct {
	preds []fakePredicate
	limit *int
}

func (f *fakeQuery) Where(ps ...fakePredicate) { f.preds = append(f.preds, ps...) }
func (f *fakeQuery) Limit(n int)               { v := n; f.limit = &v }

// stubViewer is a configurable viewer.Context.
type stubViewer struct {
	tid    uint64
	system bool
}

func (s *stubViewer) UserID() uint64                { return 0 }
func (s *stubViewer) TenantID() uint64              { return s.tid }
func (s *stubViewer) OrgUnitID() uint64             { return 0 }
func (s *stubViewer) Permissions() []string         { return nil }
func (s *stubViewer) Roles() []string               { return nil }
func (s *stubViewer) DataScope() []viewer.DataScope { return nil }
func (s *stubViewer) TraceID() string               { return "" }
func (s *stubViewer) HasPermission(_, _ string) bool {
	return false
}
func (s *stubViewer) IsPlatformContext() bool { return s.tid == 0 }
func (s *stubViewer) IsTenantContext() bool   { return s.tid > 0 }
func (s *stubViewer) IsSystemContext() bool   { return s.system }
func (s *stubViewer) ShouldAudit() bool       { return false }

func tenantCtx(tid uint64) context.Context {
	return viewer.WithContext(context.Background(), &stubViewer{tid: tid})
}

func systemCtx() context.Context {
	return viewer.WithContext(context.Background(), &stubViewer{system: true})
}

// appliedTo builds a selector and applies the captured predicates to it,
// returning the rendered SQL and its bound args.
func (f *fakeQuery) appliedTo(t *testing.T) (string, []any) {
	t.Helper()
	s := sql.Dialect(dialect.SQLite).Select().From(sql.Table("fake"))
	for _, p := range f.preds {
		p(s)
	}
	query, args := s.Query()
	return query, args
}

// capturingQuerier records the query object handed down the pipeline.
type capturingQuerier struct {
	gotQuery ent.Query
	calls    int
}

func (c *capturingQuerier) Query(_ context.Context, q ent.Query) (ent.Value, error) {
	c.calls++
	c.gotQuery = q
	return "ok", nil
}

// ---------------------------------------------------------------------------
// injectTenantWhere
// ---------------------------------------------------------------------------

func TestInjectTenantWhere_NamedPredicateType(t *testing.T) {
	q := &fakeQuery{}
	if err := injectTenantWhere(q, 7); err != nil {
		t.Fatalf("injectTenantWhere: %v", err)
	}
	if len(q.preds) != 1 {
		t.Fatalf("expected 1 injected predicate, got %d", len(q.preds))
	}
	query, args := q.appliedTo(t)
	if !strings.Contains(query, "tenant_id") {
		t.Errorf("injected SQL must reference tenant_id, got %q", query)
	}
	if !strings.Contains(query, "IS NULL") == false && !strings.Contains(query, "=") {
		t.Errorf("injected SQL must be an equality predicate, got %q", query)
	}
	if len(args) != 1 {
		t.Fatalf("expected 1 bound arg, got %v", args)
	}
	if v, ok := args[0].(uint64); !ok || v != 7 {
		t.Errorf("bound arg = %v (%T), want uint64(7)", args[0], args[0])
	}
}

func TestInjectTenantWhere_NoWhereMethod(t *testing.T) {
	// fail-closed: a query without a usable Where method must error.
	if err := injectTenantWhere(struct{}{}, 7); err == nil {
		t.Error("injectTenantWhere on a Where-less query must return an error")
	}
}

func TestInjectTenantWhere_NonVariadicWhere(t *testing.T) {
	q := &nonVariadicQuery{}
	if err := injectTenantWhere(q, 7); err == nil {
		t.Error("non-variadic Where must fail the signature check")
	}
}

type nonVariadicQuery struct{ got int }

func (n *nonVariadicQuery) Where(p fakePredicate) { n.got++ }

func TestInjectTenantWhere_BadPredicateSignature(t *testing.T) {
	q := &badPredicateQuery{}
	if err := injectTenantWhere(q, 7); err == nil {
		t.Error("Where with non-func predicate element must fail the signature check")
	}
}

type badPredicateQuery struct{ got int }

func (b *badPredicateQuery) Where(p ...int) { b.got++ }

// ---------------------------------------------------------------------------
// injectSoftDeleteWhere
// ---------------------------------------------------------------------------

func TestInjectSoftDeleteWhere(t *testing.T) {
	q := &fakeQuery{}
	if err := injectSoftDeleteWhere(q); err != nil {
		t.Fatalf("injectSoftDeleteWhere: %v", err)
	}
	if len(q.preds) != 1 {
		t.Fatalf("expected 1 injected predicate, got %d", len(q.preds))
	}
	query, _ := q.appliedTo(t)
	if !strings.Contains(query, "deleted_at") {
		t.Errorf("injected SQL must reference deleted_at, got %q", query)
	}
	if !strings.Contains(query, "IS NULL") {
		t.Errorf("soft-delete predicate must filter deleted_at IS NULL, got %q", query)
	}
}

func TestInjectSoftDeleteWhere_NoWhereMethod(t *testing.T) {
	if err := injectSoftDeleteWhere(struct{}{}); err == nil {
		t.Error("injectSoftDeleteWhere on a Where-less query must return an error")
	}
}

func TestInjectSoftDeleteWhere_BadPredicateSignature(t *testing.T) {
	if err := injectSoftDeleteWhere(&badPredicateQuery{}); err == nil {
		t.Error("bad predicate signature must return an error")
	}
}

// ---------------------------------------------------------------------------
// TenantInterceptor
// ---------------------------------------------------------------------------

func TestTenantInterceptor_MissingViewerFailClosed(t *testing.T) {
	next := &capturingQuerier{}
	q := TenantInterceptor().Intercept(next)

	fq := &fakeQuery{}
	if _, err := q.Query(context.Background(), fq); err == nil {
		t.Error("missing ViewerContext must fail-closed with an error")
	}
	if next.calls != 0 {
		t.Error("fail-closed must not delegate to the next querier")
	}
	if len(fq.preds) != 0 {
		t.Error("fail-closed must not inject predicates")
	}
}

func TestTenantInterceptor_PlatformContextPassThrough(t *testing.T) {
	next := &capturingQuerier{}
	q := TenantInterceptor().Intercept(next)

	fq := &fakeQuery{}
	v, err := q.Query(tenantCtx(0), fq)
	if err != nil {
		t.Fatalf("platform context must pass through, got %v", err)
	}
	if v != "ok" {
		t.Errorf("unexpected value %v, want the next querier's result", v)
	}
	if next.calls != 1 {
		t.Errorf("next querier must be invoked once, got %d", next.calls)
	}
	if len(fq.preds) != 0 {
		t.Errorf("platform context must not inject predicates, got %d", len(fq.preds))
	}
}

func TestTenantInterceptor_SystemContextPassThrough(t *testing.T) {
	next := &capturingQuerier{}
	q := TenantInterceptor().Intercept(next)

	fq := &fakeQuery{}
	if _, err := q.Query(systemCtx(), fq); err != nil {
		t.Fatalf("system context must pass through, got %v", err)
	}
	if len(fq.preds) != 0 {
		t.Errorf("system context must not inject predicates, got %d", len(fq.preds))
	}
}

func TestTenantInterceptor_TenantContextInjectsPredicate(t *testing.T) {
	next := &capturingQuerier{}
	q := TenantInterceptor().Intercept(next)

	fq := &fakeQuery{}
	if _, err := q.Query(tenantCtx(42), fq); err != nil {
		t.Fatalf("tenant context query: %v", err)
	}
	if next.calls != 1 {
		t.Fatalf("next querier must be invoked, got %d calls", next.calls)
	}
	// The next querier must still receive the same (now-filtered) query object.
	if next.gotQuery != ent.Query(fq) {
		t.Errorf("next querier must receive the original query, got %T", next.gotQuery)
	}
	if len(fq.preds) != 1 {
		t.Fatalf("expected 1 injected predicate, got %d", len(fq.preds))
	}
	query, args := fq.appliedTo(t)
	if !strings.Contains(query, "tenant_id") {
		t.Errorf("injected SQL must reference tenant_id, got %q", query)
	}
	if len(args) != 1 || args[0] != uint64(42) {
		t.Errorf("bound args = %v, want [uint64(42)]", args)
	}
}

// ---------------------------------------------------------------------------
// SoftDeleteInterceptor
// ---------------------------------------------------------------------------

func TestSoftDeleteInterceptor_InjectsDeletedAtFilter(t *testing.T) {
	next := &capturingQuerier{}
	q := SoftDeleteInterceptor().Intercept(next)

	fq := &fakeQuery{}
	// Works with or without a viewer in context: soft-delete filtering is
	// unconditional.
	if _, err := q.Query(context.Background(), fq); err != nil {
		t.Fatalf("soft delete interceptor: %v", err)
	}
	if next.calls != 1 {
		t.Fatalf("next querier must be invoked, got %d calls", next.calls)
	}
	query, _ := fq.appliedTo(t)
	if !strings.Contains(query, "deleted_at") || !strings.Contains(query, "IS NULL") {
		t.Errorf("query must filter deleted_at IS NULL, got %q", query)
	}
}

func TestSoftDeleteInterceptor_PropagatesFailure(t *testing.T) {
	next := &capturingQuerier{}
	q := SoftDeleteInterceptor().Intercept(next)

	if _, err := q.Query(context.Background(), struct{}{}); err == nil {
		t.Error("query without a usable Where must fail closed")
	}
	if next.calls != 0 {
		t.Error("failed injection must not delegate to the next querier")
	}
}

// ---------------------------------------------------------------------------
// SharedLimiter
// ---------------------------------------------------------------------------

func TestSharedLimiter_AppliesFallbackLimit(t *testing.T) {
	convertedCalls := 0
	f := func(q ent.Query) (*fakeQuery, error) {
		convertedCalls++
		return q.(*fakeQuery), nil
	}
	next := &capturingQuerier{}
	q := SharedLimiter(f, 100).Intercept(next)

	// QueryContext present with a nil Limit: the caller has not set a Limit.
	// (In real ent runtime the generated query always stores a QueryContext
	// in ctx before interceptors run.)
	ctx := ent.NewQueryContext(context.Background(), &ent.QueryContext{
		Op:   "All",
		Type: "Fake",
	})

	fq := &fakeQuery{}
	if _, err := q.Query(ctx, fq); err != nil {
		t.Fatalf("SharedLimiter query: %v", err)
	}
	if convertedCalls != 1 {
		t.Errorf("converter must be invoked once, got %d", convertedCalls)
	}
	if fq.limit == nil || *fq.limit != 100 {
		t.Errorf("fallback limit must be applied, got %v", fq.limit)
	}
	if next.calls != 1 {
		t.Errorf("next querier must be invoked once, got %d", next.calls)
	}
	if next.gotQuery != ent.Query(fq) {
		t.Errorf("the limit-applied object must be executed, got %T", next.gotQuery)
	}
}

func TestSharedLimiter_RespectsCallerLimit(t *testing.T) {
	convertedCalls := 0
	f := func(q ent.Query) (*fakeQuery, error) {
		convertedCalls++
		return q.(*fakeQuery), nil
	}
	next := &capturingQuerier{}
	q := SharedLimiter(f, 100).Intercept(next)

	// A QueryContext with a non-nil Limit means the caller set one explicitly.
	callerLimit := 10
	ctx := ent.NewQueryContext(context.Background(), &ent.QueryContext{
		Op:    "First",
		Type:  "Fake",
		Limit: &callerLimit,
	})

	fq := &fakeQuery{}
	if _, err := q.Query(ctx, fq); err != nil {
		t.Fatalf("SharedLimiter query: %v", err)
	}
	if convertedCalls != 0 {
		t.Error("caller-set Limit must bypass the converter entirely")
	}
	if fq.limit != nil {
		t.Errorf("caller-set Limit must not be overwritten, got %v", *fq.limit)
	}
	if next.calls != 1 {
		t.Errorf("next querier must be invoked once, got %d", next.calls)
	}
}

func TestSharedLimiter_ConverterErrorPropagates(t *testing.T) {
	wantErr := errors.New("convert failed")
	f := func(q ent.Query) (*fakeQuery, error) { return nil, wantErr }
	next := &capturingQuerier{}
	q := SharedLimiter(f, 100).Intercept(next)

	// The caller-limit lookup happens before the converter runs, so a
	// QueryContext with a nil Limit must be present in ctx.
	ctx := ent.NewQueryContext(context.Background(), &ent.QueryContext{Op: "All", Type: "Fake"})

	v, err := q.Query(ctx, &fakeQuery{})
	if !errors.Is(err, wantErr) {
		t.Errorf("converter error must propagate, got %v", err)
	}
	if v != nil {
		t.Errorf("failed query must not return a value, got %v", v)
	}
	if next.calls != 0 {
		t.Error("failed conversion must not delegate to the next querier")
	}
}

// A context without any QueryContext must fall back to the shared limit
// instead of panicking on a nil QueryContext dereference.
func TestSharedLimiter_NilQueryContextAppliesFallback(t *testing.T) {
	convertedCalls := 0
	f := func(q ent.Query) (*fakeQuery, error) {
		convertedCalls++
		return q.(*fakeQuery), nil
	}
	next := &capturingQuerier{}
	q := SharedLimiter(f, 50).Intercept(next)

	// Plain background context: ent.QueryFromContext returns nil.
	fq := &fakeQuery{}
	if _, err := q.Query(context.Background(), fq); err != nil {
		t.Fatalf("SharedLimiter query: %v", err)
	}
	if convertedCalls != 1 {
		t.Errorf("converter must be invoked once, got %d", convertedCalls)
	}
	if fq.limit == nil || *fq.limit != 50 {
		t.Errorf("fallback limit must be applied, got %v", fq.limit)
	}
	if next.calls != 1 {
		t.Errorf("next querier must be invoked once, got %d", next.calls)
	}
}
