package pagination

import (
	"testing"

	"github.com/tx7do/go-crud/opensearch/query"
	"github.com/tx7do/go-crud/pagination"
	"github.com/tx7do/go-crud/pagination/paginator"
)

// buildDSL is a helper that runs a fresh builder through Build and returns the DSL map.
func buildDSL(t *testing.T, b *query.Builder) map[string]any {
	t.Helper()
	if b == nil {
		t.Fatal("builder is nil")
	}
	dsl := b.Build()
	if dsl == nil {
		t.Fatal("Build() returned nil")
	}
	return dsl
}

// fromSize extracts the "from" and "size" values from a built DSL map.
func fromSize(t *testing.T, dsl map[string]any) (int, int) {
	t.Helper()
	from, ok := dsl["from"].(int)
	if !ok {
		t.Fatalf("dsl[\"from\"] missing or not int: %v (%T)", dsl["from"], dsl["from"])
	}
	size, ok := dsl["size"].(int)
	if !ok {
		t.Fatalf("dsl[\"size\"] missing or not int: %v (%T)", dsl["size"], dsl["size"])
	}
	return from, size
}

// boolFilter returns the filter slice of the built bool query (nil if absent).
func boolFilter(dsl map[string]any) []map[string]any {
	q, ok := dsl["query"].(map[string]any)
	if !ok {
		return nil
	}
	b, ok := q["bool"].(map[string]any)
	if !ok {
		return nil
	}
	f, ok := b["filter"].([]map[string]any)
	if !ok {
		return nil
	}
	return f
}

// ---------------------------------------------------------------------------
// OffsetPaginator
// ---------------------------------------------------------------------------

func TestNewOffsetPaginator(t *testing.T) {
	p := NewOffsetPaginator()
	if p == nil {
		t.Fatal("NewOffsetPaginator returned nil")
	}
	if p.impl == nil {
		t.Fatal("NewOffsetPaginator should initialize the internal paginator")
	}
	if p.impl.Limit() != paginator.DefaultLimit {
		t.Errorf("default limit = %d, want %d", p.impl.Limit(), paginator.DefaultLimit)
	}
}

func TestOffsetPaginator_BuildClause(t *testing.T) {
	p := NewOffsetPaginator()

	dsl := buildDSL(t, p.BuildClause(query.NewQueryBuilder(), 20, 5))
	from, size := fromSize(t, dsl)

	if from != 20 {
		t.Errorf("from = %d, want 20", from)
	}
	if size != 5 {
		t.Errorf("size = %d, want 5", size)
	}
}

func TestOffsetPaginator_BuildClause_ZeroOffset(t *testing.T) {
	p := NewOffsetPaginator()

	dsl := buildDSL(t, p.BuildClause(query.NewQueryBuilder(), 0, 10))
	from, size := fromSize(t, dsl)

	if from != 0 {
		t.Errorf("from = %d, want 0", from)
	}
	if size != 10 {
		t.Errorf("size = %d, want 10", size)
	}
}

func TestOffsetPaginator_BuildClause_NegativeOffsetClamped(t *testing.T) {
	p := NewOffsetPaginator()

	// A negative offset is normalized to 0 by the underlying paginator.
	dsl := buildDSL(t, p.BuildClause(query.NewQueryBuilder(), -10, 5))
	from, size := fromSize(t, dsl)

	if from != 0 {
		t.Errorf("from = %d, want 0", from)
	}
	if size != 5 {
		t.Errorf("size = %d, want 5", size)
	}
}

func TestOffsetPaginator_BuildClause_ZeroLimitBecomesOne(t *testing.T) {
	p := NewOffsetPaginator()

	// The underlying paginator clamps limit < 1 to 1, so size becomes 1
	// rather than keeping the builder default of 10.
	dsl := buildDSL(t, p.BuildClause(query.NewQueryBuilder(), 0, 0))
	_, size := fromSize(t, dsl)

	if size != 1 {
		t.Errorf("size = %d, want 1 (limit clamped to minimum)", size)
	}
}

func TestOffsetPaginator_BuildClause_LimitClampedToMax(t *testing.T) {
	oldMax := paginator.MaxLimit
	defer func() { paginator.MaxLimit = oldMax }()
	paginator.MaxLimit = paginator.DefaultMaxLimit

	p := NewOffsetPaginator()

	// Limits above MaxLimit are clamped to 100.
	dsl := buildDSL(t, p.BuildClause(query.NewQueryBuilder(), 0, 500))
	_, size := fromSize(t, dsl)

	if size != paginator.DefaultMaxLimit {
		t.Errorf("size = %d, want %d (MaxLimit clamp)", size, paginator.DefaultMaxLimit)
	}
}

func TestOffsetPaginator_BuildClause_ReturnsSameBuilder(t *testing.T) {
	p := NewOffsetPaginator()
	b := query.NewQueryBuilder()

	if got := p.BuildClause(b, 0, 5); got != b {
		t.Error("BuildClause should return the same builder instance")
	}
}

// ---------------------------------------------------------------------------
// PagePaginator
// ---------------------------------------------------------------------------

func TestNewPagePaginator(t *testing.T) {
	p := NewPagePaginator()
	if p == nil {
		t.Fatal("NewPagePaginator returned nil")
	}
	if p.impl == nil {
		t.Fatal("NewPagePaginator should initialize the internal paginator")
	}
}

func TestPagePaginator_BuildClause(t *testing.T) {
	p := NewPagePaginator()

	// page 2 with size 5 means from = (2-1)*5 = 5.
	dsl := buildDSL(t, p.BuildClause(query.NewQueryBuilder(), 2, 5))
	from, size := fromSize(t, dsl)

	if from != 5 {
		t.Errorf("from = %d, want 5", from)
	}
	if size != 5 {
		t.Errorf("size = %d, want 5", size)
	}
}

func TestPagePaginator_BuildClause_FirstPage(t *testing.T) {
	p := NewPagePaginator()

	dsl := buildDSL(t, p.BuildClause(query.NewQueryBuilder(), 1, 10))
	from, size := fromSize(t, dsl)

	if from != 0 {
		t.Errorf("from = %d, want 0", from)
	}
	if size != 10 {
		t.Errorf("size = %d, want 10", size)
	}
}

func TestPagePaginator_BuildClause_ZeroPageClamped(t *testing.T) {
	p := NewPagePaginator()

	// SetPage normalizes page < 1 to 1.
	dsl := buildDSL(t, p.BuildClause(query.NewQueryBuilder(), 0, 10))
	from, _ := fromSize(t, dsl)

	if from != 0 {
		t.Errorf("from = %d, want 0", from)
	}
}

func TestPagePaginator_BuildClause_ZeroSizeUsesDefault(t *testing.T) {
	p := NewPagePaginator()

	// SetPage normalizes size <= 0 to the default of 10.
	dsl := buildDSL(t, p.BuildClause(query.NewQueryBuilder(), 3, 0))
	from, size := fromSize(t, dsl)

	if from != 20 {
		t.Errorf("from = %d, want 20 (page 3 with default size 10)", from)
	}
	if size != 10 {
		t.Errorf("size = %d, want 10 (default)", size)
	}
}

func TestPagePaginator_BuildClause_LargeSizeClamped(t *testing.T) {
	p := NewPagePaginator()

	// A size above MaxLimit must be clamped before it reaches the DSL.
	dsl := buildDSL(t, p.BuildClause(query.NewQueryBuilder(), 1, 500))
	_, size := fromSize(t, dsl)

	if size != paginator.MaxLimit {
		t.Errorf("size = %d, want %d (clamped to MaxLimit)", size, paginator.MaxLimit)
	}
}

func TestPagePaginator_BuildClause_ReturnsSameBuilder(t *testing.T) {
	p := NewPagePaginator()
	b := query.NewQueryBuilder()

	if got := p.BuildClause(b, 2, 10); got != b {
		t.Error("BuildClause should return the same builder instance")
	}
}

// ---------------------------------------------------------------------------
// TokenPaginator
// ---------------------------------------------------------------------------

func TestNewTokenPaginator(t *testing.T) {
	p := NewTokenPaginator()
	if p == nil {
		t.Fatal("NewTokenPaginator returned nil")
	}
	if p.impl == nil {
		t.Fatal("NewTokenPaginator should initialize the internal paginator")
	}
}

func TestTokenPaginator_BuildClause_EmptyToken(t *testing.T) {
	p := NewTokenPaginator()

	// Without a token only from=0/size=N is set, no range filter.
	b := p.BuildClause(query.NewQueryBuilder(), "", 3)
	dsl := buildDSL(t, b)
	from, size := fromSize(t, dsl)

	if from != 0 {
		t.Errorf("from = %d, want 0", from)
	}
	if size != 3 {
		t.Errorf("size = %d, want 3", size)
	}
	if f := boolFilter(dsl); len(f) != 0 {
		t.Errorf("filter = %v, want empty", f)
	}
}

func TestTokenPaginator_BuildClause_InvalidToken(t *testing.T) {
	p := NewTokenPaginator()

	// An undecodable token degrades to a plain from=0 page.
	dsl := buildDSL(t, p.BuildClause(query.NewQueryBuilder(), "not-a-valid-token", 4))
	from, size := fromSize(t, dsl)

	if from != 0 {
		t.Errorf("from = %d, want 0", from)
	}
	if size != 4 {
		t.Errorf("size = %d, want 4", size)
	}
	if f := boolFilter(dsl); len(f) != 0 {
		t.Errorf("filter = %v, want empty for invalid token", f)
	}
}

func TestTokenPaginator_BuildClause_PlainToken(t *testing.T) {
	// With no secret configured, EncodeAndSign produces a legacy unsigned token.
	token := pagination.EncodeAndSign(42, nil)

	p := NewTokenPaginator()
	dsl := buildDSL(t, p.BuildClause(query.NewQueryBuilder(), token, 5))
	from, size := fromSize(t, dsl)

	if from != 0 {
		t.Errorf("from = %d, want 0", from)
	}
	if size != 5 {
		t.Errorf("size = %d, want 5", size)
	}

	f := boolFilter(dsl)
	if len(f) != 1 {
		t.Fatalf("filter length = %d, want 1", len(f))
	}
	rangeMap, ok := f[0]["range"].(map[string]any)
	if !ok {
		t.Fatalf("filter[0] = %v, want a range clause", f[0])
	}
	idRange, ok := rangeMap["id"].(map[string]any)
	if !ok {
		t.Fatalf("range clause = %v, want an id field entry", rangeMap)
	}
	if idRange["gte"] != int64(42) {
		t.Errorf("range gte = %v (%T), want 42 (int64)", idRange["gte"], idRange["gte"])
	}
}

func TestTokenPaginator_BuildClause_SignedToken(t *testing.T) {
	secret := []byte("unit-test-secret")
	pagination.SetTokenSecret(secret)
	defer pagination.SetTokenSecret(nil)

	token := pagination.EncodeAndSign(7, pagination.TokenSecret())

	p := NewTokenPaginator()
	dsl := buildDSL(t, p.BuildClause(query.NewQueryBuilder(), token, 6))
	from, size := fromSize(t, dsl)

	if from != 0 {
		t.Errorf("from = %d, want 0", from)
	}
	if size != 6 {
		t.Errorf("size = %d, want 6", size)
	}

	f := boolFilter(dsl)
	if len(f) != 1 {
		t.Fatalf("filter length = %d, want 1", len(f))
	}
	rangeMap := f[0]["range"].(map[string]any)
	idRange := rangeMap["id"].(map[string]any)
	if idRange["gte"] != int64(7) {
		t.Errorf("range gte = %v, want 7", idRange["gte"])
	}
}

func TestTokenPaginator_BuildClause_SignedTokenRejectedWithoutSecret(t *testing.T) {
	// A signed token must be rejected when no secret is configured.
	pagination.SetTokenSecret(nil)

	token := pagination.EncodeAndSign(7, []byte("some-secret"))

	p := NewTokenPaginator()
	dsl := buildDSL(t, p.BuildClause(query.NewQueryBuilder(), token, 6))
	_, size := fromSize(t, dsl)

	if size != 6 {
		t.Errorf("size = %d, want 6", size)
	}
	if f := boolFilter(dsl); len(f) != 0 {
		t.Errorf("filter = %v, want empty when the signed token is rejected", f)
	}
}

func TestTokenPaginator_BuildClause_ZeroSizeBecomesOne(t *testing.T) {
	p := NewTokenPaginator()

	// The underlying paginator clamps size < 1 to 1.
	dsl := buildDSL(t, p.BuildClause(query.NewQueryBuilder(), "", 0))
	_, size := fromSize(t, dsl)

	if size != 1 {
		t.Errorf("size = %d, want 1 (size clamped to minimum)", size)
	}
}

func TestTokenPaginator_BuildClause_ReturnsSameBuilder(t *testing.T) {
	p := NewTokenPaginator()
	b := query.NewQueryBuilder()

	if got := p.BuildClause(b, "", 5); got != b {
		t.Error("BuildClause should return the same builder instance")
	}
}
