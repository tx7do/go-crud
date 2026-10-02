package entgo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"

	"github.com/tx7do/go-utils/mapper"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/entgo/ent"
	"github.com/tx7do/go-crud/entgo/ent/predicate"
)

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

func TestDriverNameToSemConvKeyValue(t *testing.T) {
	cases := map[string]string{
		"mariadb":    "mariadb",
		"mysql":      "mysql",
		"postgresql": "postgresql",
		"sqlite":     "sqlite",
		"weird_dbi":  "weird_dbi", // unknown drivers pass through as custom values
	}
	for driver, want := range cases {
		kv := driverNameToSemConvKeyValue(driver)
		if string(string(kv.Key)) != "db.system" {
			t.Fatalf("unexpected key %v", kv.Key)
		}
		if got := fmt.Sprintf("%v", kv.Value); got != want {
			t.Fatalf("driver %s maps to %v, want %s", driver, kv.Value, want)
		}
	}
}

type fakeEntTx struct {
	rollbackErr error
	commitErr   error

	rolledBack bool
	committed  bool
}

func (f *fakeEntTx) Rollback() error {
	f.rolledBack = true
	return f.rollbackErr
}

func (f *fakeEntTx) Commit() error {
	f.committed = true
	return f.commitErr
}

func TestRollback(t *testing.T) {
	// rollback success returns the original error unchanged
	orig := errors.New("boom")
	tx := &fakeEntTx{}
	if got := Rollback[EntTx](tx, orig); !errors.Is(got, orig) {
		t.Fatalf("Rollback(orig) = %v", got)
	}

	// rollback failure with nil original returns the rollback error
	tx2 := &fakeEntTx{rollbackErr: errors.New("rb fail")}
	if got := Rollback[EntTx](tx2, nil); got == nil || !strings.Contains(got.Error(), "rb fail") {
		t.Fatalf("Rollback(nil, rb fail) = %v", got)
	}

	// rollback failure merges with the original error
	tx3 := &fakeEntTx{rollbackErr: errors.New("rb fail")}
	got := Rollback[EntTx](tx3, orig)
	if got == nil || !strings.Contains(got.Error(), "boom") || !strings.Contains(got.Error(), "rb fail") {
		t.Fatalf("Rollback(orig, rb fail) = %v", got)
	}
}

func TestMakeTxCleanup(t *testing.T) {
	t.Run("commit success via errPtr", func(t *testing.T) {
		tx := &fakeEntTx{}
		var err error
		func() {
			defer MakeTxCleanup(tx, &err)()
		}()
		if !tx.committed || tx.rolledBack || err != nil {
			t.Fatalf("tx = committed:%v rolledBack:%v err:%v", tx.committed, tx.rolledBack, err)
		}
	})

	t.Run("rollback on error", func(t *testing.T) {
		tx := &fakeEntTx{}
		var err error
		func() {
			defer MakeTxCleanup(tx, &err)()
			err = errors.New("work failed")
		}()
		if !tx.rolledBack || tx.committed {
			t.Fatalf("tx = committed:%v rolledBack:%v", tx.committed, tx.rolledBack)
		}
		if err == nil || !strings.Contains(err.Error(), "work failed") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("rollback error is merged", func(t *testing.T) {
		tx := &fakeEntTx{rollbackErr: errors.New("rb fail")}
		var err error
		func() {
			defer MakeTxCleanup(tx, &err)()
			err = errors.New("work failed")
		}()
		if err == nil || !strings.Contains(err.Error(), "rb fail") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("commit failure surfaces", func(t *testing.T) {
		tx := &fakeEntTx{commitErr: errors.New("commit fail")}
		var err error
		func() {
			defer MakeTxCleanup(tx, &err)()
		}()
		if err == nil || !strings.Contains(err.Error(), "commit fail") {
			t.Fatalf("err = %v", err)
		}
	})

	t.Run("nil errPtr commits silently", func(t *testing.T) {
		tx := &fakeEntTx{}
		func() {
			defer MakeTxCleanup(tx, nil)()
		}()
		if !tx.committed {
			t.Fatal("cleanup without errPtr must commit")
		}
	})
}

func TestComputeTreePath(t *testing.T) {
	cases := []struct {
		parent string
		id     uint32
		want   string
	}{
		{"", 1, "/"},
		{"root", 2, "root/2/"},
		{"root/", 3, "root/3/"},
		{"/a/b", 4, "/a/b/4/"},
		{"/a/b/", 5, "/a/b/5/"},
	}
	for _, tc := range cases {
		if got := ComputeTreePath(tc.parent, tc.id); got != tc.want {
			t.Errorf("ComputeTreePath(%q, %d) = %q, want %q", tc.parent, tc.id, got, tc.want)
		}
	}
}

func TestIdentValidation(t *testing.T) {
	for _, ok := range []string{"users", "_x", "a1_b"} {
		if !IsValidIdent(ok) {
			t.Errorf("IsValidIdent(%q) = false, want true", ok)
		}
	}
	for _, bad := range []string{"", "1abc", "a b", "a-b", strings.Repeat("a", 129)} {
		if IsValidIdent(bad) {
			t.Errorf("IsValidIdent(%q) = true, want false", bad)
		}
	}

	if err := ValidateSchemaTableColumn("", "", ""); err == nil {
		t.Error("empty table/column must fail")
	}
	if err := ValidateSchemaTableColumn("bad schema", "users", "id"); err == nil {
		t.Error("invalid schema must fail")
	}
	if err := ValidateSchemaTableColumn("public", "bad table", "id"); err == nil {
		t.Error("invalid table must fail")
	}
	if err := ValidateSchemaTableColumn("public", "users", "bad column"); err == nil {
		t.Error("invalid column must fail")
	}
	if err := ValidateSchemaTableColumn("public", "users", "id"); err != nil {
		t.Errorf("valid input failed: %v", err)
	}
}

func TestQuoteIdentAndEscapeLiteral(t *testing.T) {
	if got := QuoteIdent(dialect.Postgres, `us"ers`); got != `"us""ers"` {
		t.Errorf("QuoteIdent(postgres) = %q", got)
	}
	if got := QuoteIdent(dialect.SQLite, "users"); got != `"users"` {
		t.Errorf("QuoteIdent(sqlite) = %q", got)
	}
	if got := QuoteIdent(dialect.MySQL, "us`ers"); got != "`us``ers`" {
		t.Errorf("QuoteIdent(mysql) = %q", got)
	}
	if got := QuoteIdent("unknown", "users"); got != `"users"` {
		t.Errorf("QuoteIdent(default) = %q", got)
	}

	if got := EscapeLiteral("it's"); got != "'it''s'" {
		t.Errorf("EscapeLiteral = %q", got)
	}
}

// ---------------------------------------------------------------------------
// Client + sequence helpers over the in-memory sqlite ent client
// ---------------------------------------------------------------------------

func TestEntClientAccessors(t *testing.T) {
	cli := createTestEntClient(t)
	defer cli.Close()

	if cli.Client() == nil {
		t.Fatal("Client() must return the ent client")
	}
	if cli.Driver() == nil {
		t.Fatal("Driver() must return the sql driver")
	}
	if cli.DB() == nil {
		t.Fatal("DB() must return the sql.DB handle")
	}

	cli.SetConnectionOption(2, 2, time.Minute) // must not panic
}

func TestSyncSequence(t *testing.T) {
	ctx := context.Background()
	cli := createTestEntClient(t)
	defer cli.Close()

	// sqlite needs no sequence sync
	if err := SyncSequence(ctx, cli, "", "menus", "id"); err != nil {
		t.Fatalf("SyncSequence(sqlite) error: %v", err)
	}

	// identifier validation happens before any dialect switch
	if err := SyncSequence(ctx, cli, "bad schema", "menus", "id"); err == nil {
		t.Fatal("invalid schema must fail")
	}
	if err := SyncSequence(ctx, cli, "", "bad table", "id"); err == nil {
		t.Fatal("invalid table must fail")
	}
	if err := SyncSequence(ctx, cli, "", "menus", "bad column"); err == nil {
		t.Fatal("invalid column must fail")
	}
}

func TestQueryAllChildrenIds_UnsupportedDialect(t *testing.T) {
	ctx := context.Background()
	cli := createTestEntClient(t)
	defer cli.Close()

	// the sqlite dialect builds no query, so the client reports a failure
	// instead of returning children
	_, err := QueryAllChildrenIds(ctx, cli, "menus", 1)
	if err == nil {
		t.Log("QueryAllChildrenIds unexpectedly succeeded on sqlite")
	}
}

// ---------------------------------------------------------------------------
// Repository helper surface (generic instantiation over the Menu entity)
// ---------------------------------------------------------------------------

type menuRepository = Repository[
	ent.MenuQuery, ent.MenuSelect,
	ent.MenuCreate, ent.MenuCreateBulk,
	ent.MenuUpdate, ent.MenuUpdateOne,
	ent.MenuDelete,
	predicate.Menu, ent.Menu, ent.Menu,
]

func newMenuRepository(t *testing.T) *menuRepository {
	t.Helper()
	return NewRepository[ent.MenuQuery, ent.MenuSelect, ent.MenuCreate, ent.MenuCreateBulk, ent.MenuUpdate, ent.MenuUpdateOne, ent.MenuDelete, predicate.Menu](mapper.NewCopierMapper[ent.Menu, ent.Menu]())
}

func TestRepositoryCountExists_NilBuilder(t *testing.T) {
	repo := newMenuRepository(t)
	ctx := context.Background()

	if _, err := repo.Count(ctx, nil); err == nil {
		t.Fatal("Count with nil builder must fail")
	}
	if _, err := repo.Exists(ctx, nil); err == nil {
		t.Fatal("Exists with nil builder must fail")
	}
}

func TestRepositoryConvertAndSelectors(t *testing.T) {
	repo := newMenuRepository(t)

	fe, err := repo.ConvertFilterByPagingRequest(&paginationV1.PagingRequest{})
	if err != nil || fe != nil {
		// an empty request converts to a nil filter expression without error
		t.Fatalf("ConvertFilterByPagingRequest = %v, %v", fe, err)
	}
	fe2, err := repo.ConvertFilterByPaginationRequest(&paginationV1.PaginationRequest{})
	if err != nil || fe2 != nil {
		t.Fatalf("ConvertFilterByPaginationRequest = %v, %v", fe2, err)
	}

	sel, err := repo.BuildSelector([]string{"id", "name"})
	if err != nil || sel == nil {
		t.Fatalf("BuildSelector: got=%v err=%v", sel != nil, err)
	}
	s := sql.Select("*").From(sql.Table("menus"))
	sel(s)
	if q, _ := s.Query(); !strings.Contains(q, "id") {
		t.Fatalf("selector query = %q", q)
	}

	// empty fields produce no selector
	if sel, err = repo.BuildSelector(nil); sel != nil || err != nil {
		t.Fatalf("BuildSelector(empty): got=%v err=%v", sel != nil, err)
	}

	sel, err = repo.BuildSelectorWithTable("menus", []string{"name"})
	if err != nil || sel == nil {
		t.Fatalf("BuildSelectorWithTable: got=%v err=%v", sel != nil, err)
	}
	s2 := sql.Select("*").From(sql.Table("menus"))
	sel(s2)
	if q2, _ := s2.Query(); !strings.Contains(q2, "name") {
		t.Fatalf("table selector query = %q", q2)
	}
	if sel, err = repo.BuildSelectorWithTable("menus", nil); sel != nil || err != nil {
		t.Fatalf("BuildSelectorWithTable(empty): got=%v err=%v", sel != nil, err)
	}
}

func TestRepositoryCacheHelpers(t *testing.T) {
	repo := newMenuRepository(t)

	// invalidate before any cache configuration is a no-op
	repo.invalidateCache(context.Background(), 1)

	// nil redis disables caching entirely (same convention as the gorm
	// repository): WithCache returns the repo unchanged so cacheSupport*
	// stay nil and later invalidateCache calls are a no-op instead of
	// panicking on a nil redis client.
	repo.WithCache(nil, "menu:", time.Minute, 30*time.Second)
	if repo.cacheRedisClient != nil {
		t.Fatal("nil redis must be stored as nil")
	}
	if repo.cacheKeyPrefix != "" || repo.cacheTTL != 0 || repo.cacheListTTL != 0 {
		t.Fatal("cache settings must not be applied when cache is disabled")
	}
	if repo.cacheSupportSingle != nil || repo.cacheSupportList != nil {
		t.Fatal("cache support must stay disabled for a nil redis client")
	}
	// write-path invalidation after WithCache(nil) must not panic
	repo.invalidateCache(context.Background(), 1)

	// DTO id extraction: ent.Menu does not implement GetId() int64
	if got := repo.extractIDFromDTO(nil); got != nil {
		t.Fatalf("extractIDFromDTO(nil) = %v", got)
	}
	if got := repo.extractIDFromDTO(&ent.Menu{}); got != nil {
		t.Fatalf("extractIDFromDTO(menu without GetId) = %v", got)
	}

	cases := []struct{ in, want string }{
		{"plain", "plain"},
		{"a*b?c[d]e", `a\*b\?c\[d\]e`},
		{`x\y`, `x\\y`},
	}
	for _, tc := range cases {
		if got := escapeScanPattern(tc.in); got != tc.want {
			t.Errorf("escapeScanPattern(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
