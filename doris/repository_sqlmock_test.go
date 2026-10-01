package doris

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/tx7do/go-utils/mapper"
	"github.com/tx7do/go-wind/log"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/doris/query"
)

// newMockRepo builds a repository over a sqlmock-backed client so the SQL
// generation and execution paths run fully hermetically.
func newMockRepo(t *testing.T, table string) (*Repository[NoDeleted, NoDeleted], sqlmock.Sqlmock, *Client, func()) {
	t.Helper()
	client, mock, cleanup := newMockClient(t)
	repo := NewRepository[NoDeleted, NoDeleted](client, mapper.NewCopierMapper[NoDeleted, NoDeleted](), table, log.GetLogger())
	return repo, mock, client, cleanup
}

// boolPtr is a small helper for paging request pointer fields.
func boolPtr(v bool) *bool { return &v }

// countRows is a single-row result shaped like SELECT COUNT(1).
func countRows(v int64) *sqlmock.Rows {
	return sqlmock.NewRows([]string{"COUNT(1)"}).AddRow(v)
}

func TestRepositoryMock_Count(t *testing.T) {
	repo, mock, _, cleanup := newMockRepo(t, "nodes")
	defer cleanup()
	ctx := context.Background()

	// plain condition
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(1) FROM nodes WHERE id = ?")).
		WithArgs(1).
		WillReturnRows(countRows(5))
	cnt, err := repo.Count(ctx, "id = ?", 1)
	if err != nil || cnt != 5 {
		t.Fatalf("Count = %d, %v", cnt, err)
	}

	// WHERE-prefixed condition and single-slice-arg expansion with a
	// pre-written placeholder list.
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(1) FROM nodes WHERE id IN (?,?,?)")).
		WithArgs(1, 2, 3).
		WillReturnRows(countRows(3))
	cnt, err = repo.Count(ctx, "WHERE id IN (?,?,?)", []int{1, 2, 3})
	if err != nil || cnt != 3 {
		t.Fatalf("Count(slice) = %d, %v", cnt, err)
	}

	// single-slice-arg expansion also expands the IN (?) placeholder to
	// match the element count (regression: arg-count mismatch).
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(1) FROM nodes WHERE id IN (?,?,?)")).
		WithArgs(1, 2, 3).
		WillReturnRows(countRows(3))
	cnt, err = repo.Count(ctx, "id IN (?)", []int{1, 2, 3})
	if err != nil || cnt != 3 {
		t.Fatalf("Count(slice IN (?)) = %d, %v", cnt, err)
	}

	// empty slice expands to IN (NULL), matching gorm's empty-IN semantics
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(1) FROM nodes WHERE id IN (NULL)")).
		WillReturnRows(countRows(0))
	cnt, err = repo.Count(ctx, "id IN (?)", []int{})
	if err != nil || cnt != 0 {
		t.Fatalf("Count(empty slice) = %d, %v", cnt, err)
	}

	// empty where: full-table count
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(1) FROM nodes")).
		WillReturnRows(countRows(0))
	cnt, err = repo.Count(ctx, "", nil)
	if err != nil || cnt != 0 {
		t.Fatalf("Count(all) = %d, %v", cnt, err)
	}

	// query failure is wrapped
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(1) FROM nodes")).
		WillReturnError(errors.New("boom"))
	if _, err := repo.Count(ctx, "", nil); err == nil || err.Error() != "count query failed" {
		t.Fatalf("expected wrapped count error, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}

	// structural errors need no query
	if _, err := (&Repository[NoDeleted, NoDeleted]{}).Count(ctx, ""); err == nil {
		t.Fatal("nil client must fail")
	}
	if _, err := NewRepository[NoDeleted, NoDeleted](nil, nil, "", nil).Count(ctx, ""); err == nil {
		t.Fatal("empty table must fail")
	}
}

func TestRepositoryMock_Exists(t *testing.T) {
	repo, mock, _, cleanup := newMockRepo(t, "nodes")
	defer cleanup()
	ctx := context.Background()

	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1 FROM nodes WHERE id = ? LIMIT 1")).
		WithArgs(1).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	ok, err := repo.Exists(ctx, "id = ?", 1)
	if err != nil || !ok {
		t.Fatalf("Exists = %v, %v", ok, err)
	}

	// single-slice-arg expansion expands the IN (?) placeholder too
	// (regression: arg-count mismatch).
	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1 FROM nodes WHERE id IN (?,?,?) LIMIT 1")).
		WithArgs(1, 2, 3).
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	ok, err = repo.Exists(ctx, "id IN (?)", []int{1, 2, 3})
	if err != nil || !ok {
		t.Fatalf("Exists(slice IN (?)) = %v, %v", ok, err)
	}

	// sql.ErrNoRows maps to false
	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1 FROM nodes LIMIT 1")).
		WillReturnError(sql.ErrNoRows)
	ok, err = repo.Exists(ctx, "", nil)
	if err != nil || ok {
		t.Fatalf("Exists(no rows) = %v, %v", ok, err)
	}

	// other errors are wrapped
	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1 FROM nodes LIMIT 1")).
		WillReturnError(errors.New("boom"))
	if _, err := repo.Exists(ctx, "", nil); err == nil || err.Error() != "exists query failed" {
		t.Fatalf("expected wrapped exists error, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestRepositoryMock_GetAndOnly(t *testing.T) {
	repo, mock, client, cleanup := newMockRepo(t, "nodes")
	defer cleanup()
	ctx := context.Background()

	// happy path: SELECT * FROM nodes LIMIT 1
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM nodes LIMIT 1")).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(42))
	got, err := repo.Get(ctx, nil, nil)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if got == nil || got.ID != 42 {
		t.Fatalf("Get scanned %+v", got)
	}

	// Only is an alias
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM nodes LIMIT 1")).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(7))
	got, err = repo.Only(ctx, nil, nil)
	if err != nil || got == nil || got.ID != 7 {
		t.Fatalf("Only scanned %+v, %v", got, err)
	}

	// empty result set yields nil, nil
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM nodes LIMIT 1")).
		WillReturnRows(sqlmock.NewRows([]string{"id"}))
	got, err = repo.Get(ctx, nil, nil)
	if err != nil || got != nil {
		t.Fatalf("Get(empty) = %+v, %v", got, err)
	}

	// query failure is wrapped
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM nodes LIMIT 1")).
		WillReturnError(errors.New("boom"))
	if _, err := repo.Get(ctx, nil, nil); err == nil || err.Error() != "get query failed" {
		t.Fatalf("expected wrapped get error, got %v", err)
	}

	// view mask: the projection IS applied — the repository normalizes mask
	// paths (backtick-wrapping them) and the field selector keeps already
	// normalized paths unchanged (idempotent), so the masked column is selected.
	mask := &fieldmaskpb.FieldMask{Paths: []string{"id"}}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT `id` FROM nodes LIMIT 1")).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(9))
	got, err = repo.Get(ctx, nil, mask)
	if err != nil || got == nil || got.ID != 9 {
		t.Fatalf("Get(mask) = %+v, %v", got, err)
	}

	// multiple masked columns are all selected, in mask order (entity has
	// both columns so the scan succeeds)
	type idNameEntity struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
	}
	repo2 := NewRepository[idNameEntity, idNameEntity](client, mapper.NewCopierMapper[idNameEntity, idNameEntity](), "nodes", log.GetLogger())
	mask2 := &fieldmaskpb.FieldMask{Paths: []string{"id", "name"}}
	mock.ExpectQuery(regexp.QuoteMeta("SELECT `id`, `name` FROM nodes LIMIT 1")).
		WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(9, "nine"))
	got2, err := repo2.Get(ctx, nil, mask2)
	if err != nil || got2 == nil || got2.ID != 9 || got2.Name != "nine" {
		t.Fatalf("Get(mask2) = %+v, %v", got2, err)
	}

	// supplied query builder is reused (with its conditions)
	qb := query.NewQueryBuilder("nodes", nil)
	qb.Where("id = ?", 3)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM nodes WHERE id = ? LIMIT 1")).
		WithArgs(3).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(3))
	got, err = repo.Get(ctx, qb, nil)
	if err != nil || got == nil || got.ID != 3 {
		t.Fatalf("Get(qb) = %+v, %v", got, err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}

	// structural errors need no query
	if _, err := (&Repository[NoDeleted, NoDeleted]{}).Get(ctx, nil, nil); err == nil {
		t.Fatal("nil client must fail")
	}
	if _, err := NewRepository[NoDeleted, NoDeleted](nil, nil, "", nil).Get(ctx, nil, nil); err == nil {
		t.Fatal("empty table must fail")
	}
}

func TestRepositoryMock_CreateAndInsert(t *testing.T) {
	repo, mock, _, cleanup := newMockRepo(t, "nodes")
	defer cleanup()
	ctx := context.Background()

	// Create: single-column insert
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO nodes (id) VALUES (?)")).
		WithArgs(1).
		WillReturnResult(sqlmock.NewResult(1, 1))
	created, err := repo.Create(ctx, &NoDeleted{ID: 1}, nil)
	if err != nil {
		t.Fatalf("Create error: %v", err)
	}
	if created == nil || created.ID != 1 {
		t.Fatalf("Create returned %+v", created)
	}

	// Insert is an alias of Create
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO nodes (id) VALUES (?)")).
		WithArgs(2).
		WillReturnResult(sqlmock.NewResult(2, 1))
	if _, err := repo.Insert(ctx, &NoDeleted{ID: 2}, nil); err != nil {
		t.Fatalf("Insert error: %v", err)
	}

	// CreateX returns the affected row count
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO nodes (id) VALUES (?)")).
		WithArgs(3).
		WillReturnResult(sqlmock.NewResult(3, 1))
	rows, err := repo.CreateX(ctx, &NoDeleted{ID: 3}, nil)
	if err != nil || rows != 1 {
		t.Fatalf("CreateX = %d, %v", rows, err)
	}

	// CreateX honours the view mask when selecting columns
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO nodes (id) VALUES (?)")).
		WithArgs(4).
		WillReturnResult(sqlmock.NewResult(4, 1))
	if _, err := repo.CreateX(ctx, &NoDeleted{ID: 4}, &fieldmaskpb.FieldMask{Paths: []string{"id"}}); err != nil {
		t.Fatalf("CreateX(mask) error: %v", err)
	}

	// execution failure is wrapped
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO nodes (id) VALUES (?)")).
		WithArgs(9).
		WillReturnError(errors.New("boom"))
	if _, err := repo.Create(ctx, &NoDeleted{ID: 9}, nil); err == nil || err.Error() != "create failed" {
		t.Fatalf("expected wrapped create error, got %v", err)
	}

	// nil dto errors
	if _, err := repo.Create(ctx, nil, nil); err == nil {
		t.Fatal("nil dto must fail")
	}
	if _, err := repo.CreateX(ctx, nil, nil); err == nil {
		t.Fatal("nil dto must fail")
	}

	// structural errors
	if _, err := (&Repository[NoDeleted, NoDeleted]{}).Create(ctx, &NoDeleted{}, nil); err == nil {
		t.Fatal("nil client must fail")
	}
	if _, err := NewRepository[NoDeleted, NoDeleted](nil, nil, "", nil).Create(ctx, &NoDeleted{}, nil); err == nil {
		t.Fatal("empty table must fail")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestRepositoryMock_BatchCreate(t *testing.T) {
	repo, mock, _, cleanup := newMockRepo(t, "nodes")
	defer cleanup()
	ctx := context.Background()

	// empty input short-circuits
	res, err := repo.BatchCreate(ctx, nil, nil)
	if err != nil || res != nil {
		t.Fatalf("BatchCreate(nil) = %v, %v", res, err)
	}

	// batch insert over extracted columns (one tuple per row)
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO nodes (id) VALUES (?),(?)")).
		WithArgs(1, 2).
		WillReturnResult(sqlmock.NewResult(0, 2))
	res, err = repo.BatchCreate(ctx, []*NoDeleted{{ID: 1}, {ID: 2}}, nil)
	if err != nil {
		t.Fatalf("BatchCreate error: %v", err)
	}
	if len(res) != 2 {
		t.Fatalf("expected 2 results, got %d", len(res))
	}

	// nil entries are skipped
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO nodes (id) VALUES (?)")).
		WithArgs(3).
		WillReturnResult(sqlmock.NewResult(0, 1))
	res, err = repo.BatchCreate(ctx, []*NoDeleted{nil, {ID: 3}}, nil)
	if err != nil || len(res) != 1 {
		t.Fatalf("BatchCreate(with nil entries) = %d items, %v", len(res), err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestRepositoryMock_DeleteHard(t *testing.T) {
	repo, mock, _, cleanup := newMockRepo(t, "nodes")
	defer cleanup()
	ctx := context.Background()

	// hard delete without conditions
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM nodes")).
		WillReturnResult(sqlmock.NewResult(0, 3))
	rows, err := repo.Delete(ctx, nil, true)
	if err != nil || rows != 3 {
		t.Fatalf("Delete(all) = %d, %v", rows, err)
	}

	// hard delete with a query builder condition
	qb := query.NewQueryBuilder("nodes", nil)
	qb.Where("id = ?", 5)
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM nodes WHERE id = ?")).
		WithArgs(5).
		WillReturnResult(sqlmock.NewResult(0, 1))
	rows, err = repo.Delete(ctx, qb, true)
	if err != nil || rows != 1 {
		t.Fatalf("Delete(where) = %d, %v", rows, err)
	}

	// execution failure is wrapped
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM nodes")).
		WillReturnError(errors.New("boom"))
	if _, err := repo.Delete(ctx, nil, true); err == nil || err.Error() != "delete failed" {
		t.Fatalf("expected wrapped delete error, got %v", err)
	}

	// soft delete is unsupported for entities without deleted_at
	if _, err := repo.SoftDelete(ctx, nil); err == nil {
		t.Fatal("SoftDelete without deleted_at must fail")
	}

	// structural errors
	if _, err := (&Repository[NoDeleted, NoDeleted]{}).Delete(ctx, nil, true); err == nil {
		t.Fatal("nil client must fail")
	}
	if _, err := NewRepository[NoDeleted, NoDeleted](nil, nil, "", nil).Delete(ctx, nil, true); err == nil {
		t.Fatal("empty table must fail")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestRepositoryMock_ListWithPaging(t *testing.T) {
	repo, mock, _, cleanup := newMockRepo(t, "nodes")
	defer cleanup()
	ctx := context.Background()

	// simple empty request: count query first, then the list query
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(1) FROM nodes")).
		WillReturnRows(countRows(2))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM nodes")).
		WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1).AddRow(2))

	res, err := repo.ListWithPaging(ctx, &paginationV1.PagingRequest{})
	if err != nil {
		t.Fatalf("ListWithPaging error: %v", err)
	}
	if res == nil || res.Total != 2 || len(res.Items) != 2 {
		t.Fatalf("ListWithPaging = %+v", res)
	}

	// nil request errors instead of panicking (same convention as the gorm
	// repository); nil client / empty table still error too.
	if _, err := repo.ListWithPaging(ctx, nil); err == nil || err.Error() != "paging request is nil" {
		t.Fatalf("ListWithPaging(nil req) = %v", err)
	}
	if _, err := (&Repository[NoDeleted, NoDeleted]{}).ListWithPaging(ctx, &paginationV1.PagingRequest{}); err == nil {
		t.Fatal("nil client must fail")
	}
	if _, err := NewRepository[NoDeleted, NoDeleted](nil, nil, "", nil).ListWithPaging(ctx, &paginationV1.PagingRequest{}); err == nil {
		t.Fatal("empty table must fail")
	}

	// ListWithPagination rejects a nil request the same way.
	if _, err := repo.ListWithPagination(ctx, nil); err == nil || err.Error() != "pagination request is nil" {
		t.Fatalf("ListWithPagination(nil req) = %v", err)
	}

	// list query failure is wrapped; the count still runs first (no_paging
	// only adds the server-side max-limit guard to the list query)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(1) FROM nodes")).
		WillReturnRows(countRows(2))
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM nodes")).
		WillReturnError(errors.New("boom"))
	np := true
	if _, err := repo.ListWithPaging(ctx, &paginationV1.PagingRequest{NoPaging: &np}); err == nil {
		t.Fatal("expected wrapped list error")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
