package doris

import (
	"context"
	"database/sql"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

func newMockClient(t *testing.T) (*Client, sqlmock.Sqlmock, func()) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	sx := sqlx.NewDb(db, "mysql")
	c, err := NewClient(WithDB(sx))
	if err != nil {
		db.Close()
		t.Fatalf("NewClient failed: %v", err)
	}
	return c, mock, func() { _ = db.Close() }
}

func TestSetSessionVars(t *testing.T) {
	cli, mock, closeFn := newMockClient(t)
	defer closeFn()

	ctx := context.Background()
	vars := map[string]string{
		"enable_profile":   "true",
		"sql_select_limit": "10000",
		"exec_mem_limit":   "4G",
		"time_zone":        "Asia/Shanghai",
	}

	// expect execs for each SET in sorted order of keys
	mock.ExpectExec(regexp.QuoteMeta("SET SESSION enable_profile = true")).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("SET SESSION exec_mem_limit = 4G")).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("SET SESSION sql_select_limit = 10000")).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(regexp.QuoteMeta("SET SESSION time_zone = 'Asia/Shanghai'")).WillReturnResult(sqlmock.NewResult(1, 1))

	if err := cli.SetSessionVars(ctx, vars); err != nil {
		t.Fatalf("SetSessionVars failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestBeginTxWithSessionAndCommit(t *testing.T) {
	cli, mock, closeFn := newMockClient(t)
	defer closeFn()

	ctx := context.Background()
	vars := map[string]string{"time_zone": "Asia/Shanghai"}

	// Expect the SET on conn, then begin and commit
	mock.ExpectExec(regexp.QuoteMeta("SET SESSION time_zone = 'Asia/Shanghai'")).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectBegin()
	mock.ExpectCommit()

	txwc, err := cli.BeginTxWithSession(ctx, vars, nil)
	if err != nil {
		t.Fatalf("BeginTxWithSession failed: %v", err)
	}

	if err := txwc.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestWithTxWithSession(t *testing.T) {
	cli, mock, closeFn := newMockClient(t)
	defer closeFn()

	ctx := context.Background()
	vars := map[string]string{"enable_profile": "true"}

	mock.ExpectExec(regexp.QuoteMeta("SET SESSION enable_profile = true")).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectBegin()
	// expect an insert inside transaction
	mock.ExpectExec(regexp.QuoteMeta("INSERT INTO test (id) VALUES (?)")).WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := cli.WithTxWithSession(ctx, vars, nil, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "INSERT INTO test (id) VALUES (?)", 1)
		return err
	})
	if err != nil {
		t.Fatalf("WithTxWithSession failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestBatchInsert(t *testing.T) {
	cli, mock, closeFn := newMockClient(t)
	defer closeFn()

	ctx := context.Background()
	cols := []string{"id", "name"}
	rows := [][]any{{1, "a"}, {2, "b"}}
	// Build expected SQL
	sqlStr, _ := BuildInsertSQL("posts", cols, len(rows))
	mock.ExpectExec(regexp.QuoteMeta(sqlStr)).WithArgs(1, "a", 2, "b").WillReturnResult(sqlmock.NewResult(1, 2))

	if _, err := cli.BatchInsert(ctx, "posts", cols, rows); err != nil {
		t.Fatalf("BatchInsert failed: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
