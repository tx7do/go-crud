package doris

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
)

type Candle struct {
	Timestamp *time.Time `json:"timestamp" db:"timestamp"`
	Symbol    *string    `json:"symbol" db:"symbol"`
	Open      *float64   `json:"open" db:"open"`
	High      *float64   `json:"high" db:"high"`
	Low       *float64   `json:"low" db:"low"`
	Close     *float64   `json:"close" db:"close"`
	Volume    *float64   `json:"volume" db:"volume"`
}

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

func newDorisTestClient() *Client {
	cli, err := NewClient(
		WithDSN("root:@tcp(localhost:9030)/finances?charset=utf8mb4&parseTime=True&loc=Local"),
	)
	if err != nil {
		return nil
	}

	return cli
}

func createCandlesTable(client *Client) {
	ctx := context.Background()
	_, err := client.DB().ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS finances.candles (
    timestamp DATETIME NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,

    INDEX idx_symbol (symbol) USING INVERTED
)
ENGINE = OLAP
UNIQUE KEY(timestamp, symbol)
PARTITION BY RANGE(timestamp) () 
DISTRIBUTED BY HASH(symbol) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "1",
    "dynamic_partition.prefix" = "p",
    "enable_unique_key_merge_on_write" = "true"
);
	`)
	if err != nil {
		panic(err)
	}
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

	if err = txwc.Commit(); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
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

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

// flattenArgs 将 []interface{} 或 [][]interface{} 里的指针全部解引用为基础类型或 nil，返回 []driver.Value
func flattenArgs(args ...interface{}) []driver.Value {
	var flat []driver.Value
	for _, v := range args {
		switch vv := v.(type) {
		case *string:
			if vv == nil {
				flat = append(flat, nil)
			} else {
				flat = append(flat, *vv)
			}
		case *float64:
			if vv == nil {
				flat = append(flat, nil)
			} else {
				flat = append(flat, *vv)
			}
		case *int:
			if vv == nil {
				flat = append(flat, nil)
			} else {
				flat = append(flat, *vv)
			}
		case *time.Time:
			if vv == nil {
				flat = append(flat, nil)
			} else {
				flat = append(flat, *vv)
			}
		default:
			flat = append(flat, v)
		}
	}
	return flat
}

func TestBatchInsert(t *testing.T) {
	cli, mock, closeFn := newMockClient(t)
	defer closeFn()

	ctx := context.Background()
	// 构造 Candle 测试数据
	now := time.Now()
	symbol1 := "BTCUSDT"
	symbol2 := "ETHUSDT"
	open1, high1, low1, close1, volume1 := 100.0, 110.0, 90.0, 105.0, 1000.0
	open2, high2, low2, close2, volume2 := 200.0, 210.0, 190.0, 205.0, 2000.0
	candles := []Candle{
		{
			Timestamp: &now,
			Symbol:    &symbol1,
			Open:      &open1,
			High:      &high1,
			Low:       &low1,
			Close:     &close1,
			Volume:    &volume1,
		},
		{
			Timestamp: &now,
			Symbol:    &symbol2,
			Open:      &open2,
			High:      &high2,
			Low:       &low2,
			Close:     &close2,
			Volume:    &volume2,
		},
	}
	cols := []string{"timestamp", "symbol", "open", "high", "low", "close", "volume"}
	rows := [][]interface{}{
		{candles[0].Timestamp, candles[0].Symbol, candles[0].Open, candles[0].High, candles[0].Low, candles[0].Close, candles[0].Volume},
		{candles[1].Timestamp, candles[1].Symbol, candles[1].Open, candles[1].High, candles[1].Low, candles[1].Close, candles[1].Volume},
	}
	sqlStr, err := BuildInsertSQL("finances.candles", cols, len(rows))
	if err != nil {
		t.Fatalf("BuildInsertSQL failed: %v", err)
	}
	args := flattenArgs(append(rows[0], rows[1]...)...)
	mock.ExpectExec(regexp.QuoteMeta(sqlStr)).WithArgs(args...).WillReturnResult(sqlmock.NewResult(1, 2))

	if _, err = cli.BatchInsert(ctx, "finances.candles", cols, rows); err != nil {
		t.Fatalf("BatchInsert failed: %v", err)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestClientQuery(t *testing.T) {
	cli, mock, closeFn := newMockClient(t)
	defer closeFn()

	ctx := context.Background()
	rows := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(1, "foo").
		AddRow(2, "bar")

	sqlStr := "SELECT id, name FROM test WHERE id > ?"
	mock.ExpectQuery(regexp.QuoteMeta(sqlStr)).
		WithArgs(0).
		WillReturnRows(rows)
	type result struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
	}
	var results []any
	err := cli.Query(ctx, func() any { return &result{} }, &results, sqlStr, 0)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("unexpected result count: %d", len(results))
	}
	r0 := results[0].(*result)
	r1 := results[1].(*result)
	if r0.ID != 1 || r0.Name != "foo" || r1.ID != 2 || r1.Name != "bar" {
		t.Fatalf("unexpected query result: %+v, %+v", r0, r1)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestClientInsert(t *testing.T) {
	cli, mock, closeFn := newMockClient(t)
	defer closeFn()

	type testEntity struct {
		ID   int    `db:"id"`
		Name string `db:"name"`
	}
	entity := testEntity{ID: 1, Name: "foo"}
	ctx := context.Background()
	_, vals, err := ExtractColumnsAndValues(entity)
	if err != nil {
		t.Fatalf("ExtractColumnsAndValues failed: %v", err)
	}
	// Insert 方法实现没有加双引号，直接拼接
	sqlStr := "INSERT INTO test (id, name) VALUES (?, ?)"
	args := flattenArgs(vals...)
	mock.ExpectExec(regexp.QuoteMeta(sqlStr)).WithArgs(args...).WillReturnResult(sqlmock.NewResult(1, 1))

	err = cli.Insert(ctx, "test", entity)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	if err = mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
