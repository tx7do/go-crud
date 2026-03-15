package doris

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/log"

	// register mysql driver for doris (doris uses MySQL protocol)
	_ "github.com/go-sql-driver/mysql"

	"github.com/jmoiron/sqlx"
)

type Client struct {
	log *log.Helper
	// sqlx DB
	db  *sqlx.DB
	dsn string

	maxOpenConns    int
	maxIdleConns    int
	connMaxLifetime time.Duration

	// Stream Load related
	streamLoadEndpoint string
	streamLoadUser     string
	streamLoadPass     string
	streamLoadMethod   string // POST or PUT
	streamLoadTimeout  time.Duration
	httpClient         *http.Client
}

func NewClient(opts ...Option) (*Client, error) {
	c := &Client{}
	// apply options
	for _, o := range opts {
		o(c)
	}

	// ensure logger
	if c.log == nil {
		c.log = log.NewHelper(log.DefaultLogger)
	}

	// if db not injected, open using MySQL driver (Doris supports MySQL protocol)
	if c.db == nil {
		if c.dsn == "" {
			return nil, fmt.Errorf("dsn must be provided")
		}
		db, err := sqlx.Open("mysql", c.dsn)
		if err != nil {
			return nil, err
		}
		// apply connection settings
		if c.maxOpenConns > 0 {
			db.SetMaxOpenConns(c.maxOpenConns)
		}
		if c.maxIdleConns > 0 {
			db.SetMaxIdleConns(c.maxIdleConns)
		}
		if c.connMaxLifetime > 0 {
			db.SetConnMaxLifetime(c.connMaxLifetime)
		}
		// ping
		if err = db.Ping(); err != nil {
			return nil, err
		}
		c.db = db
	}
	// default http client and stream load method
	if c.httpClient == nil {
		c.httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	if c.streamLoadMethod == "" {
		c.streamLoadMethod = "PUT"
	}
	if c.streamLoadTimeout > 0 {
		c.httpClient.Timeout = c.streamLoadTimeout
	}

	return c, nil
}

// Close closes underlying DB if present
func (c *Client) Close() error {
	if c.db == nil {
		return nil
	}
	return c.db.Close()
}

// Exec executes a query
func (c *Client) Exec(query string, args ...any) (sql.Result, error) {
	return c.db.Exec(query, args...)
}

// Get fetches one row into dest
func (c *Client) Get(dest any, query string, args ...any) error {
	return c.db.Get(dest, query, args...)
}

// GetContext fetches one row into dest with context
func (c *Client) GetContext(ctx context.Context, dest any, query string, args ...any) error {
	return c.db.GetContext(ctx, dest, query, args...)
}

// Select fetches multiple rows into dest
func (c *Client) Select(dest any, query string, args ...any) error {
	return c.db.Select(dest, query, args...)
}

// SelectContext fetches multiple rows into dest with context
func (c *Client) SelectContext(ctx context.Context, dest any, query string, args ...any) error {
	return c.db.SelectContext(ctx, dest, query, args...)
}

// ExecContext executes a query with context
func (c *Client) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return c.db.ExecContext(ctx, query, args...)
}

// BatchInsert inserts multiple rows into table. Each row must match columns length.
func (c *Client) BatchInsert(ctx context.Context, table string, columns []string, rows [][]any) (sql.Result, error) {
	if len(rows) == 0 {
		return nil, fmt.Errorf("no rows to insert")
	}
	rowsCount := len(rows)
	sqlStr, err := BuildInsertSQL(table, columns, rowsCount)
	if err != nil {
		return nil, err
	}

	args := make([]any, 0, rowsCount*len(columns))
	for _, r := range rows {
		if len(r) != len(columns) {
			return nil, fmt.Errorf("row column count mismatch: expected %d got %d", len(columns), len(r))
		}
		args = append(args, r...)
	}

	return c.db.ExecContext(ctx, sqlStr, args...)
}

// SetSession executes a session-level SET statement. Note: when executed on the DB pool,
// the statement runs on a single connection from the pool; subsequent queries may use
// different connections and therefore may not see the session variable. For a guaranteed
// per-connection session, use WithSessionConn.
func (c *Client) SetSession(ctx context.Context, stmt string, args ...any) error {
	_, err := c.db.ExecContext(ctx, stmt, args...)
	return err
}

// SetSQLMode sets the session sql_mode. Warning: this affects only the connection that
// executes the statement. Use WithSessionConn to ensure subsequent operations run on
// the same physical connection.
func (c *Client) SetSQLMode(ctx context.Context, mode string) error {
	_, err := c.db.ExecContext(ctx, "SET SESSION sql_mode = ?", mode)
	return err
}

// WithSessionConn acquires a dedicated underlying *sql.Conn, executes the provided
// session statements (e.g. SET SESSION ...) on that connection, then runs fn using
// the same connection. This guarantees the session variables apply for the duration
// of fn. The provided fn receives the *sql.Conn and should perform operations using
// database/sql APIs (QueryContext/ExecContext) on that conn. The conn is closed after fn returns.
func (c *Client) WithSessionConn(ctx context.Context, sessionStmts []string, fn func(ctx context.Context, conn *sql.Conn) error) error {
	if c.db == nil {
		return fmt.Errorf("db not initialized")
	}

	sqlDB := c.db.DB
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Apply session statements
	for _, s := range sessionStmts {
		if strings.TrimSpace(s) == "" {
			continue
		}
		if _, err = conn.ExecContext(ctx, s); err != nil {
			return err
		}
	}

	// Run user supplied function on the same connection
	return fn(ctx, conn)
}

// SetSessionVars sets multiple session variables on the server. Each value is formatted
// safely: numeric values (with optional unit K/M/G) are used as-is; others are quoted.
// Note: when executed via the connection pool, SET statements may apply to different
// connections; use RunWithSession to guarantee that subsequent operations run on the
// same connection.
func (c *Client) SetSessionVars(ctx context.Context, vars map[string]string) error {
	if len(vars) == 0 {
		return nil
	}
	// deterministic order
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := vars[k]
		if !isSafeIdent(k) {
			return fmt.Errorf("invalid session variable name: %s", k)
		}
		stmt := fmt.Sprintf("SET SESSION %s = %s", k, formatSessionValue(v))
		if _, err := c.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// RunWithSession sets given session variables on a single connection and runs fn on that connection.
// This guarantees the session variables are visible to operations inside fn.
func (c *Client) RunWithSession(ctx context.Context, vars map[string]string, fn func(ctx context.Context, conn *sql.Conn) error) error {
	// build session statements
	stmts := make([]string, 0, len(vars))
	for k, v := range vars {
		if !isSafeIdent(k) {
			return fmt.Errorf("invalid session variable name: %s", k)
		}
		stmts = append(stmts, fmt.Sprintf("SET SESSION %s = %s", k, formatSessionValue(v)))
	}
	return c.WithSessionConn(ctx, stmts, fn)
}

// BeginTx starts a new transaction with given options and returns a *sqlx.Tx.
// Caller should Commit or Rollback the returned transaction.
func (c *Client) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error) {
	if c.db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	return c.db.BeginTxx(ctx, opts)
}

// WithTx runs fn inside a transaction. It will commit if fn returns nil,
// otherwise rollback. It also rollbacks on panic and re-panics after rollback.
func (c *Client) WithTx(ctx context.Context, opts *sql.TxOptions, fn func(tx *sqlx.Tx) error) error {
	tx, err := c.BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

	if err = fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			// return original error but log rollback error if needed
			return fmt.Errorf("tx error: %v; rollback error: %v", err, rbErr)
		}
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}
	return nil
}

// RunInTx is an alias for WithTx
func (c *Client) RunInTx(ctx context.Context, opts *sql.TxOptions, fn func(tx *sqlx.Tx) error) error {
	return c.WithTx(ctx, opts, fn)
}

// BeginTxWithSession sets given session variables on a dedicated connection and begins a transaction
// on that same connection, returning a TxWithConn which must be Commit()ed or Rollback()ed.
func (c *Client) BeginTxWithSession(ctx context.Context, vars map[string]string, opts *sql.TxOptions) (*TxWithConn, error) {
	if c.db == nil {
		return nil, fmt.Errorf("db not initialized")
	}
	sqlDB := c.db.DB
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return nil, err
	}

	// apply session vars on this connection
	for k, v := range vars {
		if !isSafeIdent(k) {
			_ = conn.Close()
			return nil, fmt.Errorf("invalid session variable name: %s", k)
		}
		stmt := fmt.Sprintf("SET SESSION %s = %s", k, formatSessionValue(v))
		if _, err = conn.ExecContext(ctx, stmt); err != nil {
			_ = conn.Close()
			return nil, err
		}
	}

	// begin tx on this connection
	sqlTx, err := conn.BeginTx(ctx, opts)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	return &TxWithConn{Tx: sqlTx, conn: conn}, nil
}

// WithTxWithSession is a convenience wrapper to run a function inside a transaction
// that has session variables applied on the same underlying connection.
func (c *Client) WithTxWithSession(ctx context.Context, vars map[string]string, opts *sql.TxOptions, fn func(tx *sql.Tx) error) error {
	txwc, err := c.BeginTxWithSession(ctx, vars, opts)
	if err != nil {
		return err
	}
	// ensure rollback+close on panic
	defer func() {
		if r := recover(); r != nil {
			_ = txwc.Rollback()
			panic(r)
		}
	}()

	if err = fn(txwc.Tx); err != nil {
		if rbErr := txwc.Rollback(); rbErr != nil {
			return fmt.Errorf("fn error: %v; rollback error: %v", err, rbErr)
		}
		return err
	}

	return txwc.Commit()
}
