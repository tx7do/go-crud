package entgo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"entgo.io/ent/dialect"
	"github.com/go-kratos/kratos/v2/log"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"

	"github.com/XSAM/otelsql"

	entSql "entgo.io/ent/dialect/sql"
)

type EntClientInterface interface {
	Close() error
}

type EntClient[T EntClientInterface] struct {
	db  T
	drv *entSql.Driver
}

func NewEntClient[T EntClientInterface](db T, drv *entSql.Driver) *EntClient[T] {
	return &EntClient[T]{
		db:  db,
		drv: drv,
	}
}

func (c *EntClient[T]) Client() T {
	return c.db
}

func (c *EntClient[T]) Driver() *entSql.Driver {
	return c.drv
}

func (c *EntClient[T]) DB() *sql.DB {
	return c.drv.DB()
}

// Close 关闭数据库连接
func (c *EntClient[T]) Close() error {
	return c.db.Close()
}

// Query 查询数据
func (c *EntClient[T]) Query(ctx context.Context, query string, args, v any) error {
	return c.Driver().Query(ctx, query, args, v)
}

func (c *EntClient[T]) Exec(ctx context.Context, query string, args, v any) error {
	return c.Driver().Exec(ctx, query, args, v)
}

// SetConnectionOption 设置连接配置
func (c *EntClient[T]) SetConnectionOption(maxIdleConnections, maxOpenConnections int, connMaxLifetime time.Duration) {
	// 连接池中最多保留的空闲连接数量
	c.DB().SetMaxIdleConns(maxIdleConnections)
	// 连接池在同一时间打开连接的最大数量
	c.DB().SetMaxOpenConns(maxOpenConnections)
	// 连接可重用的最大时间长度
	c.DB().SetConnMaxLifetime(connMaxLifetime)
}

func driverNameToSemConvKeyValue(driverName string) attribute.KeyValue {
	switch driverName {
	case "mariadb":
		return semconv.DBSystemMariaDB
	case "mysql":
		return semconv.DBSystemMySQL
	case "postgresql":
		return semconv.DBSystemPostgreSQL
	case "sqlite":
		return semconv.DBSystemSqlite
	default:
		return semconv.DBSystemKey.String(driverName)
	}
}

// CreateDriver 创建数据库驱动
func CreateDriver(driverName, dsn string, enableTrace, enableMetrics bool) (*entSql.Driver, error) {
	var db *sql.DB
	var drv *entSql.Driver
	var err error

	if enableTrace {
		// Connect to database with otel tracing
		if db, err = otelsql.Open(driverName, dsn, otelsql.WithAttributes(
			driverNameToSemConvKeyValue(driverName),
		)); err != nil {
			return nil, errors.New(fmt.Sprintf("failed opening connection to db: %v", err))
		}

		drv = entSql.OpenDB(driverName, db)
	} else {
		// Connect to database without otel tracing
		if drv, err = entSql.Open(driverName, dsn); err != nil {
			return nil, errors.New(fmt.Sprintf("failed opening connection to db: %v", err))
		}

		db = drv.DB()
	}

	// Register DB stats to meter
	if enableMetrics {
		_, err = otelsql.RegisterDBStatsMetrics(db, otelsql.WithAttributes(
			driverNameToSemConvKeyValue(driverName),
		))
		if err != nil {
			return nil, errors.New(fmt.Sprintf("failed register otel meter: %v", err))
		}
	}

	return drv, nil
}

type Rollbacker interface {
	Rollback() error
}

// Rollback calls to tx.Rollback and wraps the given error
func Rollback[T Rollbacker](tx T, err error) error {
	if rErr := tx.Rollback(); rErr != nil {
		if err == nil {
			err = rErr
		} else {
			err = fmt.Errorf("%w: rollback failed: %v", err, rErr)
		}
	}
	return err
}

// QueryAllChildrenIds 使用CTE递归查询所有子节点ID
func QueryAllChildrenIds[T EntClientInterface](ctx context.Context, entClient *EntClient[T], tableName string, parentID uint32) ([]uint32, error) {
	var query string
	switch entClient.Driver().Dialect() {
	case dialect.MySQL:
		query = fmt.Sprintf(`
			WITH RECURSIVE all_descendants AS (
				SELECT 
					id,
					parent_id,
					name,
					1 AS depth
				FROM %s
				WHERE parent_id = ?
				
				UNION ALL
				
				SELECT 
					p.id,
					p.parent_id,
					p.name,
					ad.depth + 1 AS depth
				FROM %s p
					INNER JOIN all_descendants ad
				ON p.parent_id = ad.id
			)
			SELECT id FROM all_descendants;
		`, tableName, tableName)

	case dialect.Postgres:
		query = fmt.Sprintf(`
        WITH RECURSIVE all_descendants AS (
            SELECT *
			FROM %s
			WHERE parent_id = $1
            UNION ALL
            SELECT p.*
			FROM %s p
            	INNER JOIN all_descendants ad
			ON p.parent_id = ad.id
        )
        SELECT id FROM all_descendants;
    `, tableName, tableName)
	}

	rows := &sql.Rows{}
	if err := entClient.Query(ctx, query, []any{parentID}, rows); err != nil {
		log.Errorf("query child nodes failed: %s", err.Error())
		return nil, errors.New("query child nodes failed: " + err.Error())
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			log.Errorf("close rows failed: %s", err.Error())
		}
	}(rows)

	childIDs := make([]uint32, 0)
	for rows.Next() {
		var id uint32

		if err := rows.Scan(&id); err != nil {
			log.Errorf("scan child node failed: %s", err.Error())
			return nil, errors.New("scan child node failed")
		}

		childIDs = append(childIDs, id)
	}

	return childIDs, nil
}

// SyncSequence 同步数据库序列
func SyncSequence[T EntClientInterface](ctx context.Context, entClient *EntClient[T], schema, table, column string) error {
	// 校验输入
	if err := ValidateSchemaTableColumn(schema, table, column); err != nil {
		return fmt.Errorf("invalid identifier: %w", err)
	}

	dial := entClient.Driver().Dialect()
	switch dial {
	case dialect.MySQL, dialect.SQLite:
		// MySQL 和 SQLite 不需要同步序列
		return nil
	case dialect.Postgres:
		// 继续执行
	default:
		// 非预期方言，直接返回 nil（或根据需求改为返回错误）
		return nil
	}

	// 构造引用后的表名（用于 FROM / MAX）
	quotedTable := QuoteIdent(dial, table)
	if schema != "" {
		quotedTable = QuoteIdent(dial, schema) + "." + quotedTable
	}
	// 构造引用后的列名
	quotedColumn := QuoteIdent(dial, column)

	// 构造作为字面量传入 pg_get_serial_sequence 的表名（必须是字符串字面量）
	fullTableForLiteral := table
	if schema != "" {
		fullTableForLiteral = schema + "." + table
	}
	literalTable := EscapeLiteral(fullTableForLiteral)

	query := fmt.Sprintf(
		`SELECT setval(pg_get_serial_sequence(%s, %s),
   COALESCE((SELECT MAX(%s) FROM %s), 1), true);`,
		literalTable, EscapeLiteral(column), quotedColumn, quotedTable,
	)

	if _, err := entClient.DB().ExecContext(ctx, query); err != nil {
		return fmt.Errorf("sync sequence failed: %w", err)
	}

	return nil
}

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// IsValidIdent 校验单个标识符（表名、列名、schema）
func IsValidIdent(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	return identRe.MatchString(s)
}

// ValidateSchemaTableColumn 校验 schema、表名和列名是否合法
func ValidateSchemaTableColumn(schema, table, column string) error {
	if table == "" || column == "" {
		return fmt.Errorf("table and column are required")
	}
	if schema != "" && !IsValidIdent(schema) {
		return fmt.Errorf("invalid schema: %s", schema)
	}
	if !IsValidIdent(table) {
		return fmt.Errorf("invalid table: %s", table)
	}
	if !IsValidIdent(column) {
		return fmt.Errorf("invalid column: %s", column)
	}
	return nil
}

// QuoteIdent 根据方言引用标识符，避免注入（并对内部的引用字符进行重复转义）
func QuoteIdent(d string, s string) string {
	switch d {
	case dialect.Postgres, dialect.SQLite:
		// 双引号并重复双引号内的双引号
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	case dialect.MySQL:
		// 反引号并重复反引号内的反引号
		return "`" + strings.ReplaceAll(s, "`", "``") + "`"
	default:
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
}

// EscapeLiteral 对作为 SQL 字面量传入的字符串做单引号转义（Postgres 用于 pg_get_serial_sequence 的第一个参数）
func EscapeLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
