package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	clickhouseV2 "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-kratos/kratos/v2/log"
)

type Client struct {
	conn clickhouseV2.Conn
	db   *sql.DB

	options *clickhouseV2.Options

	logger *log.Helper
}

func NewClient(opts ...Option) (*Client, error) {
	c := &Client{
		options: &clickhouseV2.Options{},
	}

	for _, o := range opts {
		o(c)
	}

	if c.logger == nil {
		c.logger = log.NewHelper(log.With(log.DefaultLogger, "module", "clickhouse-client"))
	}

	if err := c.createClickHouseClient(c.options); err != nil {
		return nil, err
	}

	return c, nil
}

// createClickHouseClient 创建ClickHouse客户端
func (c *Client) createClickHouseClient(opts *clickhouseV2.Options) error {
	conn, err := clickhouseV2.Open(opts)
	if err != nil {
		c.logger.Errorf("failed to create clickhouse client: %v", err)
		return ErrConnectionFailed
	}

	c.conn = conn

	return nil
}

// Close 关闭ClickHouse客户端连接
func (c *Client) Close() {
	if c.conn == nil {
		c.logger.Warn("clickhouse client is already closed or not initialized")
		return
	}

	if err := c.conn.Close(); err != nil {
		c.logger.Errorf("failed to close clickhouse client: %v", err)
	} else {
		c.logger.Info("clickhouse client closed successfully")
	}
}

// GetServerVersion 获取ClickHouse服务器版本
func (c *Client) GetServerVersion() string {
	if c.conn == nil {
		c.logger.Error("clickhouse client is not initialized")
		return ""
	}

	version, err := c.conn.ServerVersion()
	if err != nil {
		c.logger.Errorf("failed to get server version: %v", err)
		return ""
	}

	c.logger.Infof("ClickHouse server version: %s", version)
	return version.String()
}

// CheckConnection 检查ClickHouse客户端连接是否正常
func (c *Client) CheckConnection(ctx context.Context) error {
	if c.conn == nil {
		c.logger.Error("clickhouse client is not initialized")
		return ErrClientNotInitialized
	}

	if err := c.conn.Ping(ctx); err != nil {
		c.logger.Errorf("ping failed: %v", err)
		return ErrPingFailed
	}

	c.logger.Info("clickhouse client connection is healthy")
	return nil
}

// Query 执行查询并返回结果
func (c *Client) Query(ctx context.Context, creator Creator, results *[]any, query string, args ...any) error {
	if c.conn == nil {
		c.logger.Error("clickhouse client is not initialized")
		return ErrClientNotInitialized
	}
	if creator == nil {
		c.logger.Error("creator function cannot be nil")
		return ErrCreatorFunctionNil
	}
	if results == nil {
		c.logger.Error("results must be a non-nil pointer to a slice")
		return ErrInvalidArgument
	}

	// 当只传入一个参数且该参数是切片时，将其展开为单独的参数
	if len(args) == 1 {
		v := reflect.ValueOf(args[0])
		if v.IsValid() && v.Kind() == reflect.Slice {
			expanded := make([]any, v.Len())
			for i := 0; i < v.Len(); i++ {
				expanded[i] = v.Index(i).Interface()
			}
			args = expanded
		}
	}

	rows, err := c.conn.Query(ctx, query, args...)
	if err != nil {
		c.logger.Errorf("query failed: %v", err)
		return ErrQueryExecutionFailed
	}
	defer func() {
		if cerr := rows.Close(); cerr != nil {
			c.logger.Errorf("failed to close rows: %v", cerr)
		}
	}()

	for rows.Next() {
		row := creator()
		if scanErr := rows.ScanStruct(row); scanErr != nil {
			c.logger.Errorf("failed to scan row: %v", scanErr)
			return ErrRowScanFailed
		}
		*results = append(*results, row)
	}

	// 检查是否有未处理的错误
	if iterErr := rows.Err(); iterErr != nil {
		c.logger.Errorf("rows iteration error: %v", iterErr)
		return ErrRowsIterationError
	}

	return nil
}

// QueryRow 执行查询并返回单行结果
func (c *Client) QueryRow(ctx context.Context, dest any, query string, args ...any) error {
	row := c.conn.QueryRow(ctx, query, args...)
	if row == nil {
		c.logger.Error("query row returned nil")
		return ErrRowNotFound
	}

	if err := row.ScanStruct(dest); err != nil {
		c.logger.Errorf("")
		return ErrRowScanFailed
	}

	return nil
}

// Select 封装 SELECT 子句
func (c *Client) Select(ctx context.Context, dest any, query string, args ...any) error {
	if c.conn == nil {
		c.logger.Error("clickhouse client is not initialized")
		return ErrClientNotInitialized
	}

	err := c.conn.Select(ctx, dest, query, args...)
	if err != nil {
		c.logger.Errorf("select failed: %v", err)
		return ErrQueryExecutionFailed
	}

	return nil
}

// Exec 执行非查询语句
func (c *Client) Exec(ctx context.Context, query string, args ...any) error {
	if c.conn == nil {
		c.logger.Error("clickhouse client is not initialized")
		return ErrClientNotInitialized
	}

	if err := c.conn.Exec(ctx, query, args...); err != nil {
		c.logger.Errorf("exec failed: %v", err)
		return ErrExecutionFailed
	}

	return nil
}

func (c *Client) prepareInsertData(data any) (string, string, []any, error) {
	val := reflect.ValueOf(data)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return "", "", nil, fmt.Errorf("data must be a non-nil pointer")
	}

	val = val.Elem()
	typ := val.Type()

	columns := make([]string, 0, typ.NumField())
	placeholders := make([]string, 0, typ.NumField())
	values := make([]any, 0, typ.NumField())

	values = structToValueArray(data)

	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		// 优先获取 `ch` 标签，其次获取 `json` 标签，最后使用字段名
		columnName := field.Tag.Get("ch")
		if columnName == "" {
			jsonTag := field.Tag.Get("json")
			if jsonTag != "" {
				tags := strings.Split(jsonTag, ",") // 只取逗号前的部分
				if len(tags) > 0 {
					columnName = tags[0]
				}
			}
		}
		if columnName == "" {
			columnName = field.Name
		}
		//columnName = strings.TrimSpace(columnName)

		columns = append(columns, columnName)
		placeholders = append(placeholders, "?")
	}

	return strings.Join(columns, ", "), strings.Join(placeholders, ", "), values, nil
}

// Insert 插入数据到指定表
func (c *Client) Insert(ctx context.Context, tableName string, in any) error {
	if c.conn == nil {
		c.logger.Error("clickhouse client is not initialized")
		return ErrClientNotInitialized
	}

	columns, placeholders, values, err := c.prepareInsertData(in)
	if err != nil {
		c.logger.Errorf("prepare insert in failed: %v", err)
		return ErrPrepareInsertDataFailed
	}

	// 构造 SQL 语句
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		columns,
		placeholders,
	)

	// 执行插入操作
	if err = c.conn.Exec(ctx, query, values...); err != nil {
		c.logger.Errorf("insert failed: %v", err)
		return ErrInsertFailed
	}

	return nil
}

func (c *Client) InsertMany(ctx context.Context, tableName string, data []any) error {
	if c.conn == nil {
		c.logger.Error("clickhouse client is not initialized")
		return ErrClientNotInitialized
	}

	if len(data) == 0 {
		c.logger.Error("data slice is empty")
		return ErrInvalidColumnData
	}

	var columns string
	var placeholders []string
	var values []any

	for _, item := range data {
		itemColumns, itemPlaceholders, itemValues, err := c.prepareInsertData(item)
		if err != nil {
			c.logger.Errorf("prepare insert data failed: %v", err)
			return ErrPrepareInsertDataFailed
		}

		if columns == "" {
			columns = itemColumns
		} else if columns != itemColumns {
			c.logger.Error("data items have inconsistent columns")
			return ErrInvalidColumnData
		}

		placeholders = append(placeholders, fmt.Sprintf("(%s)", itemPlaceholders))
		values = append(values, itemValues...)
	}

	// 构造 SQL 语句
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		tableName,
		columns,
		strings.Join(placeholders, ", "),
	)

	// 执行插入操作
	if err := c.conn.Exec(ctx, query, values...); err != nil {
		c.logger.Errorf("insert many failed: %v", err)
		return ErrInsertFailed
	}

	return nil
}

// AsyncInsert 异步插入数据
func (c *Client) AsyncInsert(ctx context.Context, tableName string, data any, wait bool) error {
	if c.conn == nil {
		c.logger.Error("clickhouse client is not initialized")
		return ErrClientNotInitialized
	}

	// 准备插入数据
	columns, placeholders, values, err := c.prepareInsertData(data)
	if err != nil {
		c.logger.Errorf("prepare insert data failed: %v", err)
		return ErrPrepareInsertDataFailed
	}

	// 构造 SQL 语句
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		columns,
		placeholders,
	)

	// 执行异步插入
	if err = c.asyncInsert(ctx, query, wait, values...); err != nil {
		c.logger.Errorf("async insert failed: %v", err)
		return ErrAsyncInsertFailed
	}

	return nil
}

// asyncInsert 异步插入数据
func (c *Client) asyncInsert(ctx context.Context, query string, wait bool, args ...any) error {
	if c.conn == nil {
		c.logger.Error("clickhouse client is not initialized")
		return ErrClientNotInitialized
	}

	if err := c.conn.AsyncInsert(ctx, query, wait, args...); err != nil {
		c.logger.Errorf("async insert failed: %v", err)
		return ErrAsyncInsertFailed
	}

	return nil
}

// AsyncInsertMany 批量异步插入数据
func (c *Client) AsyncInsertMany(ctx context.Context, tableName string, data []any, wait bool) error {
	if c.conn == nil {
		c.logger.Error("clickhouse client is not initialized")
		return ErrClientNotInitialized
	}

	if len(data) == 0 {
		c.logger.Error("data slice is empty")
		return ErrInvalidColumnData
	}

	// 准备插入数据的列名和占位符
	var columns string
	var placeholders []string
	var values []any

	for _, item := range data {
		itemColumns, itemPlaceholders, itemValues, err := c.prepareInsertData(item)
		if err != nil {
			c.logger.Errorf("prepare insert data failed: %v", err)
			return ErrPrepareInsertDataFailed
		}

		if columns == "" {
			columns = itemColumns
		} else if columns != itemColumns {
			c.logger.Error("data items have inconsistent columns")
			return ErrInvalidColumnData
		}

		placeholders = append(placeholders, fmt.Sprintf("(%s)", itemPlaceholders))
		values = append(values, itemValues...)
	}

	// 构造 SQL 语句
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		tableName,
		columns,
		strings.Join(placeholders, ", "),
	)

	// 执行异步插入操作
	if err := c.asyncInsert(ctx, query, wait, values...); err != nil {
		c.logger.Errorf("batch insert failed: %v", err)
		return err
	}

	return nil
}

// BatchInsert 批量插入数据
func (c *Client) BatchInsert(ctx context.Context, tableName string, data []any) error {
	if c.conn == nil {
		c.logger.Error("clickhouse client is not initialized")
		return ErrClientNotInitialized
	}

	if len(data) == 0 {
		c.logger.Error("data slice is empty")
		return ErrInvalidColumnData
	}

	// 准备插入数据的列名和占位符
	var columns string
	var values [][]any

	for _, item := range data {
		itemColumns, _, itemValues, err := c.prepareInsertData(item)
		if err != nil {
			c.logger.Errorf("prepare insert data failed: %v", err)
			return ErrPrepareInsertDataFailed
		}

		if columns == "" {
			columns = itemColumns
		} else if columns != itemColumns {
			c.logger.Error("data items have inconsistent columns")
			return ErrInvalidColumnData
		}

		values = append(values, itemValues)
	}

	// 构造 SQL 语句
	query := fmt.Sprintf("INSERT INTO %s (%s) VALUES", tableName, columns)

	// 调用 batchExec 方法执行批量插入
	if err := c.batchExec(ctx, query, values); err != nil {
		c.logger.Errorf("batch insert failed: %v", err)
		return ErrBatchInsertFailed
	}

	return nil
}

// batchExec 执行批量操作
func (c *Client) batchExec(ctx context.Context, query string, data [][]any) error {
	batch, err := c.conn.PrepareBatch(ctx, query)
	if err != nil {
		c.logger.Errorf("failed to prepare batch: %v", err)
		return ErrBatchPrepareFailed
	}

	for _, row := range data {
		if err = batch.Append(row...); err != nil {
			c.logger.Errorf("failed to append batch data: %v", err)
			return ErrBatchAppendFailed
		}
	}

	if err = batch.Send(); err != nil {
		c.logger.Errorf("failed to send batch: %v", err)
		return ErrBatchSendFailed
	}

	return nil
}

// BatchStructs 批量插入结构体数据
func (c *Client) BatchStructs(ctx context.Context, query string, data []any) error {
	if c.conn == nil {
		c.logger.Error("clickhouse client is not initialized")
		return ErrClientNotInitialized
	}

	// 准备批量插入
	batch, err := c.conn.PrepareBatch(ctx, query)
	if err != nil {
		c.logger.Errorf("failed to prepare batch: %v", err)
		return ErrBatchPrepareFailed
	}

	// 遍历数据并添加到批量插入
	for _, row := range data {
		if err = batch.AppendStruct(row); err != nil {
			c.logger.Errorf("failed to append batch struct data: %v", err)
			return ErrBatchAppendFailed
		}
	}

	// 发送批量插入
	if err = batch.Send(); err != nil {
		c.logger.Errorf("failed to send batch: %v", err)
		return ErrBatchSendFailed
	}

	return nil
}
