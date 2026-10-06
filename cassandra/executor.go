package cassandra

import (
	"context"

	"github.com/gocql/gocql"
)

// sessionExecutor 仓库对底层会话的操作面抽象。
//
// gocql.Session 是具体类型无法替身，仓库只依赖本接口：*Client 是默认实现，
// 离线测试注入记录型替身（见 fakeexecutor_test.go）。
type sessionExecutor interface {
	Exec(ctx context.Context, stmt string, args ...any) error
	Select(ctx context.Context, stmt string, args ...any) ([]map[string]any, error)
	Batch(ctx context.Context, batchType gocql.BatchType, stmts []string, argsList [][]any) error
}

// 编译期断言：*Client 满足仓库的会话操作面。
var _ sessionExecutor = (*Client)(nil)

// Select 执行查询语句并以「列名 → 值」映射返回全部行。
// 值的 Go 类型由 gocql 按列类型决定（int 列 → int、bigint 列 → int64、
// uuid 列 → gocql.UUID，见 goType 映射表）。
func (c *Client) Select(ctx context.Context, stmt string, args ...any) ([]map[string]any, error) {
	if c.session == nil || c.session.Closed() {
		return nil, ErrSessionClosed
	}
	if stmt == "" {
		return nil, ErrInvalidRequest
	}
	iter := c.session.Query(stmt, args...).WithContext(ctx).Iter()
	rows := make([]map[string]any, 0, 8)
	for {
		row := map[string]any{}
		if !iter.MapScan(row) {
			break
		}
		rows = append(rows, row)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return rows, nil
}

// Batch 执行批量语句（ExecBatch 的接口形态别名，见 sessionExecutor）。
func (c *Client) Batch(ctx context.Context, batchType gocql.BatchType, stmts []string, argsList [][]any) error {
	return c.ExecBatch(ctx, batchType, stmts, argsList)
}
