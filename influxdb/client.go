package influxdb

import (
	"context"
	"fmt"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/tx7do/go-wind/log"
)

type Client struct {
	cli     *influxdb3.Client
	options *influxdb3.ClientConfig
}

func NewClient(opts ...Option) (*Client, error) {
	c := &Client{
		options: &influxdb3.ClientConfig{},
	}

	for _, o := range opts {
		o(c)
	}
	if err := c.createInfluxdbClient(c.options); err != nil {
		return nil, err
	}

	return c, nil
}

// createInfluxdbClient 创建InfluxDB客户端
func (c *Client) createInfluxdbClient(opt *influxdb3.ClientConfig) error {
	client, err := influxdb3.New(*opt)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to create influxdb client: %v", err))
		return err
	}

	c.cli = client

	return nil
}

// Close 关闭InfluxDB客户端
func (c *Client) Close() {
	if c.cli == nil {
		log.Warn(context.Background(), "influxdb client is nil, nothing to close")
		return
	}

	if err := c.cli.Close(); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to close influxdb client: %v", err))
	} else {
		log.Info(context.Background(), "influxdb client closed successfully")
		c.cli = nil
	}
}

// ServerVersion 获取InfluxDB服务器版本
func (c *Client) ServerVersion() string {
	if c.cli == nil {
		log.Warn(context.Background(), "influxdb client is nil, cannot get server version")
		return ""
	}
	ver, err := c.cli.GetServerVersion()
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to get server version: %v", err))
		return ""
	}
	return ver
}

// Query 查询数据
func (c *Client) Query(ctx context.Context, query string) (*influxdb3.QueryIterator, error) {
	if c.cli == nil {
		return nil, ErrInfluxDBClientNotInitialized
	}

	result, err := c.ExecInfluxQLQuery(ctx, query)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to query data: %v", err))
		return nil, ErrInfluxDBQueryFailed
	}

	return result, nil
}

// QueryWithParams 使用参数化方式查询数据
func (c *Client) QueryWithParams(
	ctx context.Context,
	table string,
	filters map[string]any,
	operators map[string]string,
	fields []string,
) (*influxdb3.QueryIterator, error) {
	if c.cli == nil {
		return nil, ErrInfluxDBClientNotInitialized
	}

	query := BuildQueryWithParams(table, filters, operators, fields)

	result, err := c.ExecInfluxQLQuery(ctx, query)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to query data: %v", err))
		return nil, ErrInfluxDBQueryFailed
	}

	return result, nil
}

// Insert 插入数据
func (c *Client) Insert(ctx context.Context, point *influxdb3.Point) error {
	if c.cli == nil {
		return ErrInfluxDBClientNotInitialized
	}

	if point == nil {
		return ErrInvalidPoint
	}

	points := []*influxdb3.Point{point}
	if err := c.WritePointsStrict(ctx, points); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to insert data: %v", err))
		return ErrInsertFailed
	}

	return nil
}

// BatchInsert 批量插入数据
func (c *Client) BatchInsert(ctx context.Context, points []*influxdb3.Point) error {
	if c.cli == nil {
		return ErrInfluxDBClientNotInitialized
	}

	if len(points) == 0 {
		return ErrNoPointsToInsert
	}

	if err := c.WritePointsStrict(ctx, points); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to batch insert data: %v", err))
		return ErrBatchInsertFailed
	}

	return nil
}

// Count 执行计数查询并返回解析到的数量
func (c *Client) Count(ctx context.Context, query string) (int64, error) {
	if c.cli == nil {
		return 0, ErrInfluxDBClientNotInitialized
	}

	it, err := c.ExecInfluxQLQuery(ctx, query, influxdb3.WithQueryType(influxdb3.InfluxQL))
	if err != nil {
		return 0, err
	}

	// 读取第一条记录的值作为计数
	for it.Next() {
		// 尝试第一个可转换的数值字段
		for _, v := range it.Value() {
			if n, ok := numericToInt64(v); ok {
				return n, nil
			}
		}
	}
	if err = it.Err(); err != nil {
		log.Error(context.Background(), fmt.Sprintf("query iterator error: %v", err))
		return 0, ErrInfluxDBQueryFailed
	}
	// 未读到记录时返回 0
	return 0, nil
}

// Exist 执行查询并判断是否存在记录（有任意一条记录即为存在）
func (c *Client) Exist(ctx context.Context, query string) (bool, error) {
	if c.cli == nil {
		return false, ErrInfluxDBClientNotInitialized
	}

	it, err := c.ExecInfluxQLQuery(ctx, query)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to exec exist query: %v, query: %s", err, query))
		return false, ErrInfluxDBQueryFailed
	}

	// 只要有一条记录即认为存在
	for it.Next() {
		return true, nil
	}

	if err = it.Err(); err != nil {
		log.Error(context.Background(), fmt.Sprintf("query iterator error: %v", err))
		return false, ErrInfluxDBQueryFailed
	}

	return false, nil
}

// ExecInfluxQLQuery 执行 Flux/InfluxQL 查询并返回原始迭代器
func (c *Client) ExecInfluxQLQuery(ctx context.Context, query string, opts ...influxdb3.QueryOption) (*influxdb3.QueryIterator, error) {
	if c.cli == nil {
		return nil, ErrInfluxDBClientNotInitialized
	}

	finalOpts := append([]influxdb3.QueryOption{influxdb3.WithQueryType(influxdb3.InfluxQL)}, opts...)
	it, err := c.cli.Query(ctx, query, finalOpts...)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to exec InfluxQL query: %v, query: %s", err, query))
		return nil, ErrInfluxDBQueryFailed
	}

	return it, nil
}

// ExecSQLQuery 执行 SQL 查询并返回原始迭代器
func (c *Client) ExecSQLQuery(ctx context.Context, query string, opts ...influxdb3.QueryOption) (*influxdb3.QueryIterator, error) {
	if c.cli == nil {
		return nil, ErrInfluxDBClientNotInitialized
	}

	finalOpts := append([]influxdb3.QueryOption{influxdb3.WithQueryType(influxdb3.SQL)}, opts...)
	it, err := c.cli.Query(ctx, query, finalOpts...)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to exec SQL query: %v, query: %s", err, query))
		return nil, ErrInfluxDBQueryFailed
	}

	return it, nil
}

// WritePointsStrict 接受严格类型 []*influxdb3.Point 并写入
func (c *Client) WritePointsStrict(ctx context.Context, points []*influxdb3.Point) error {
	if c.cli == nil {
		return ErrInfluxDBClientNotInitialized
	}
	if len(points) == 0 {
		return nil
	}
	if err := c.cli.WritePoints(ctx, points); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to write points: %v", err))
		return ErrBatchInsertFailed
	}
	return nil
}

// WritePoints 将通用点集合写入 InfluxDB；仅支持传入的元素为 *influxdb3.Point
func (c *Client) WritePoints(ctx context.Context, pts []any) error {
	points, err := ConvertAnyToPointsSafe(pts)
	if err != nil {
		return err
	}
	return c.WritePointsStrict(ctx, points)
}
