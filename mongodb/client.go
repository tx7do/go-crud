package mongodb

import (
	"context"
	"fmt"
	"time"

	"github.com/tx7do/go-wind/log"

	mongoV2 "go.mongodb.org/mongo-driver/v2/mongo"
	optionsV2 "go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Client struct {
	cli     *mongoV2.Client
	options []*optionsV2.ClientOptions

	database string
	timeout  time.Duration // 默认超时时间
}

func NewClient(opts ...Option) (*Client, error) {
	c := &Client{
		options: []*optionsV2.ClientOptions{},
		timeout: 10 * time.Second,
	}

	for _, o := range opts {
		o(c)
	}
	if err := c.createMongodbClient(c.options); err != nil {
		return nil, err
	}

	return c, nil
}

// createMongodbClient 创建MongoDB客户端
func (c *Client) createMongodbClient(options []*optionsV2.ClientOptions) error {
	cli, err := mongoV2.Connect(options...)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to create mongodb client: %v", err))
		return err
	}

	c.cli = cli

	return nil
}

// Close 关闭MongoDB客户端
func (c *Client) Close() {
	if c.cli == nil {
		log.Warn(context.Background(), "mongodb client is already closed or not initialized")
		return
	}

	if err := c.cli.Disconnect(context.Background()); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to disconnect mongodb client: %v", err))
	} else {
		log.Info(context.Background(), "mongodb client disconnected successfully")
	}
	c.cli = nil
}

// CheckConnect 检查MongoDB连接状态
func (c *Client) CheckConnect() bool {
	if c.cli == nil {
		log.Error(context.Background(), fmt.Sprintf("mongodb client is not initialized"))
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	if err := c.cli.Ping(ctx, nil); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to ping mongodb: %v", err))
		return false
	}

	log.Info(context.Background(), "mongodb client is connected")
	return true
}

// FindOne 查询单个文档
func (c *Client) FindOne(ctx context.Context, collection string, filter any, result any) error {
	if c.cli == nil {
		log.Error(context.Background(), fmt.Sprintf("mongodb client is not initialized"))
		return mongoV2.ErrClientDisconnected
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	return c.cli.Database(c.database).Collection(collection).FindOne(ctx, filter).Decode(result)
}

// Find 查询多个文档
func (c *Client) Find(ctx context.Context, collection string, filter any, results any) error {
	if c.cli == nil {
		log.Error(context.Background(), fmt.Sprintf("mongodb client is not initialized"))
		return mongoV2.ErrClientDisconnected
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	cursor, err := c.cli.Database(c.database).Collection(collection).Find(ctx, filter)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to find documents in collection %s: %v", collection, err))
		return err
	}
	defer func(cursor *mongoV2.Cursor, ctx context.Context) {
		if err = cursor.Close(ctx); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to close cursor: %v", err))
		}
	}(cursor, ctx)

	return cursor.All(ctx, results)
}

// InsertOne 插入单个文档
func (c *Client) InsertOne(ctx context.Context, collection string, document any) (*mongoV2.InsertOneResult, error) {
	if c.cli == nil {
		log.Error(context.Background(), fmt.Sprintf("mongodb client is not initialized"))
		return nil, mongoV2.ErrClientDisconnected
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	return c.cli.Database(c.database).Collection(collection).InsertOne(ctx, document)
}

// InsertMany 插入多个文档
func (c *Client) InsertMany(ctx context.Context, collection string, documents []any) (*mongoV2.InsertManyResult, error) {
	if c.cli == nil {
		log.Error(context.Background(), fmt.Sprintf("mongodb client is not initialized"))
		return nil, mongoV2.ErrClientDisconnected
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	res, err := c.cli.Database(c.database).Collection(collection).InsertMany(ctx, documents)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to insert documents into collection %s: %v", collection, err))
		return nil, err
	}

	return res, nil
}

// UpdateOne 更新单个文档
func (c *Client) UpdateOne(ctx context.Context, collection string, filter, update any) (*mongoV2.UpdateResult, error) {
	if c.cli == nil {
		log.Error(context.Background(), fmt.Sprintf("mongodb client is not initialized"))
		return nil, mongoV2.ErrClientDisconnected
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	res, err := c.cli.Database(c.database).Collection(collection).UpdateOne(ctx, filter, update)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to update document in collection %s: %v", collection, err))
		return nil, err
	}

	return res, nil
}

// UpdateMany 更新多个文档
func (c *Client) UpdateMany(ctx context.Context, collection string, filter, update any) (*mongoV2.UpdateResult, error) {
	if c.cli == nil {
		log.Error(context.Background(), fmt.Sprintf("mongodb client is not initialized"))
		return nil, mongoV2.ErrClientDisconnected
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	res, err := c.cli.Database(c.database).Collection(collection).UpdateMany(ctx, filter, update)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to update documents in collection %s: %v", collection, err))
		return nil, err
	}

	return res, nil
}

// FindOneAndUpdate 在集合中查找并更新单个文档，结果 Decode 到 result 参数。
// 可传入可选的 *optionsV2.FindOneAndUpdateOptions。
func (c *Client) FindOneAndUpdate(ctx context.Context, collection string, filter, update any, result any, opts ...optionsV2.Lister[optionsV2.FindOneAndUpdateOptions]) error {
	if c.cli == nil {
		log.Error(context.Background(), fmt.Sprintf("mongodb client is not initialized"))
		return mongoV2.ErrClientDisconnected
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	sr := c.cli.Database(c.database).Collection(collection).FindOneAndUpdate(ctx, filter, update, opts...)
	if err := sr.Decode(result); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to FindOneAndUpdate in collection %s: %v", collection, err))
		return err
	}

	return nil
}

// DeleteOne 删除单个文档
func (c *Client) DeleteOne(ctx context.Context, collection string, filter any) (*mongoV2.DeleteResult, error) {
	if c.cli == nil {
		log.Error(context.Background(), fmt.Sprintf("mongodb client is not initialized"))
		return nil, mongoV2.ErrClientDisconnected
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	return c.cli.Database(c.database).Collection(collection).DeleteOne(ctx, filter)
}

// DeleteMany 删除多个文档
func (c *Client) DeleteMany(ctx context.Context, collection string, filter any) (*mongoV2.DeleteResult, error) {
	if c.cli == nil {
		log.Error(context.Background(), fmt.Sprintf("mongodb client is not initialized"))
		return nil, mongoV2.ErrClientDisconnected
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	res, err := c.cli.Database(c.database).Collection(collection).DeleteMany(ctx, filter)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to delete documents in collection %s: %v", collection, err))
		return nil, err
	}

	return res, nil
}

// Count 统计集合中文档数量，使用 Client 配置的超时和日志方式
func (c *Client) Count(ctx context.Context, collection string, filter any) (int64, error) {
	if c.cli == nil {
		log.Error(context.Background(), fmt.Sprintf("mongodb client is not initialized"))
		return 0, mongoV2.ErrClientDisconnected
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	count, err := c.cli.Database(c.database).Collection(collection).CountDocuments(ctx, filter)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to count documents in collection %s: %v", collection, err))
		return 0, err
	}

	return count, nil
}

// Exist 检查集合中是否存在满足 filter 的文档，返回布尔值和可能的错误。
// 使用 Client 的超时配置，客户端未初始化时返回 mongoV2.ErrClientDisconnected。
func (c *Client) Exist(ctx context.Context, collection string, filter any) (bool, error) {
	if c.cli == nil {
		log.Error(context.Background(), fmt.Sprintf("mongodb client is not initialized"))
		return false, mongoV2.ErrClientDisconnected
	}

	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	lim := int64(1)
	countOpts := optionsV2.Count().SetLimit(lim)
	count, err := c.cli.Database(c.database).Collection(collection).CountDocuments(ctx, filter, countOpts)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to count documents for existence check in collection %s: %v", collection, err))
		return false, err
	}

	return count > 0, nil
}
