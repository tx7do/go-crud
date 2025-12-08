package mongodb

import (
	"context"
	"os"
	"time"

	"github.com/go-kratos/kratos/v2/log"

	mongoV2 "go.mongodb.org/mongo-driver/v2/mongo"
	optionsV2 "go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Client struct {
	log *log.Helper

	cli      *mongoV2.Client
	database string
	timeout  time.Duration
}

func NewClient(opts ...Option) (*Client, error) {
	c := &Client{}

	var opt options
	for _, o := range opts {
		o(&opt)
	}

	if opt.Logger != nil {
		c.log = opt.Logger
	} else {
		c.log = log.NewHelper(log.NewStdLogger(os.Stderr))
	}

	if err := c.createMongodbClient(&opt); err != nil {
		return nil, err
	}

	return c, nil
}

// createMongodbClient 创建MongoDB客户端
func (c *Client) createMongodbClient(opt *options) error {
	var opts []*optionsV2.ClientOptions

	if opt.URI != "" {
		opts = append(opts, optionsV2.Client().ApplyURI(opt.URI))
	}
	if opt.Username != "" && opt.Password != "" {
		credential := optionsV2.Credential{
			Username: opt.Username,
			Password: opt.Password,
		}

		if opt.Password != "" {
			credential.PasswordSet = true
		}

		opts = append(opts, optionsV2.Client().SetAuth(credential))
	}
	if opt.ConnectTimeout != nil {
		opts = append(opts, optionsV2.Client().SetConnectTimeout(*opt.ConnectTimeout))
	}
	if opt.ServerSelectionTimeout != nil {
		opts = append(opts, optionsV2.Client().SetServerSelectionTimeout(*opt.ServerSelectionTimeout))
	}
	if opt.Timeout != nil {
		opts = append(opts, optionsV2.Client().SetTimeout(*opt.Timeout))
	}

	opts = append(opts, optionsV2.Client().SetBSONOptions(&optionsV2.BSONOptions{
		UseJSONStructTags: true, // 使用JSON结构标签
	}))

	cli, err := mongoV2.Connect(opts...)
	if err != nil {
		c.log.Errorf("failed to create mongodb client: %v", err)
		return err
	}

	c.database = opt.Database
	if opt.Timeout != nil {
		c.timeout = *opt.Timeout
	} else {
		c.timeout = 10 * time.Second // 默认超时时间
	}

	c.cli = cli

	return nil
}

// Close 关闭MongoDB客户端
func (c *Client) Close() {
	if c.cli == nil {
		c.log.Warn("mongodb client is already closed or not initialized")
		return
	}

	if err := c.cli.Disconnect(context.Background()); err != nil {
		c.log.Errorf("failed to disconnect mongodb client: %v", err)
	} else {
		c.log.Info("mongodb client disconnected successfully")
	}
}

// CheckConnect 检查MongoDB连接状态
func (c *Client) CheckConnect() {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	if err := c.cli.Ping(ctx, nil); err != nil {
		c.log.Errorf("failed to ping mongodb: %v", err)
	} else {
		c.log.Info("mongodb client is connected")
	}
}

// InsertOne 插入单个文档
func (c *Client) InsertOne(ctx context.Context, collection string, document interface{}) (*mongoV2.InsertOneResult, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	return c.cli.Database(c.database).Collection(collection).InsertOne(ctx, document)
}

// InsertMany 插入多个文档
func (c *Client) InsertMany(ctx context.Context, collection string, documents []interface{}) (*mongoV2.InsertManyResult, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	return c.cli.Database(c.database).Collection(collection).InsertMany(ctx, documents)
}

// FindOne 查询单个文档
func (c *Client) FindOne(ctx context.Context, collection string, filter interface{}, result interface{}) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	return c.cli.Database(c.database).Collection(collection).FindOne(ctx, filter).Decode(result)
}

// Find 查询多个文档
func (c *Client) Find(ctx context.Context, collection string, filter interface{}, results interface{}) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	// 防御性检查
	if c.cli == nil {
		if c.log != nil {
			c.log.Errorf("mongodb client is not initialized")
		}
		return mongoV2.ErrClientDisconnected
	}

	cursor, err := c.cli.Database(c.database).Collection(collection).Find(ctx, filter)
	if err != nil {
		c.log.Errorf("failed to find documents in collection %s: %v", collection, err)
		return err
	}
	defer func(cursor *mongoV2.Cursor, ctx context.Context) {
		if err = cursor.Close(ctx); err != nil {
			c.log.Errorf("failed to close cursor: %v", err)
		}
	}(cursor, ctx)

	return cursor.All(ctx, results)
}

// UpdateOne 更新单个文档
func (c *Client) UpdateOne(ctx context.Context, collection string, filter, update interface{}) (*mongoV2.UpdateResult, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	return c.cli.Database(c.database).Collection(collection).UpdateOne(ctx, filter, update)
}

// DeleteOne 删除单个文档
func (c *Client) DeleteOne(ctx context.Context, collection string, filter interface{}) (*mongoV2.DeleteResult, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	return c.cli.Database(c.database).Collection(collection).DeleteOne(ctx, filter)
}
