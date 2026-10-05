package milvus

import (
	"context"
	"math"
	"time"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"google.golang.org/grpc"
)

// Client 包装 milvus 官方 SDK 客户端。
type Client struct {
	cli client.Client

	address  string
	username string
	password string
	apiKey   string
	dbName   string
}

// NewClient 创建 Milvus 客户端。
//
// 拨号选项覆写官方默认（client.DefaultGrpcOpts）：
//   - 去掉 grpc.WithBlock()：官方默认在服务端不可达时无限阻塞构造函数，
//     改为 gRPC 惯例的惰性连接（首个请求时建立）；
//   - 保留官方默认的接收消息上限（2GB），否则大结果集会被 4MB 默认截断；
//   - DisableConn：跳过 Connect 握手。该握手仅填充遥测标识与
//     ServerVersion（本 SDK 内只写不读），其能力开关仅面向不支持
//     Connect 的旧服务端（2.4+ 均支持）。
func NewClient(opts ...Option) (*Client, error) {
	c := &Client{
		address: "localhost:19530",
	}
	for _, o := range opts {
		if o != nil {
			o(c)
		}
	}
	if c.cli != nil {
		return c, nil
	}

	cli, err := client.NewClient(context.Background(), client.Config{
		Address:  c.address,
		Username: c.username,
		Password: c.password,
		APIKey:   c.apiKey,
		DBName:   c.dbName,
		DialOptions: []grpc.DialOption{
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(math.MaxInt32)),
		},
		DisableConn: true,
	})
	if err != nil {
		return nil, err
	}
	c.cli = cli
	return c, nil
}

// Close 关闭客户端连接。
func (c *Client) Close() error {
	if c.cli == nil {
		return nil
	}
	return c.cli.Close()
}

// CheckConnect 探测连接可用性（健康检查探针）。
func (c *Client) CheckConnect() bool {
	if c.cli == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	state, err := c.cli.CheckHealth(ctx)
	return err == nil && state != nil && state.IsHealthy
}

// HasCollection 判断集合是否存在。
func (c *Client) HasCollection(ctx context.Context, collection string) (bool, error) {
	if c.cli == nil {
		return false, ErrClientNotInitialized
	}
	return c.cli.HasCollection(ctx, collection)
}

// DropCollection 删除集合。
func (c *Client) DropCollection(ctx context.Context, collection string) error {
	if c.cli == nil {
		return ErrClientNotInitialized
	}
	return c.cli.DropCollection(ctx, collection)
}

// DropVectorCollection 删除带向量字段的集合（DropCollection 的别名）。
func (c *Client) DropVectorCollection(ctx context.Context, collection string) error {
	return c.DropCollection(ctx, collection)
}
