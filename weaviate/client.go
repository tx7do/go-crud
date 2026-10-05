package weaviate

import (
	"context"
	"fmt"
	"net/http"
	"time"

	wvc "github.com/weaviate/weaviate-go-client/v4/weaviate"
)

// Client 包装 weaviate 官方 REST 客户端。
type Client struct {
	cli *wvc.Client

	host       string
	scheme     string
	headers    map[string]string
	httpClient *http.Client
}

// NewClient 创建 Weaviate 客户端。
// 连接为惰性建立（不做启动探测，StartupTimeout 置 0），离线环境亦可构建。
func NewClient(opts ...Option) (*Client, error) {
	c := &Client{
		host:   "localhost:8080",
		scheme: "http",
	}
	for _, o := range opts {
		if o != nil {
			o(c)
		}
	}
	if c.cli != nil {
		return c, nil
	}

	cli, err := wvc.NewClient(wvc.Config{
		Host:             c.host,
		Scheme:           c.scheme,
		Headers:          c.headers,
		ConnectionClient: c.httpClient,
		StartupTimeout:   0,
	})
	if err != nil {
		return nil, err
	}
	c.cli = cli
	return c, nil
}

// Close 关闭客户端连接（本模块不启用 gRPC 通道，无连接可关，幂等返回）。
func (c *Client) Close() error {
	return nil
}

// CheckConnect 探测连接可用性（就绪探针）。
func (c *Client) CheckConnect() bool {
	if c.cli == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ready, err := c.cli.Misc().ReadyChecker().Do(ctx)
	return err == nil && ready
}

// HasCollection 判断集合（class）是否存在。
func (c *Client) HasCollection(ctx context.Context, collection string) (bool, error) {
	if c.cli == nil {
		return false, ErrClientNotInitialized
	}
	return c.cli.Schema().ClassExistenceChecker().WithClassName(collection).Do(ctx)
}

// DropCollection 删除集合（class）。
func (c *Client) DropCollection(ctx context.Context, collection string) error {
	if c.cli == nil {
		return ErrClientNotInitialized
	}
	if err := c.cli.Schema().ClassDeleter().WithClassName(collection).Do(ctx); err != nil {
		return fmt.Errorf("%w: %v", ErrDeleteFailed, err)
	}
	return nil
}
