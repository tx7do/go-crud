package neo4j

import (
	"context"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// Client 包装 Neo4j 官方驱动。
type Client struct {
	drv  neo4j.DriverWithContext
	uri  string
	auth neo4j.AuthToken
}

// NewClient 创建 Neo4j 客户端。
func NewClient(opts ...Option) (*Client, error) {
	c := &Client{
		uri:  "bolt://localhost:7687",
		auth: neo4j.NoAuth(),
	}
	for _, o := range opts {
		if o != nil {
			o(c)
		}
	}
	if c.drv != nil {
		return c, nil
	}

	drv, err := neo4j.NewDriverWithContext(c.uri, c.auth)
	if err != nil {
		return nil, err
	}
	c.drv = drv
	return c, nil
}

// Close 关闭驱动及其连接池。
func (c *Client) Close() error {
	if c.drv == nil {
		return nil
	}
	return c.drv.Close(context.Background())
}

// CheckConnect 探测连接可用性。
func (c *Client) CheckConnect() bool {
	if c.drv == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return c.drv.VerifyConnectivity(ctx) == nil
}
