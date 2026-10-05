package milvus

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// requireService skips the test unless integration mode is enabled (KRATOS_IT)
// or in -short mode, to keep hermetic runs green.
func requireService(t *testing.T) {
	t.Helper()
	if os.Getenv("KRATOS_IT") == "" {
		t.Skip("skipping integration test: requires a live server; set KRATOS_IT to enable")
	}
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}
}

// createTestClient 建立到本地 Milvus 的连接（仅集成模式）。
func createTestClient(t *testing.T) *Client {
	requireService(t)
	cli, err := NewClient(WithAddress("localhost:19530"))
	require.NoError(t, err)
	require.True(t, cli.CheckConnect(), "milvus not reachable at localhost:19530")
	return cli
}

// TestCheckConnect_NoClient 未初始化的客户端连接探测恒失败。
func TestCheckConnect_NoClient(t *testing.T) {
	c := &Client{}
	assert.False(t, c.CheckConnect())
}

// TestClient_Guards 客户端守卫：未初始化客户端。
func TestClient_Guards(t *testing.T) {
	c := &Client{}
	ctx := context.Background()

	_, err := c.HasCollection(ctx, "x")
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	err = c.DropCollection(ctx, "x")
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	err = c.DropVectorCollection(ctx, "x")
	assert.ErrorIs(t, err, ErrClientNotInitialized)
}

// TestClient_ZeroValueClose 零值客户端 Close 幂等返回（无连接可关）。
func TestClient_ZeroValueClose(t *testing.T) {
	assert.NoError(t, (&Client{}).Close())
}

// TestClient_Options 各选项的字段落位。
func TestClient_Options(t *testing.T) {
	c := &Client{}
	for _, o := range []Option{
		WithAddress("h:1"),
		WithUsername("u"),
		WithPassword("p"),
		WithAPIKey("k"),
		WithDBName("db"),
	} {
		o(c)
	}
	assert.Equal(t, "h:1", c.address)
	assert.Equal(t, "u", c.username)
	assert.Equal(t, "p", c.password)
	assert.Equal(t, "k", c.apiKey)
	assert.Equal(t, "db", c.dbName)
}

// TestClient_DropVectorCollectionAlias 别名透传到注入的客户端。
func TestClient_DropVectorCollectionAlias(t *testing.T) {
	fc := &fakeMilvusClient{}
	c, err := NewClient(WithMilvusClient(fc))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	require.NoError(t, c.DropVectorCollection(context.Background(), "coll"))
	assert.Equal(t, 1, fc.dropCalls)
}
