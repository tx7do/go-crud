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
}
