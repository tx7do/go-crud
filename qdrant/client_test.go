package qdrant

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tx7do/go-crud/vector"
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

// createTestClient 建立到本地 Qdrant 的连接（仅集成模式）。
func createTestClient(t *testing.T) *Client {
	requireService(t)
	cli, err := NewClient(
		WithHost("localhost"),
		WithPort(6334),
	)
	require.NoError(t, err)
	require.True(t, cli.CheckConnect(), "qdrant not reachable at localhost:6334")
	return cli
}

// TestCheckConnect_NoClient 未初始化的客户端连接探测恒失败。
func TestCheckConnect_NoClient(t *testing.T) {
	c := &Client{}
	assert.False(t, c.CheckConnect())
}

// TestClient_Guards 客户端守卫：未初始化 / 参数非法。
func TestClient_Guards(t *testing.T) {
	c := &Client{}
	ctx := context.Background()

	_, err := c.HasCollection(ctx, "x")
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	err = c.DropCollection(ctx, "x")
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	err = c.CreateVectorCollection(ctx, "x", 4, vector.MetricCosine)
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	err = c.CreatePayloadIndex(ctx, "x", "f", PayloadIndexKeyword)
	assert.ErrorIs(t, err, ErrClientNotInitialized)

	// 参数非法（本地客户端即可断言，不触网络）。
	local := NewClientLocal(t)
	err = local.CreateVectorCollection(ctx, "", 4, vector.MetricCosine)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	err = local.CreateVectorCollection(ctx, "x", 0, vector.MetricCosine)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	err = local.CreateVectorCollection(ctx, "x", 4, "bogus")
	assert.ErrorIs(t, err, ErrInvalidRequest)
	err = local.CreatePayloadIndex(ctx, "", "f", PayloadIndexKeyword)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	err = local.CreatePayloadIndex(ctx, "x", "", PayloadIndexKeyword)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	err = local.CreatePayloadIndex(ctx, "x", "f", PayloadIndexKind(99))
	assert.ErrorIs(t, err, ErrInvalidRequest)
}

// NewClientLocal 构造带占位 SDK 客户端的包装（不连网，仅触发参数校验分支）。
func NewClientLocal(t *testing.T) *Client {
	t.Helper()
	c, err := NewClient(WithHost("localhost"), WithPort(6334))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestIntegration_VectorCollectionLifecycle 集合生命周期（需 KRATOS_IT）。
func TestIntegration_VectorCollectionLifecycle(t *testing.T) {
	c := createTestClient(t)
	defer c.Close()
	ctx := context.Background()

	const coll = "go_crud_qdrant_test_lifecycle"
	_ = c.DropCollection(ctx, coll)

	require.NoError(t, c.CreateVectorCollection(ctx, coll, 4, vector.MetricCosine))
	exists, err := c.HasCollection(ctx, coll)
	require.NoError(t, err)
	assert.True(t, exists)

	// tenant_id 整数载荷索引（多租户推荐实践）。
	require.NoError(t, c.CreatePayloadIndex(ctx, coll, "tenant_id", PayloadIndexInteger))

	require.NoError(t, c.DropVectorCollection(ctx, coll))
	exists, err = c.HasCollection(ctx, coll)
	require.NoError(t, err)
	assert.False(t, exists)
}
