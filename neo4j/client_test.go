package neo4j

import (
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

// createTestClient 建立到本地 Neo4j 的连接（仅集成模式；默认
// bolt://localhost:7687 无认证）。
func createTestClient(t *testing.T) *Client {
	requireService(t)
	cli, err := NewClient()
	require.NoError(t, err)
	require.True(t, cli.CheckConnect(), "neo4j not reachable at localhost:7687")
	return cli
}

// TestCheckConnect_NoClient 未初始化的客户端连接探测恒失败。
func TestCheckConnect_NoClient(t *testing.T) {
	c := &Client{}
	assert.False(t, c.CheckConnect())
}

// TestCheckConnect_FakeDriver 注入替身驱动的连接探测随替身结果翻转。
func TestCheckConnect_FakeDriver(t *testing.T) {
	okDrv := &fakeDriver{}
	c1, err := NewClient(WithNeo4jDriver(okDrv))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c1.Close() })
	assert.True(t, c1.CheckConnect())

	okDrv.connErr = assert.AnError
	assert.False(t, c1.CheckConnect())
}

// TestClient_ZeroValueClose 零值客户端 Close 幂等返回（无连接可关）。
func TestClient_ZeroValueClose(t *testing.T) {
	assert.NoError(t, (&Client{}).Close())
}

// TestClient_Options 可断言选项的字段落位（WithBasicAuth 的凭证结构体
// 无公开面，仅调用覆盖）。
func TestClient_Options(t *testing.T) {
	c := &Client{}
	for _, o := range []Option{
		WithURI("bolt://host:1"),
		WithBasicAuth("u", "p", "realm"),
		nil,
	} {
		if o != nil {
			o(c)
		}
	}
	assert.Equal(t, "bolt://host:1", c.uri)

	drv := &fakeDriver{}
	WithNeo4jDriver(drv)(c)
	assert.Same(t, drv, c.drv, "injected driver takes over")
}
