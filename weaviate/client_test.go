package weaviate

import (
	"context"
	"net/http"
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

// createTestClient 建立到本地 Weaviate 的连接（仅集成模式；默认
// http://localhost:8080 匿名访问）。
func createTestClient(t *testing.T) *Client {
	requireService(t)
	cli, err := NewClient(WithHost("localhost:8080"))
	require.NoError(t, err)
	require.True(t, cli.CheckConnect(), "weaviate not reachable at localhost:8080")
	return cli
}

// TestCheckConnect_NoClient 未初始化的客户端连接探测恒失败。
func TestCheckConnect_NoClient(t *testing.T) {
	c := &Client{}
	assert.False(t, c.CheckConnect())
}

// TestCheckConnect_FakeTransport 就绪探针随替身结果翻转。
func TestCheckConnect_FakeTransport(t *testing.T) {
	ft := &fakeTransport{}
	c1, err := NewClient(WithHost("localhost:8080"), WithHTTPClient(&http.Client{Transport: ft}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c1.Close() })
	assert.True(t, c1.CheckConnect())

	ft.readyFail = true
	assert.False(t, c1.CheckConnect())
}

// TestClient_ZeroValueClose 零值客户端 Close 幂等返回（本模块不启用
// gRPC 通道，无连接可关）。
func TestClient_ZeroValueClose(t *testing.T) {
	assert.NoError(t, (&Client{}).Close())
}

// TestClient_Options 可断言选项的字段落位。
func TestClient_Options(t *testing.T) {
	c := &Client{}
	for _, o := range []Option{
		WithHost("weaviate:1234"),
		WithScheme("https"),
		WithHeaders(map[string]string{"Authorization": "Bearer x"}),
		nil,
	} {
		if o != nil {
			o(c)
		}
	}
	assert.Equal(t, "weaviate:1234", c.host)
	assert.Equal(t, "https", c.scheme)
	assert.Equal(t, map[string]string{"Authorization": "Bearer x"}, c.headers)

	hc := &http.Client{Transport: &fakeTransport{}}
	WithHTTPClient(hc)(c)
	assert.Same(t, hc, c.httpClient, "injected http client takes over")

	inner, err := NewClient(WithHost("localhost:8080"), WithHTTPClient(&http.Client{Transport: &fakeTransport{}}))
	require.NoError(t, err)
	injected, err := NewClient(WithWeaviateClient(inner.cli))
	require.NoError(t, err)
	assert.Same(t, inner.cli, injected.cli, "injected client takes over (skips real construction)")
}

// TestClient_Guards 未初始化客户端的集合守卫。
func TestClient_Guards(t *testing.T) {
	c := &Client{}
	ctx := context.Background()

	_, err := c.HasCollection(ctx, "x")
	assert.ErrorIs(t, err, ErrClientNotInitialized)
	err = c.DropCollection(ctx, "x")
	assert.ErrorIs(t, err, ErrClientNotInitialized)
}
