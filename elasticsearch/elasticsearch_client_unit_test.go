package elasticsearch

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// newBareClient returns a Client whose With* setters can be exercised without
// invoking elasticsearch.New. The setters populate esOpts, which these tests
// inspect directly. NewElasticsearchClient (tested separately) consumes and
// clears esOpts, so accumulation is observed on a bare instance.
func newBareClient() *Client {
	return &Client{}
}

// TestNewElasticsearchClient_NoOptions ensures the migrated constructor builds
// a client without any options and initializes the embedded *Client. The
// opensearch counterpart is mirrored here.
func TestNewElasticsearchClient_NoOptions(t *testing.T) {
	c, err := NewElasticsearchClient()
	assert.Nil(t, err)
	if assert.NotNil(t, c) {
		assert.NotNil(t, c.Client, "embedded elasticsearch client should be initialized")
	}
	// esOpts must be cleared after construction.
	assert.Nil(t, c.esOpts)
}

// TestWithAddresses_Accumulates verifies WithAddresses routes to the library
// address option and appends exactly one option to esOpts.
func TestWithAddresses_Accumulates(t *testing.T) {
	c := newBareClient()
	WithAddresses("http://node-a:9200", "http://node-b:9200")(c)
	assert.Len(t, c.esOpts, 1)
}

// TestWithBasicAuth_BufferOrderIndependence verifies that WithUsername /
// WithPassword are buffered (not emitted as separate options) and that both
// call orders leave the buffer fully populated, ready for the single
// WithBasicAuth flush performed in NewElasticsearchClient.
func TestWithBasicAuth_BufferOrderIndependence(t *testing.T) {
	t.Run("user_then_pass", func(t *testing.T) {
		c := newBareClient()
		WithUsername("u")(c)
		// Before the password arrives, only the user half is buffered and no
		// option has been emitted.
		assert.True(t, c.pendingBasicAuth.haveUser)
		assert.False(t, c.pendingBasicAuth.havePass)
		assert.Empty(t, c.esOpts)
		WithPassword("p")(c)
		assert.True(t, c.pendingBasicAuth.haveUser && c.pendingBasicAuth.havePass)
		// Still no option emitted — the flush happens only at construction.
		assert.Empty(t, c.esOpts)
	})

	t.Run("pass_then_user", func(t *testing.T) {
		c := newBareClient()
		WithPassword("p")(c)
		assert.False(t, c.pendingBasicAuth.haveUser)
		assert.True(t, c.pendingBasicAuth.havePass)
		WithUsername("u")(c)
		assert.True(t, c.pendingBasicAuth.haveUser && c.pendingBasicAuth.havePass)
		assert.Empty(t, c.esOpts)
	})
}

// TestNewElasticsearchClient_BasicAuthFlush verifies that when both
// credentials are supplied, the constructor consumes the buffered pair
// (resetting the buffer) and clears esOpts after handing them to New.
func TestNewElasticsearchClient_BasicAuthFlush(t *testing.T) {
	c, err := NewElasticsearchClient(
		WithUsername("u"),
		WithPassword("p"),
	)
	assert.Nil(t, err)
	if assert.NotNil(t, c) {
		// The buffer is reset and esOpts cleared after construction.
		assert.False(t, c.pendingBasicAuth.haveUser || c.pendingBasicAuth.havePass)
		assert.Nil(t, c.esOpts)
	}
}

// TestWithSingleOptionSetters verifies the boolean-gated setters that emit
// exactly one option when enabled, and zero when disabled.
func TestWithSingleOptionSetters(t *testing.T) {
	cases := []struct {
		name string
		fn    func(*Client)
	}{
		{"DiscoverNodesOnStart", func(c *Client) { WithDiscoverNodesOnStart(true)(c) }},
		{"EnableCompatibilityMode", func(c *Client) { WithEnableCompatibilityMode(true)(c) }},
		{"DisableMetaHeader", func(c *Client) { WithDisableMetaHeader(true)(c) }},
		{"AutoDrainBody", func(c *Client) { WithAutoDrainBody(true)(c) }},
		{"CompressRequestBody", func(c *Client) { WithCompressRequestBody(true)(c) }},
		{"PoolCompressor", func(c *Client) { WithPoolCompressor(true)(c) }},
	}
	for _, tc := range cases {
		t.Run(tc.name+"/enabled", func(t *testing.T) {
			c := newBareClient()
			tc.fn(c)
			assert.Len(t, c.esOpts, 1, "%s enabled should append one option", tc.name)
		})
		t.Run(tc.name+"/disabled", func(t *testing.T) {
			c := newBareClient()
			tc.fn(c) // override with disabled below
			// Re-run with the disabled variant.
			c = newBareClient()
			switch tc.name {
			case "DiscoverNodesOnStart":
				WithDiscoverNodesOnStart(false)(c)
			case "EnableCompatibilityMode":
				WithEnableCompatibilityMode(false)(c)
			case "DisableMetaHeader":
				WithDisableMetaHeader(false)(c)
			case "AutoDrainBody":
				WithAutoDrainBody(false)(c)
			case "CompressRequestBody":
				WithCompressRequestBody(false)(c)
			case "PoolCompressor":
				WithPoolCompressor(false)(c)
			}
			assert.Empty(t, c.esOpts, "%s disabled should append no option", tc.name)
		})
	}
}

// TestWithTransportPassthroughSetters verifies setters routed through
// WithTransportOptions append exactly one option each.
func TestWithTransportPassthroughSetters(t *testing.T) {
	cases := []struct {
		name string
		fn    func(*Client)
	}{
		{"Transport", func(c *Client) { WithTransport(http.DefaultTransport)(c) }},
		{"Header", func(c *Client) { WithHeader(http.Header{"X-A": []string{"b"}})(c) }},
		{"DiscoverNodesInterval", func(c *Client) { WithDiscoverNodesInterval(time.Second)(c) }},
		{"DisableRetry", func(c *Client) { WithDisableRetry(true)(c) }},
		{"MaxRetries", func(c *Client) { WithMaxRetries(5)(c) }},
		{"RetryOnStatus", func(c *Client) { WithRetryOnStatus(503)(c) }},
		{"RetryBackoff", func(c *Client) {
			WithRetryBackoff(func(int) time.Duration { return time.Second })(c)
		}},
		{"RetryOnError", func(c *Client) {
			WithRetryOnError(func(*http.Request, error) bool { return false })(c)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newBareClient()
			tc.fn(c)
			assert.Len(t, c.esOpts, 1, "%s should append one transport option", tc.name)
		})
	}
}

// TestWithScalarSetters verifies string/scalar setters that always emit one
// option regardless of value.
func TestWithScalarSetters(t *testing.T) {
	cases := []struct {
		name string
		fn    func(*Client)
	}{
		{"CloudID", func(c *Client) { WithCloudID("cloud-id")(c) }},
		{"APIKey", func(c *Client) { WithAPIKey("key")(c) }},
		{"ServiceToken", func(c *Client) { WithServiceToken("tok")(c) }},
		{"CertificateFingerprint", func(c *Client) { WithCertificateFingerprint("fp")(c) }},
		{"CACert", func(c *Client) { WithCACert([]byte("cert"))(c) }},
		{"CompressRequestBodyLevel", func(c *Client) { WithCompressRequestBodyLevel(1)(c) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newBareClient()
			tc.fn(c)
			assert.Len(t, c.esOpts, 1, "%s should append one option", tc.name)
		})
	}
}

// TestBatchUpdateDocument_EmptyDataSet verifies the empty-dataSet short-circuit
// returns nil without performing any bulk work.
func TestBatchUpdateDocument_EmptyDataSet(t *testing.T) {
	c := newBareClient()
	err := c.BatchUpdateDocument(context.Background(), "any", nil, nil)
	assert.Nil(t, err)
	err = c.BatchUpdateDocument(context.Background(), "any", []any{}, nil)
	assert.Nil(t, err)
}

// TestBatchDeleteDocument_EmptyIDs verifies the empty-ids short-circuit returns
// nil without performing any bulk work.
func TestBatchDeleteDocument_EmptyIDs(t *testing.T) {
	c := newBareClient()
	err := c.BatchDeleteDocument(context.Background(), "any", nil)
	assert.Nil(t, err)
	err = c.BatchDeleteDocument(context.Background(), "any", []string{})
	assert.Nil(t, err)
}

// TestCount_EmptyIndex verifies the empty-index guard returns ErrInvalidRequest.
func TestCount_EmptyIndex(t *testing.T) {
	c := newBareClient()
	_, err := c.Count(context.Background(), "", nil)
	assert.True(t, errors.Is(err, ErrInvalidRequest))
}

// TestSearchScroll_EmptyScrollID verifies the empty-scrollID guard returns
// ErrInvalidRequest before contacting the cluster.
func TestSearchScroll_EmptyScrollID(t *testing.T) {
	c := newBareClient()
	_, err := c.SearchScroll(context.Background(), "", "1m")
	assert.True(t, errors.Is(err, ErrInvalidRequest))
}

// TestClearScroll_EmptyScrollID verifies the empty-scrollID guard returns
// ErrInvalidRequest.
func TestClearScroll_EmptyScrollID(t *testing.T) {
	c := newBareClient()
	err := c.ClearScroll(context.Background(), "")
	assert.True(t, errors.Is(err, ErrInvalidRequest))
}
