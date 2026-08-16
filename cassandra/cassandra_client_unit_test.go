package cassandra

import (
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
)

// TestNewCassandraClient_NoHosts verifies that constructing without hosts
// fails (gocql rejects an empty host list) and returns a nil client.
func TestNewCassandraClient_NoHosts(t *testing.T) {
	c, err := NewCassandraClient()
	assert.NotNil(t, err, "construction without hosts must error")
	assert.Nil(t, c)
}

// TestClient_NilSessionClosed verifies the nil-session guard: a client built
// without a live session reports closed and its methods return ErrSessionClosed.
func TestClient_NilSessionClosed(t *testing.T) {
	c := &Client{session: nil}
	assert.True(t, c.Closed())

	err := c.Exec(nil, "SELECT * FROM t")
	assert.True(t, errors.Is(err, ErrSessionClosed))
	_, err = c.Query(nil, "SELECT * FROM t")
	assert.True(t, errors.Is(err, ErrSessionClosed))
	err = c.ExecBatch(nil, gocql.LoggedBatch, []string{"x"}, [][]any{{}})
	assert.True(t, errors.Is(err, ErrSessionClosed))
	err = c.ExecBatchBound(nil, gocql.LoggedBatch, []string{"x"}, []func(*gocql.QueryInfo) ([]any, error){nil})
	assert.True(t, errors.Is(err, ErrSessionClosed))
}

// TestOptions_Setters verifies the With* setters populate the unexported
// options struct fields. Same-package access lets us assert directly.
func TestOptions_Setters(t *testing.T) {
	var o options

	WithHosts("h1", "h2")(&o)
	assert.Equal(t, []string{"h1", "h2"}, o.Hosts)

	WithUsername("u")(&o)
	assert.Equal(t, "u", o.Username)
	WithPassword("p")(&o)
	assert.Equal(t, "p", o.Password)

	WithKeyspace("ks")(&o)
	assert.Equal(t, "ks", o.Keyspace)

	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	WithTLSConfig(tlsCfg)(&o)
	assert.Equal(t, tlsCfg, o.TLSConfig)

	WithConnectTimeout(7 * time.Second)(&o)
	assert.Equal(t, 7*time.Second, o.ConnectTimeout)
	WithTimeout(9 * time.Second)(&o)
	assert.Equal(t, 9*time.Second, o.Timeout)

	WithConsistency(0x0001)(&o)
	assert.Equal(t, uint32(0x0001), o.Consistency)

	WithDisableInitialHostLookup(true)(&o)
	assert.True(t, o.DisableInitialHostLookup)
	WithIgnorePeerAddr(true)(&o)
	assert.True(t, o.IgnorePeerAddr)
}

// TestRows_NilSafe verifies Rows methods are no-ops on a nil/empty receiver,
// so callers that hold a nil *Rows (e.g. from a failed Query) don't panic.
func TestRows_NilSafe(t *testing.T) {
	var r *Rows
	assert.Nil(t, r.Columns())
	assert.False(t, r.Scan())
	assert.Nil(t, r.PageState())
	assert.Nil(t, r.Close())

	r2 := &Rows{iter: nil}
	assert.Nil(t, r2.Columns())
	assert.False(t, r2.Scan())
	assert.Nil(t, r2.PageState())
	assert.Nil(t, r2.Close())
}
