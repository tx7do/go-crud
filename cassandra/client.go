package cassandra

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
	"github.com/tx7do/go-wind/log"
)

// Client wraps a gocql.Session. It applies connection configuration supplied
// via Option values and exposes typed query/batch helpers. The zero value is
// not usable; obtain an instance via NewCassandraClient.
type Client struct {
	session *gocql.Session
}

// NewCassandraClient builds a Client by applying the given options to a
// gocql.ClusterConfig and opening a session. On failure it returns a nil
// client and the underlying error.
func NewCassandraClient(opts ...Option) (*Client, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	clusterConfig := gocql.NewCluster(o.Hosts...)

	clusterConfig.Authenticator = gocql.PasswordAuthenticator{
		Username: o.Username,
		Password: o.Password,
	}

	clusterConfig.Keyspace = o.Keyspace

	// TLS: gocql enables TLS via SslOpts (which embeds *tls.Config), not a
	// dedicated TLSConfig field. See gocql doc.go and conn.go SslOptions.
	if o.TLSConfig != nil {
		clusterConfig.SslOpts = &gocql.SslOptions{Config: o.TLSConfig}
	}

	clusterConfig.ConnectTimeout = o.ConnectTimeout
	clusterConfig.Timeout = o.Timeout

	clusterConfig.Consistency = gocql.Consistency(o.Consistency)

	clusterConfig.DisableInitialHostLookup = o.DisableInitialHostLookup
	clusterConfig.IgnorePeerAddr = o.IgnorePeerAddr

	session, err := clusterConfig.CreateSession()
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed opening connection to cassandra: %v", err))
		return nil, err
	}

	return &Client{session: session}, nil
}

// Close releases the underlying session. After this call the client is unusable.
func (c *Client) Close() {
	if c != nil && c.session != nil {
		c.session.Close()
	}
}

// Closed reports whether the underlying session has been closed.
func (c *Client) Closed() bool {
	if c == nil || c.session == nil {
		return true
	}
	return c.session.Closed()
}

// Session returns the wrapped gocql.Session for callers that need direct
// access to gocql APIs not covered by this wrapper.
func (c *Client) Session() *gocql.Session {
	return c.session
}

// Exec executes a statement that returns no rows (INSERT/UPDATE/DELETE).
func (c *Client) Exec(ctx context.Context, stmt string, args ...any) error {
	if c.session == nil || c.session.Closed() {
		return ErrSessionClosed
	}
	if stmt == "" {
		return ErrInvalidRequest
	}
	q := c.session.Query(stmt, args...).WithContext(ctx)
	if err := q.Exec(); err != nil {
		log.Error(ctx, fmt.Sprintf("exec query failed: %v", err))
		return ErrExecQuery
	}
	return nil
}

// Query executes a statement that returns rows and wraps the iterator in *Rows.
// The caller MUST call Rows.Close() when done to release the connection.
func (c *Client) Query(ctx context.Context, stmt string, args ...any) (*Rows, error) {
	if c.session == nil || c.session.Closed() {
		return nil, ErrSessionClosed
	}
	if stmt == "" {
		return nil, ErrInvalidRequest
	}
	iter := c.session.Query(stmt, args...).WithContext(ctx).Iter()
	return &Rows{iter: iter}, nil
}

// ExecBatch executes a batch of non-bound statements. stmts and argsList must
// be the same length; each pair becomes one batch entry.
func (c *Client) ExecBatch(ctx context.Context, batchType gocql.BatchType, stmts []string, argsList [][]any) error {
	if c.session == nil || c.session.Closed() {
		return ErrSessionClosed
	}
	if len(stmts) == 0 {
		return ErrInvalidRequest
	}
	_ = ctx
	batch := c.session.NewBatch(batchType)
	for i, stmt := range stmts {
		if stmt == "" {
			return ErrInvalidRequest
		}
		batch.Query(stmt, argsList[i]...)
	}
	if err := c.session.ExecuteBatch(batch); err != nil {
		log.Error(context.Background(), fmt.Sprintf("exec batch failed: %v", err))
		return ErrExecQuery
	}
	return nil
}

// ExecBatchBound executes a batch of bound statements. stmts and bindings must
// be the same length; each binding produces the arguments for its statement.
func (c *Client) ExecBatchBound(ctx context.Context, batchType gocql.BatchType, stmts []string, bindings []func(*gocql.QueryInfo) ([]any, error)) error {
	if c.session == nil || c.session.Closed() {
		return ErrSessionClosed
	}
	if len(stmts) == 0 {
		return ErrInvalidRequest
	}
	_ = ctx
	batch := c.session.NewBatch(batchType)
	for i, stmt := range stmts {
		if stmt == "" {
			return ErrInvalidRequest
		}
		batch.Bind(stmt, bindings[i])
	}
	if err := c.session.ExecuteBatch(batch); err != nil {
		log.Error(context.Background(), fmt.Sprintf("exec batch (bound) failed: %v", err))
		return ErrExecQuery
	}
	return nil
}
