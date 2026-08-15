package doris

import (
	"net/http"
	"time"

	"github.com/tx7do/go-crud/log"
	"github.com/jmoiron/sqlx"
)

type Option func(o *Client)

// WithDSN sets the data source name used to open the DB connection.
func WithDSN(dsn string) Option {
	return func(o *Client) { o.dsn = dsn }
}

// WithDB injects an existing *sqlx.DB into the client (skips Open).
func WithDB(db *sqlx.DB) Option {
	return func(o *Client) { o.db = db }
}

// WithLogger attaches a log helper to the client.
func WithLogger(l *log.Helper) Option {
	return func(o *Client) { o.log = l }
}

// WithMaxOpenConns sets maximum open connections.
func WithMaxOpenConns(n int) Option {
	return func(o *Client) { o.maxOpenConns = n }
}

// WithMaxIdleConns sets maximum idle connections.
func WithMaxIdleConns(n int) Option {
	return func(o *Client) { o.maxIdleConns = n }
}

// WithConnMaxLifetime sets maximum connection lifetime.
func WithConnMaxLifetime(d time.Duration) Option {
	return func(o *Client) { o.connMaxLifetime = d }
}

// Stream Load related options

// WithStreamLoadEndpoint sets the FE host (including scheme and port) for stream load, e.g. http://fe-host:8030
func WithStreamLoadEndpoint(endpoint string) Option {
	return func(o *Client) { o.streamLoadEndpoint = endpoint }
}

// WithStreamLoadAuth sets basic auth credentials for stream load (username, password)
func WithStreamLoadAuth(username, password string) Option {
	return func(o *Client) { o.streamLoadUser = username; o.streamLoadPass = password }
}

// WithHTTPClient injects an existing http.Client to use for stream load requests
func WithHTTPClient(hc *http.Client) Option {
	return func(o *Client) { o.httpClient = hc }
}

// WithStreamLoadTimeout sets timeout for stream load HTTP requests
func WithStreamLoadTimeout(d time.Duration) Option {
	return func(o *Client) { o.streamLoadTimeout = d }
}

// WithStreamLoadMethod sets HTTP method for stream load ("POST" or "PUT")
func WithStreamLoadMethod(method string) Option {
	return func(o *Client) { o.streamLoadMethod = method }
}
