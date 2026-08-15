package cassandra

import (
	"crypto/tls"
	"time"

	"github.com/tx7do/go-crud/log"
)

type options struct {
	Hosts []string

	Username string
	Password string

	Keyspace string

	TLSConfig *tls.Config
	Logger    *log.Helper

	ConnectTimeout time.Duration
	Timeout        time.Duration

	Consistency uint32

	DisableInitialHostLookup bool
	IgnorePeerAddr           bool
}

type Option func(o *options)

func WithLogger(logger log.Logger) Option {
	return func(o *options) {
		o.Logger = log.NewHelper(log.With(logger, "module", "cassandra-client"))
	}
}

func WithHosts(hosts ...string) Option {
	return func(o *options) {
		o.Hosts = hosts
	}
}

func WithUsername(username string) Option {
	return func(o *options) {
		o.Username = username
	}
}

func WithPassword(password string) Option {
	return func(o *options) {
		o.Password = password
	}
}

func WithKeyspace(keyspace string) Option {
	return func(o *options) {
		o.Keyspace = keyspace
	}
}

func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(o *options) {
		o.TLSConfig = tlsConfig
	}
}

func WithConnectTimeout(timeout time.Duration) Option {
	return func(o *options) {
		o.ConnectTimeout = timeout
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(o *options) {
		o.Timeout = timeout
	}
}

func WithConsistency(consistency uint32) Option {
	return func(o *options) {
		o.Consistency = consistency
	}
}

func WithDisableInitialHostLookup(disable bool) Option {
	return func(o *options) {
		o.DisableInitialHostLookup = disable
	}
}

func WithIgnorePeerAddr(ignore bool) Option {
	return func(o *options) {
		o.IgnorePeerAddr = ignore
	}
}
