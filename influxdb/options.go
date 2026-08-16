package influxdb

import (
	"crypto/tls"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
)

type Option func(o *Client)

func WithOptions(opts *influxdb3.ClientConfig) Option {
	return func(o *Client) {
		if opts == nil {
			return
		}
		o.options = opts
	}
}

func WithHost(host string) Option {
	return func(o *Client) {
		o.options.Host = host
	}
}

func WithToken(token string) Option {
	return func(o *Client) {
		o.options.Token = token
	}
}

func WithOrganization(organization string) Option {
	return func(o *Client) {
		o.options.Organization = organization
	}
}

func WithDatabase(database string) Option {
	return func(o *Client) {
		o.options.Database = database
	}
}

func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(o *Client) {
		//o.options.TLS = tlsConfig
	}
}

func WithWriteTimeout(timeout time.Duration) Option {
	return func(o *Client) {
		o.options.WriteTimeout = timeout
	}
}

func WithQueryTimeout(timeout time.Duration) Option {
	return func(o *Client) {
		o.options.QueryTimeout = timeout
	}
}

func WithIdleConnectionTimeout(idleTimeout time.Duration) Option {
	return func(o *Client) {
		o.options.IdleConnectionTimeout = idleTimeout
	}
}
func WithMaxIdleConnections(maxIdle int) Option {
	return func(o *Client) {
		o.options.MaxIdleConnections = maxIdle
	}
}

func WithAuthScheme(authScheme string) Option {
	return func(o *Client) {
		o.options.AuthScheme = authScheme
	}
}
