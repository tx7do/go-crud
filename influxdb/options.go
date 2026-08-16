package influxdb

import (
	"crypto/tls"
	"net/http"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/tx7do/go-wind/log"
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

// WithTLSConfig applies the caller's TLS configuration to the underlying
// HTTPClient transport. InfluxDB3's ClientConfig has no dedicated TLS field;
// TLS is configured on the http.Transport carried by HTTPClient. We install a
// transport (cloned from the default so other transport settings remain) with
// the caller's tls.Config, and assign it to HTTPClient so that influxdb3.New
// preserves it. The library's IdleConnectionTimeout/MaxIdleConnections are
// still applied to this transport afterward via configureHTTPClient.
func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(o *Client) {
		if tlsConfig == nil {
			return
		}
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsConfig
		o.options.HTTPClient = &http.Client{Transport: transport}
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

func WithLogger(logger log.Logger) Option {
	return func(o *Client) {
		log.SetLogger(logger)
	}
}
