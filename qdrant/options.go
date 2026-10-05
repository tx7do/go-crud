package qdrant

import (
	"crypto/tls"

	qdrant "github.com/qdrant/go-client/qdrant"
)

type Option func(o *Client)

func WithHost(host string) Option {
	return func(o *Client) {
		o.host = host
	}
}

func WithPort(port int) Option {
	return func(o *Client) {
		o.port = port
	}
}

// WithAPIKey sets the API key for authentication.
func WithAPIKey(apiKey string) Option {
	return func(o *Client) {
		o.apiKey = apiKey
	}
}

// WithTLS enables TLS for the connection.
func WithTLS(useTLS bool) Option {
	return func(o *Client) {
		o.useTLS = useTLS
	}
}

// WithTLSConfig sets a custom TLS configuration.
func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(o *Client) {
		o.tlsConfig = tlsConfig
	}
}

// WithQdrantClient injects a pre-built qdrant client (used in tests).
func WithQdrantClient(cli *qdrant.Client) Option {
	return func(o *Client) {
		o.cli = cli
	}
}
