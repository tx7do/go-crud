package weaviate

import (
	"net/http"

	wvc "github.com/weaviate/weaviate-go-client/v4/weaviate"
)

type Option func(o *Client)

func WithHost(host string) Option {
	return func(o *Client) {
		o.host = host
	}
}

func WithScheme(scheme string) Option {
	return func(o *Client) {
		o.scheme = scheme
	}
}

// WithHeaders sets headers added to every request (e.g. authorization).
func WithHeaders(headers map[string]string) Option {
	return func(o *Client) {
		o.headers = headers
	}
}

// WithHTTPClient injects a custom HTTP client (used to override the
// transport in tests).
func WithHTTPClient(hc *http.Client) Option {
	return func(o *Client) {
		o.httpClient = hc
	}
}

// WithWeaviateClient injects a pre-built weaviate client (used in tests).
func WithWeaviateClient(cli *wvc.Client) Option {
	return func(o *Client) {
		o.cli = cli
	}
}
