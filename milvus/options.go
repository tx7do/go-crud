package milvus

import (
	"github.com/milvus-io/milvus-sdk-go/v2/client"
)

type Option func(o *Client)

func WithAddress(address string) Option {
	return func(o *Client) {
		o.address = address
	}
}

// WithUsername sets the username for authentication.
func WithUsername(username string) Option {
	return func(o *Client) {
		o.username = username
	}
}

// WithPassword sets the password for authentication.
func WithPassword(password string) Option {
	return func(o *Client) {
		o.password = password
	}
}

// WithAPIKey sets the API key for authentication.
func WithAPIKey(apiKey string) Option {
	return func(o *Client) {
		o.apiKey = apiKey
	}
}

// WithDBName selects the database for this client.
func WithDBName(dbName string) Option {
	return func(o *Client) {
		o.dbName = dbName
	}
}

// WithMilvusClient injects a pre-built milvus client (used in tests).
func WithMilvusClient(cli client.Client) Option {
	return func(o *Client) {
		o.cli = cli
	}
}
