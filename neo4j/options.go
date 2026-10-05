package neo4j

import (
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

type Option func(o *Client)

func WithURI(uri string) Option {
	return func(o *Client) {
		o.uri = uri
	}
}

func WithBasicAuth(username, password, realm string) Option {
	return func(o *Client) {
		o.auth = neo4j.BasicAuth(username, password, realm)
	}
}

// WithNeo4jDriver injects a pre-built neo4j driver (used in tests).
func WithNeo4jDriver(drv neo4j.DriverWithContext) Option {
	return func(o *Client) {
		o.drv = drv
	}
}
