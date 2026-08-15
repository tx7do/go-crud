package mongodb

import (
	"crypto/tls"
	"time"

	"github.com/tx7do/go-crud/log"
	optionsV2 "go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Option func(o *Client)

func WithURI(uri string) Option {
	return func(o *Client) {
		o.options = append(o.options, optionsV2.Client().ApplyURI(uri))
	}
}

func WithDatabase(database string) Option {
	return func(o *Client) {
		o.database = database
	}
}

func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(o *Client) {
		o.options = append(o.options, optionsV2.Client().SetTLSConfig(tlsConfig))
	}
}

func WithLogger(logger log.Logger) Option {
	return func(o *Client) {
		o.log = log.NewHelper(log.With(logger, "module", "mongodb-client"))
	}
}

func WithTimeout(timeout time.Duration) Option {
	return func(o *Client) {
		o.options = append(o.options, optionsV2.Client().SetTimeout(timeout))
	}
}

func WithConnectTimeout(connectTimeout time.Duration) Option {
	return func(o *Client) {
		o.options = append(o.options, optionsV2.Client().SetConnectTimeout(connectTimeout))
	}
}

func WithServerSelectionTimeout(serverSelectionTimeout time.Duration) Option {
	return func(o *Client) {
		o.options = append(o.options, optionsV2.Client().SetServerSelectionTimeout(serverSelectionTimeout))
	}
}

func WithHeartbeatInterval(interval time.Duration) Option {
	return func(o *Client) {
		o.options = append(o.options, optionsV2.Client().SetHeartbeatInterval(interval))
	}
}

func WithLocalThreshold(threshold time.Duration) Option {
	return func(o *Client) {
		o.options = append(o.options, optionsV2.Client().SetLocalThreshold(threshold))
	}
}

func WithMaxConnIdleTime(maxIdleTime time.Duration) Option {
	return func(o *Client) {
		o.options = append(o.options, optionsV2.Client().SetMaxConnIdleTime(maxIdleTime))
	}
}

func WithCredentials(username, password string) Option {
	return func(o *Client) {
		if username != "" && password != "" {
			credential := optionsV2.Credential{
				Username: username,
				Password: password,
			}

			if password != "" {
				credential.PasswordSet = true
			}

			o.options = append(o.options, optionsV2.Client().SetAuth(credential))
		}
	}
}

func WithBSONOptions(opt *optionsV2.BSONOptions) Option {
	return func(o *Client) {
		o.options = append(o.options, optionsV2.Client().SetBSONOptions(opt))
	}
}
