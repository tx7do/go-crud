package elasticsearch

import (
	"crypto/tls"
	"time"

	"github.com/go-kratos/kratos/v2/log"
)

type Option func(o *Client)

func WithAddresses(addresses ...string) Option {
	return func(o *Client) {
		o.options.Addresses = addresses
	}
}

func WithUsername(username string) Option {
	return func(o *Client) {
		o.options.Username = username
	}
}

func WithPassword(password string) Option {
	return func(o *Client) {
		o.options.Password = password
	}
}

func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(o *Client) {
		//o.options.TLS = tlsConfig
	}
}

func WithEnableMetrics(enable bool) Option {
	return func(o *Client) {
		o.options.EnableMetrics = enable
	}
}

func WithEnableDebugLogger(enable bool) Option {
	return func(o *Client) {
		o.options.EnableDebugLogger = enable
	}
}
func WithEnableCompatibilityMode(enable bool) Option {
	return func(o *Client) {
		o.options.EnableCompatibilityMode = enable
	}
}

func WithDisableMetaHeader(disable bool) Option {
	return func(o *Client) {
		o.options.DisableMetaHeader = disable
	}
}

func WithDiscoverNodesOnStart(enable bool) Option {
	return func(o *Client) {
		o.options.DiscoverNodesOnStart = enable
	}
}
func WithDiscoverNodesInterval(interval time.Duration) Option {
	return func(o *Client) {
		o.options.DiscoverNodesInterval = interval
	}
}

func WithDisableRetry(disable bool) Option {
	return func(o *Client) {
		o.options.DisableRetry = disable
	}
}
func WithMaxRetries(maxRetries int) Option {
	return func(o *Client) {
		o.options.MaxRetries = maxRetries
	}
}
func WithCompressRequestBody(enable bool) Option {
	return func(o *Client) {
		o.options.CompressRequestBody = enable
	}
}
func WithCompressRequestBodyLevel(level int) Option {
	return func(o *Client) {
		o.options.CompressRequestBodyLevel = level
	}
}
func WithPoolCompressor(enable bool) Option {
	return func(o *Client) {
		o.options.PoolCompressor = enable
	}
}
func WithCloudID(cloudID string) Option {
	return func(o *Client) {
		o.options.CloudID = cloudID
	}
}
func WithAPIKey(apiKey string) Option {
	return func(o *Client) {
		o.options.APIKey = apiKey
	}
}
func WithServiceToken(serviceToken string) Option {
	return func(o *Client) {
		o.options.ServiceToken = serviceToken
	}
}
func WithCertificateFingerprint(fingerprint string) Option {
	return func(o *Client) {
		o.options.CertificateFingerprint = fingerprint
	}
}

func WithLogger(logger log.Logger) Option {
	return func(o *Client) {
		o.log = log.NewHelper(log.With(logger, "module", "elasticsearch-client"))
	}
}
