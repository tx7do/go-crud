package elasticsearch

import (
	"net/http"
	"time"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	"github.com/tx7do/go-crud/log"
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

func WithTransport(transport http.RoundTripper) Option {
	return func(o *Client) {
		o.options.Transport = transport
	}
}

func WithRetryOnStatus(statuses ...int) Option {
	return func(o *Client) {
		o.options.RetryOnStatus = statuses
	}
}

func WithRetryBackoff(backoff func(attempt int) time.Duration) Option {
	return func(o *Client) {
		o.options.RetryBackoff = backoff
	}
}

func WithCACert(cert []byte) Option {
	return func(o *Client) {
		o.options.CACert = cert
	}
}

func WithHeader(header http.Header) Option {
	return func(o *Client) {
		o.options.Header = header
	}
}

func WithConnectionPoolFunc(fnc func([]*elastictransport.Connection, elastictransport.Selector) elastictransport.ConnectionPool) Option {
	return func(o *Client) {
		o.options.ConnectionPoolFunc = fnc
	}
}

func WithInterceptors(interceptors ...elastictransport.InterceptorFunc) Option {
	return func(o *Client) {
		o.options.Interceptors = append(o.options.Interceptors, interceptors...)
	}
}

func WithInstrumentation(instrumentation elastictransport.Instrumentation) Option {
	return func(o *Client) {
		o.options.Instrumentation = instrumentation
	}
}

func WithSelector(selector elastictransport.Selector) Option {
	return func(o *Client) {
		o.options.Selector = selector
	}
}

func WithAutoDrainBody(autoDrainBody bool) Option {
	return func(o *Client) {
		o.options.AutoDrainBody = autoDrainBody
	}
}

func WithRetryOnError(retryOnError func(*http.Request, error) bool) Option {
	return func(o *Client) {
		o.options.RetryOnError = retryOnError
	}
}
