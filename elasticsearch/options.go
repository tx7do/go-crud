package elasticsearch

import (
	"net/http"
	"time"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	elasticsearchV9 "github.com/elastic/go-elasticsearch/v9"
	"github.com/tx7do/go-crud/log"
)

type Option func(o *Client)

// addOpts appends library-level [elasticsearchV9.Option] values accumulated by
// the With* setters. They are forwarded to [elasticsearchV9.New] in
// NewElasticsearchClient, replacing the deprecated [elasticsearchV9.Config].
func (o *Client) addOpts(opts ...elasticsearchV9.Option) {
	o.esOpts = append(o.esOpts, opts...)
}

func WithAddresses(addresses ...string) Option {
	return func(o *Client) {
		o.addOpts(elasticsearchV9.WithAddresses(addresses...))
	}
}

func WithUsername(username string) Option {
	return func(o *Client) {
		o.pendingBasicAuth.username = username
		o.pendingBasicAuth.haveUser = true
	}
}

func WithPassword(password string) Option {
	return func(o *Client) {
		o.pendingBasicAuth.password = password
		o.pendingBasicAuth.havePass = true
	}
}

func WithEnableMetrics(enable bool) Option {
	return func(o *Client) {
		if enable {
			o.addOpts(elasticsearchV9.WithTransportOptions(elastictransport.WithMetrics()))
		}
	}
}

func WithEnableDebugLogger(enable bool) Option {
	return func(o *Client) {
		if enable {
			o.addOpts(elasticsearchV9.WithTransportOptions(elastictransport.WithDebugLogger()))
		}
	}
}
func WithEnableCompatibilityMode(enable bool) Option {
	return func(o *Client) {
		if enable {
			o.addOpts(elasticsearchV9.WithCompatibilityMode())
		}
	}
}

func WithDisableMetaHeader(disable bool) Option {
	return func(o *Client) {
		if disable {
			o.addOpts(elasticsearchV9.WithDisableMetaHeader())
		}
	}
}

func WithDiscoverNodesOnStart(enable bool) Option {
	return func(o *Client) {
		if enable {
			o.addOpts(elasticsearchV9.WithDiscoverNodesOnStart())
		}
	}
}
func WithDiscoverNodesInterval(interval time.Duration) Option {
	return func(o *Client) {
		o.addOpts(elasticsearchV9.WithTransportOptions(elastictransport.WithDiscoverNodesInterval(interval)))
	}
}

func WithDisableRetry(disable bool) Option {
	return func(o *Client) {
		if disable {
			o.addOpts(elasticsearchV9.WithTransportOptions(elastictransport.WithDisableRetry()))
		}
	}
}
func WithMaxRetries(maxRetries int) Option {
	return func(o *Client) {
		o.addOpts(elasticsearchV9.WithTransportOptions(elastictransport.WithMaxRetries(maxRetries)))
	}
}
func WithCompressRequestBody(enable bool) Option {
	return func(o *Client) {
		if enable {
			o.addOpts(elasticsearchV9.WithCompression())
		}
	}
}
func WithCompressRequestBodyLevel(level int) Option {
	return func(o *Client) {
		o.addOpts(elasticsearchV9.WithCompression(level))
	}
}
func WithPoolCompressor(enable bool) Option {
	return func(o *Client) {
		if enable {
			o.addOpts(elasticsearchV9.WithCompression())
		}
	}
}
func WithCloudID(cloudID string) Option {
	return func(o *Client) {
		o.addOpts(elasticsearchV9.WithCloudID(cloudID))
	}
}
func WithAPIKey(apiKey string) Option {
	return func(o *Client) {
		o.addOpts(elasticsearchV9.WithAPIKey(apiKey))
	}
}
func WithServiceToken(serviceToken string) Option {
	return func(o *Client) {
		o.addOpts(elasticsearchV9.WithServiceToken(serviceToken))
	}
}
func WithCertificateFingerprint(fingerprint string) Option {
	return func(o *Client) {
		o.addOpts(elasticsearchV9.WithCertificateFingerprint(fingerprint))
	}
}

func WithLogger(logger log.Logger) Option {
	return func(o *Client) {
		o.log = log.NewHelper(log.With(logger, "module", "elasticsearch-client"))
	}
}

func WithTransport(transport http.RoundTripper) Option {
	return func(o *Client) {
		o.addOpts(elasticsearchV9.WithTransportOptions(elastictransport.WithTransport(transport)))
	}
}

func WithRetryOnStatus(statuses ...int) Option {
	return func(o *Client) {
		o.addOpts(elasticsearchV9.WithTransportOptions(elastictransport.WithRetryOnStatus(statuses...)))
	}
}

func WithRetryBackoff(backoff func(attempt int) time.Duration) Option {
	return func(o *Client) {
		o.addOpts(elasticsearchV9.WithTransportOptions(elastictransport.WithRetryBackoff(backoff)))
	}
}

func WithCACert(cert []byte) Option {
	return func(o *Client) {
		o.addOpts(elasticsearchV9.WithCACert(cert))
	}
}

func WithHeader(header http.Header) Option {
	return func(o *Client) {
		o.addOpts(elasticsearchV9.WithTransportOptions(elastictransport.WithHeader(header)))
	}
}

func WithConnectionPoolFunc(fnc func([]*elastictransport.Connection, elastictransport.Selector) elastictransport.ConnectionPool) Option {
	return func(o *Client) {
		o.addOpts(elasticsearchV9.WithTransportOptions(elastictransport.WithConnectionPoolFunc(fnc)))
	}
}

func WithInterceptors(interceptors ...elastictransport.InterceptorFunc) Option {
	return func(o *Client) {
		o.addOpts(elasticsearchV9.WithTransportOptions(elastictransport.WithInterceptors(interceptors...)))
	}
}

func WithInstrumentation(instrumentation elastictransport.Instrumentation) Option {
	return func(o *Client) {
		o.addOpts(elasticsearchV9.WithInstrumentation(instrumentation))
	}
}

func WithSelector(selector elastictransport.Selector) Option {
	return func(o *Client) {
		o.addOpts(elasticsearchV9.WithTransportOptions(elastictransport.WithSelector(selector)))
	}
}

func WithAutoDrainBody(autoDrainBody bool) Option {
	return func(o *Client) {
		if autoDrainBody {
			o.addOpts(elasticsearchV9.WithAutoDrainBody())
		}
	}
}

func WithRetryOnError(retryOnError func(*http.Request, error) bool) Option {
	return func(o *Client) {
		o.addOpts(elasticsearchV9.WithTransportOptions(elastictransport.WithRetryOnError(retryOnError)))
	}
}
