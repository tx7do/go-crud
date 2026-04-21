package opensearch

import (
	"net/http"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/opensearch-project/opensearch-go/v4/opensearchtransport"
	"github.com/opensearch-project/opensearch-go/v4/signer"
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

func WithEnableRetryOnTimeout(enable bool) Option {
	return func(o *Client) {
		o.options.EnableRetryOnTimeout = enable
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

func WithLogger(logger log.Logger) Option {
	return func(o *Client) {
		o.log = log.NewHelper(log.With(logger, "module", "opensearch-client"))
	}
}

func WithTransport(transport http.RoundTripper) Option {
	return func(o *Client) {
		o.options.Transport = transport
	}
}

func WithSelector(selector opensearchtransport.Selector) Option {
	return func(o *Client) {
		o.options.Selector = selector
	}
}

func WithConnectionPoolFunc(fnc func([]*opensearchtransport.Connection, opensearchtransport.Selector) opensearchtransport.ConnectionPool) Option {
	return func(o *Client) {
		o.options.ConnectionPoolFunc = fnc
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

func WithSigner(signer signer.Signer) Option {
	return func(o *Client) {
		o.options.Signer = signer
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
