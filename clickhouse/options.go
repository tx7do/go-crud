package clickhouse

import (
	"crypto/tls"
	"net/url"
	"time"

	clickhouseV2 "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/go-kratos/kratos/v2/log"
)

type Creator func() any

var compressionMap = map[string]clickhouseV2.CompressionMethod{
	"none":    clickhouseV2.CompressionNone,
	"zstd":    clickhouseV2.CompressionZSTD,
	"lz4":     clickhouseV2.CompressionLZ4,
	"lz4hc":   clickhouseV2.CompressionLZ4HC,
	"gzip":    clickhouseV2.CompressionGZIP,
	"deflate": clickhouseV2.CompressionDeflate,
	"br":      clickhouseV2.CompressionBrotli,
}

type Option func(o *Client)

func WithLogger(logger log.Logger) Option {
	return func(o *Client) {
		o.logger = log.NewHelper(log.With(logger, "module", "clickhouse-client"))
	}
}

func WithOptions(opts *clickhouseV2.Options) Option {
	return func(o *Client) {
		if opts == nil {
			return
		}
		o.options = opts
	}
}

func WithDsn(dsn string) Option {
	return func(o *Client) {
		tmp, err := clickhouseV2.ParseDSN(dsn)
		if err != nil {
			return
		}

		o.options = tmp
	}
}

func WithHttpProxy(httpProxy string) Option {
	return func(o *Client) {
		proxyURL, err := url.Parse(httpProxy)
		if err != nil {
			return
		}

		o.options.HTTPProxyURL = proxyURL
	}
}

func WithScheme(scheme string) Option {
	return func(o *Client) {
		switch scheme {
		case "http":
			o.options.Protocol = clickhouseV2.HTTP
		case "https":
			o.options.Protocol = clickhouseV2.HTTP
		default:
			o.options.Protocol = clickhouseV2.Native
		}
	}
}

func WithAddresses(addresses ...string) Option {
	return func(o *Client) {
		o.options.Addr = addresses
	}
}

func WithUsername(username string) Option {
	return func(o *Client) {
		o.options.Auth.Username = username
	}
}

func WithPassword(password string) Option {
	return func(o *Client) {
		o.options.Auth.Password = password
	}
}

func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(o *Client) {
		//o.TLS = tlsConfig
	}
}

func WithDatabase(database string) Option {
	return func(o *Client) {
		o.options.Auth.Database = database
	}
}

func WithDebug(debug bool) Option {
	return func(o *Client) {
		o.options.Debug = debug
	}
}

func WithDebugMode(debug bool) Option {
	return func(o *Client) {
		o.options.Debug = debug
	}
}

func WithEnableTracing(enableTracing bool) Option {
	return func(o *Client) {
		//o.options.EnableTracing = enableTracing
	}
}

func WithEnableMetrics(enableMetrics bool) Option {
	return func(o *Client) {
		//o.options.EnableMetrics = enableMetrics
	}
}

func WithDialTimeout(dialTimeout time.Duration) Option {
	return func(o *Client) {
		o.options.DialTimeout = dialTimeout
	}
}

func WithReadTimeout(readTimeout time.Duration) Option {
	return func(o *Client) {
		o.options.ReadTimeout = readTimeout
	}
}

func WithConnMaxLifetime(connMaxLifetime time.Duration) Option {
	return func(o *Client) {
		o.options.ConnMaxLifetime = connMaxLifetime
	}
}

func WithMaxIdleConns(maxIdleConns int) Option {
	return func(o *Client) {
		o.options.MaxIdleConns = maxIdleConns
	}
}

func WithMaxOpenConns(maxOpenConns int) Option {
	return func(o *Client) {
		o.options.MaxOpenConns = maxOpenConns
	}
}

func WithBlockBufferSize(blockBufferSize uint8) Option {
	return func(o *Client) {
		o.options.BlockBufferSize = blockBufferSize
	}
}

func WithCompressionMethod(compressionMethod string) Option {
	return func(o *Client) {
		if o.options.Compression == nil {
			o.options.Compression = &clickhouseV2.Compression{}
		}
		if compressionMethod != "" {
			o.options.Compression.Method = compressionMap[compressionMethod]
		}
	}
}
func WithCompressionLevel(compressionLevel int) Option {
	return func(o *Client) {
		if o.options.Compression == nil {
			o.options.Compression = &clickhouseV2.Compression{}
		}
		o.options.Compression.Level = compressionLevel
	}
}
func WithMaxCompressionBuffer(maxCompressionBuffer int) Option {
	return func(o *Client) {
		o.options.MaxCompressionBuffer = maxCompressionBuffer
	}
}
func WithConnectionOpenStrategy(connectionOpenStrategy string) Option {
	return func(o *Client) {
		strategy := clickhouseV2.ConnOpenInOrder
		switch connectionOpenStrategy {
		case "in_order":
			strategy = clickhouseV2.ConnOpenInOrder
		case "round_robin":
			strategy = clickhouseV2.ConnOpenRoundRobin
		case "random":
			strategy = clickhouseV2.ConnOpenRandom
		}
		o.options.ConnOpenStrategy = strategy
	}
}
