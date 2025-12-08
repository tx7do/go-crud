package clickhouse

import (
	"crypto/tls"
	"time"

	"github.com/go-kratos/kratos/v2/log"
)

type options struct {
	Addresses []string `json:"addresses,omitempty"` // 对端网络地址

	Database string `json:"database,omitempty"` // 数据库名
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`

	Debug  bool   `json:"debug,omitempty"`  // 调试开关
	Scheme string `json:"scheme,omitempty"` // 协议：http、https、native

	TLS *tls.Config `json:"tls,omitempty"` // TLS 配置

	BlockBufferSize *int `json:"block_buffer_size,omitempty"` // 数据块缓冲区大小

	CompressionMethod    string `json:"compression_method,omitempty"`     // 压缩方法：zstd、lz4、lz4hc、gzip、deflate、br、none
	CompressionLevel     *int   `json:"compression_level,omitempty"`      // 压缩级别：0-9
	MaxCompressionBuffer *int   `json:"max_compression_buffer,omitempty"` // 最大压缩缓冲区大小

	ConnectionOpenStrategy string `json:"connection_open_strategy,omitempty"` // in_order、round_robin、random

	DialTimeout     *time.Duration `json:"dial_timeout,omitempty"`
	ReadTimeout     *time.Duration `json:"read_timeout,omitempty"`
	ConnMaxLifetime *time.Duration `json:"conn_max_lifetime,omitempty"`

	MaxIdleConns int `json:"max_idle_conns,omitempty"`
	MaxOpenConns int `json:"max_open_conns,omitempty"`

	Dsn       string `json:"dsn,omitempty"`        // 数据源名称（DSN字符串）
	HttpProxy string `json:"http_proxy,omitempty"` // HTTP代理地址

	EnableTracing bool `json:"enable_tracing,omitempty"`
	EnableMetrics bool `json:"enable_metrics,omitempty"`

	Logger *log.Helper
}

type Option func(o *options)

func WithLogger(logger log.Logger) Option {
	return func(o *options) {
		o.Logger = log.NewHelper(log.With(logger, "module", "clickhouse-client"))
	}
}

func WithDsn(dsn string) Option {
	return func(o *options) {
		o.Dsn = dsn
	}
}

func WithHttpProxy(httpProxy string) Option {
	return func(o *options) {
		o.HttpProxy = httpProxy
	}
}

func WithScheme(scheme string) Option {
	return func(o *options) {
		o.Scheme = scheme
	}
}

func WithAddresses(addresses ...string) Option {
	return func(o *options) {
		o.Addresses = addresses
	}
}

func WithUsername(username string) Option {
	return func(o *options) {
		o.Username = username
	}
}

func WithPassword(password string) Option {
	return func(o *options) {
		o.Password = password
	}
}

func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(o *options) {
		o.TLS = tlsConfig
	}
}

func WithDatabase(database string) Option {
	return func(o *options) {
		o.Database = database
	}
}

func WithDebug(debug bool) Option {
	return func(o *options) {
		o.Debug = debug
	}
}

func WithDebugMode(debug bool) Option {
	return func(o *options) {
		o.Debug = debug
	}
}

func WithEnableTracing(enableTracing bool) Option {
	return func(o *options) {
		o.EnableTracing = enableTracing
	}
}

func WithEnableMetrics(enableMetrics bool) Option {
	return func(o *options) {
		o.EnableMetrics = enableMetrics
	}
}

func WithDialTimeout(dialTimeout time.Duration) Option {
	return func(o *options) {
		o.DialTimeout = &dialTimeout
	}
}

func WithReadTimeout(readTimeout time.Duration) Option {
	return func(o *options) {
		o.ReadTimeout = &readTimeout
	}
}

func WithConnMaxLifetime(connMaxLifetime time.Duration) Option {
	return func(o *options) {
		o.ConnMaxLifetime = &connMaxLifetime
	}
}

func WithMaxIdleConns(maxIdleConns int) Option {
	return func(o *options) {
		o.MaxIdleConns = maxIdleConns
	}
}

func WithMaxOpenConns(maxOpenConns int) Option {
	return func(o *options) {
		o.MaxOpenConns = maxOpenConns
	}
}

func WithBlockBufferSize(blockBufferSize int) Option {
	return func(o *options) {
		o.BlockBufferSize = &blockBufferSize
	}
}

func WithCompressionMethod(compressionMethod string) Option {
	return func(o *options) {
		o.CompressionMethod = compressionMethod
	}
}
func WithCompressionLevel(compressionLevel int) Option {
	return func(o *options) {
		o.CompressionLevel = &compressionLevel
	}
}
func WithMaxCompressionBuffer(maxCompressionBuffer int) Option {
	return func(o *options) {
		o.MaxCompressionBuffer = &maxCompressionBuffer
	}
}
func WithConnectionOpenStrategy(connectionOpenStrategy string) Option {
	return func(o *options) {
		o.ConnectionOpenStrategy = connectionOpenStrategy
	}
}
