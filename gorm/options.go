package gorm

import (
	"context"
	"time"

	"github.com/tx7do/go-wind/log"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
	"gorm.io/plugin/opentelemetry/tracing"
	"gorm.io/plugin/prometheus"
)

type Option func(*Client)

type Mixin func(*gorm.DB) error
type GetMigrateModelsFunc func() []any

func WithGormDB(db *gorm.DB) Option {
	return func(c *Client) {
		c.DB = db
	}
}

func WithDriverName(name string) Option {
	return func(c *Client) {
		c.driverName = name
	}
}

func WithDSN(dsn string) Option {
	return func(c *Client) {
		c.masterDSN = dsn
	}
}

func WithReplicaDsns(dsn []string) Option {
	return func(c *Client) {
		c.replicaDsns = dsn
	}
}

func WithEnableTrace(enable bool) Option {
	return func(c *Client) {
		c.enableTrace = enable
	}
}

func WithEnableMigrate(enable bool) Option {
	return func(c *Client) {
		c.enableMigrate = enable
	}
}

func WithEnableMetrics(enable bool) Option {
	return func(c *Client) {
		c.enableMetrics = enable
	}
}

func WithEnableDbResolver(enable bool) Option {
	return func(c *Client) {
		c.enableDbResolver = enable
	}
}

func WithGormConfig(cfg *gorm.Config) Option {
	return func(c *Client) {
		if cfg != nil {
			c.gormCfg = cfg
		}
	}
}

// WithMixin 将单个 mixin 转换为 Option
func WithMixin(m Mixin) Option {
	return func(c *Client) {
		c.mixins = append(c.mixins, m)
	}
}

// WithMixins 批量添加 mixin
func WithMixins(ms ...Mixin) Option {
	return func(c *Client) {
		c.mixins = append(c.mixins, ms...)
	}
}

// WithAutoMigrate 将 AutoMigrate 封装为 mixin，在 NewClient 时自动执行
func WithAutoMigrate(models ...any) Option {
	return func(c *Client) {
		c.mixins = append(c.mixins, func(db *gorm.DB) error {
			return db.AutoMigrate(models...)
		})
	}
}

// WithGetMigrateModels 注入一个返回迁移模型的函数，兼容内部注册的 getMigrateModels()
func WithGetMigrateModels(fn GetMigrateModelsFunc) Option {
	return func(c *Client) {
		c.getMigrateModels = fn
	}
}

func WithLogger(l log.Logger) Option {
	return func(c *Client) {
		c.gormCfg.Logger = NewGormLogger(l)
	}
}

// WithContext 将 context 注入 Client
func WithContext(ctx context.Context) Option {
	return func(c *Client) {
		c.ctx = ctx
	}
}

// WithBeforeOpen 在建立 gorm.DB 之前执行的回调（可用于修改 DSN、日志等）
func WithBeforeOpen(fn func(*gorm.DB) error) Option {
	return func(c *Client) {
		c.beforeOpen = append(c.beforeOpen, fn)
	}
}

// WithAfterOpen 在建立 gorm.DB 之后执行的回调（可用于初始化指标、注册事件等）
func WithAfterOpen(fn func(*gorm.DB) error) Option {
	return func(c *Client) {
		c.afterOpen = append(c.afterOpen, fn)
	}
}

func WithPrometheusConfig(cfg prometheus.Config) Option {
	return func(c *Client) {
		c.prometheusConfig = cfg
	}
}

func WithPrometheusDbName(dbName string) Option {
	return func(c *Client) {
		c.prometheusConfig.DBName = dbName
	}
}

func WithPrometheusPushAddr(pushAddr string) Option {
	return func(c *Client) {
		c.prometheusConfig.PushAddr = pushAddr
	}
}

func WithPrometheusHTTPServerPort(httpServerPort uint32) Option {
	return func(c *Client) {
		c.prometheusConfig.HTTPServerPort = httpServerPort
	}
}

func WithPrometheusRefreshInterval(refreshInterval uint32) Option {
	return func(c *Client) {
		c.prometheusConfig.RefreshInterval = refreshInterval
	}
}

func WithPrometheusPushAuth(user, password string) Option {
	return func(c *Client) {
		c.prometheusConfig.PushUser = user
		c.prometheusConfig.PushPassword = password
	}
}

func WithPrometheusStartServer(startServer bool) Option {
	return func(c *Client) {
		c.prometheusConfig.StartServer = startServer
	}
}

func WithPrometheusLabels(labels map[string]string) Option {
	return func(c *Client) {
		c.prometheusConfig.Labels = labels
	}
}

func WithMaxIdleConns(maxIdleConns int) Option {
	return func(c *Client) {
		c.maxIdleConns = &maxIdleConns
	}
}

func WithMaxOpenConns(maxOpenConns int) Option {
	return func(c *Client) {
		c.maxOpenConns = &maxOpenConns
	}
}

func WithConnMaxLifetime(connMaxLifetime time.Duration) Option {
	return func(c *Client) {
		c.connMaxLifetime = &connMaxLifetime
	}
}

func WithTracingOptions(opts ...tracing.Option) Option {
	return func(c *Client) {
		c.tracingOption = append(c.tracingOption, opts...)
	}
}

func WithTracerProvider(provider trace.TracerProvider) Option {
	return func(c *Client) {
		c.tracingOption = append(c.tracingOption, tracing.WithTracerProvider(provider))
	}
}

func WithTracingAttributes(attrs ...attribute.KeyValue) Option {
	return func(c *Client) {
		c.tracingOption = append(c.tracingOption, tracing.WithAttributes(attrs...))
	}
}

func WithTracingDBSystem(name string) Option {
	return func(c *Client) {
		c.tracingOption = append(c.tracingOption, tracing.WithAttributes(semconv.DBSystemNameKey.String(name)))
	}
}

func WithTracingWithoutMetrics() Option {
	return func(c *Client) {
		c.tracingOption = append(c.tracingOption, tracing.WithoutMetrics())
	}
}

func WithTracingWithoutServerAddress() Option {
	return func(c *Client) {
		c.tracingOption = append(c.tracingOption, tracing.WithoutServerAddress())
	}
}
