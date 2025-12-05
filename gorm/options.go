package gorm

import (
	"context"

	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm"
)

type Option func(*Client)

type Mixin func(*gorm.DB) error
type GetMigrateModelsFunc func() []interface{}
type RawOptions map[string]interface{}

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
		c.dsn = dsn
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
func WithAutoMigrate(models ...interface{}) Option {
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

func WithLogger(l *log.Helper) Option {
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

// WithConfigStruct 注入任意配置结构体（例如从 config 解码后的结构）
func WithConfigStruct(cfg interface{}) Option {
	return func(c *Client) {
		c.cfgStruct = cfg
	}
}

// WithEnvPrefix 指定从环境变量读取配置时的前缀（可在 NewClient 中解析）
func WithEnvPrefix(prefix string) Option {
	return func(c *Client) {
		c.envPrefix = prefix
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

// WithRawOptions 注入任意键值参数供 Client 或 mixin 使用
func WithRawOptions(m RawOptions) Option {
	return func(c *Client) {
		if c.rawOptions == nil {
			c.rawOptions = make(RawOptions)
		}
		for k, v := range m {
			c.rawOptions[k] = v
		}
	}
}
