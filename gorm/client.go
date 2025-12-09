package gorm

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kratos/kratos/v2/log"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"gorm.io/plugin/opentelemetry/tracing"

	goSqlite "github.com/glebarez/sqlite"
	"gorm.io/driver/bigquery"
	"gorm.io/driver/clickhouse"
	"gorm.io/driver/gaussdb"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/driver/sqlserver"
)

type gormLoggerWriter struct {
	helper *log.Helper
}

func (w gormLoggerWriter) Printf(format string, args ...interface{}) {
	w.helper.Debugf(format, args...)
}

func NewGormLogger(l *log.Helper) logger.Interface {
	w := gormLoggerWriter{helper: l}
	return logger.New(
		w,
		logger.Config{
			SlowThreshold: time.Millisecond * 100, // 慢 SQL 阈值（超过 100ms 标为慢 SQL）
			LogLevel:      logger.Info,            // 核心：Info 级别会打印所有 SQL
			Colorful:      true,                   // 终端彩色输出（文件输出需关闭）
		},
	)
}

// Client GORM 客户端
type Client struct {
	*gorm.DB

	// 基础配置
	driverName    string
	dsn           string
	enableTrace   bool
	enableMigrate bool
	enableMetrics bool
	migrateModels []interface{}
	gormCfg       *gorm.Config

	mixins           []Mixin
	getMigrateModels GetMigrateModelsFunc

	ctx       context.Context
	cfgStruct interface{}
	envPrefix string

	// 钩子
	beforeOpen []func(*gorm.DB) error
	afterOpen  []func(*gorm.DB) error

	// 任意原始选项
	rawOptions RawOptions

	// logger helper
	logger *log.Helper
}

// NewClient 创建 GORM 客户端
func NewClient(opts ...Option) (*Client, error) {
	c := &Client{
		ctx:     context.Background(),
		mixins:  make([]Mixin, 0),
		gormCfg: &gorm.Config{},
	}

	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}

	// 如果没有外部传入 DB，则尝试根据 driverName/dsn 创建
	if c.DB == nil {
		if c.driverName == "" || c.dsn == "" {
			return nil, fmt.Errorf("gorm DB not provided; either use WithGormDB or provide driverName/dsn")
		}
		if err := c.createGormClient(); err != nil {
			return nil, err
		}
	}

	for _, fn := range c.beforeOpen {
		if fn == nil {
			continue
		}
		if err := fn(c.DB); err != nil {
			return nil, err
		}
	}

	// 执行 mixins
	for _, m := range c.mixins {
		if m == nil {
			continue
		}
		if err := m(c.DB); err != nil {
			return nil, err
		}
	}

	// 如果开启自动迁移，使用 resolveMigrateModels 汇总并执行 AutoMigrate
	if c.enableMigrate {
		models := c.resolveMigrateModels()
		if len(models) > 0 {
			if err := c.DB.AutoMigrate(models...); err != nil {
				return nil, err
			}
		}
	}

	for _, fn := range c.afterOpen {
		if fn == nil {
			continue
		}
		if err := fn(c.DB); err != nil {
			return nil, err
		}
	}

	return c, nil
}

// Use 注册 GORM Mixin 插件
func (c *Client) Use(m Mixin) {
	c.mixins = append(c.mixins, m)
}

// createDriver 创建数据库驱动
func createDriver(driverName, dsn string) gorm.Dialector {
	switch driverName {
	default:
		fallthrough
	case "sqlite":
		return sqlite.Open(dsn)
	case "go_sqlite":
		return goSqlite.Open(dsn)

	case "mysql":
		return mysql.Open(dsn)

	case "postgres":
		return postgres.Open(dsn)

	case "clickhouse":
		return clickhouse.Open(dsn)

	case "sqlserver":
		return sqlserver.Open(dsn)

	case "bigquery":
		return bigquery.Open(dsn)

	case "gaussdb":
		return gaussdb.Open(dsn)
	}
}

func (c *Client) resolveMigrateModels() []interface{} {
	var out []interface{}

	// 已注册的模型（全局注册函数）
	if regs := getRegisteredMigrateModels(); len(regs) > 0 {
		out = append(out, regs...)
	}

	// 通过注入函数获得的模型
	if c.getMigrateModels != nil {
		out = append(out, c.getMigrateModels()...)
	}

	// 实例级别的 migrateModels
	if len(c.migrateModels) > 0 {
		out = append(out, c.migrateModels...)
	}

	if len(out) == 0 {
		return nil
	}
	return out
}

// createGormClient 创建GORM的客户端
func (c *Client) createGormClient() error {
	driver := createDriver(c.driverName, c.dsn)
	if driver == nil {
		return fmt.Errorf("unsupported database driver: %s", c.driverName)
	}

	client, err := gorm.Open(driver, c.gormCfg)
	if err != nil {
		return fmt.Errorf("failed opening connection to db: %v", err)
	}

	if c.enableTrace {
		var opts []tracing.Option
		if c.enableMetrics {
			opts = append(opts, tracing.WithoutMetrics())
		}

		if err = client.Use(tracing.NewPlugin(opts...)); err != nil {
			return fmt.Errorf("failed opening connection to db: %v", err)
		}
	}

	// 运行数据库迁移工具
	if c.enableMigrate {
		if err = client.AutoMigrate(
			c.getMigrateModels()...,
		); err != nil {
			return fmt.Errorf("failed creating schema resources: %v", err)
		}
	}

	c.DB = client

	return nil
}
