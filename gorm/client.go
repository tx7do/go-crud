package gorm

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kratos/kratos/v2/log"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"gorm.io/plugin/opentelemetry/tracing"
	"gorm.io/plugin/prometheus"

	goSqlite "github.com/glebarez/sqlite"
	//"github.com/oracle-samples/gorm-oracle/oracle"
	"gorm.io/driver/bigquery"
	"gorm.io/driver/clickhouse"
	"gorm.io/driver/gaussdb"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/driver/sqlserver"
	"gorm.io/plugin/dbresolver"
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
	driverName  string
	masterDSN   string
	replicaDsns []string

	enableTrace      bool
	enableMigrate    bool
	enableMetrics    bool
	enableDbResolver bool

	migrateModels    []interface{}
	getMigrateModels GetMigrateModelsFunc

	gormCfg   *gorm.Config
	cfgStruct interface{}
	mixins    []Mixin

	ctx       context.Context
	envPrefix string

	// 钩子
	beforeOpen []func(*gorm.DB) error
	afterOpen  []func(*gorm.DB) error

	// 任意原始选项
	rawOptions RawOptions

	// logger helper
	logger *log.Helper

	prometheusConfig prometheus.Config
	tracingOption    []tracing.Option

	maxIdleConns    *int
	maxOpenConns    *int
	connMaxLifetime *time.Duration
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

	// 如果没有外部传入 DB，则尝试根据 driverName/masterDSN 创建
	if c.DB == nil {
		if c.driverName == "" || c.masterDSN == "" {
			return nil, fmt.Errorf("gorm DB not provided; either use WithGormDB or provide driverName/masterDSN")
		}
		if err := c.initGormClient(); err != nil {
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

// initGormClient 初始化GORM的客户端
func (c *Client) initGormClient() error {
	client, err := createGormClient(c.driverName, c.masterDSN, c.gormCfg)
	if err != nil {
		return fmt.Errorf("failed opening connection to db: %v", err)
	}
	c.DB = client

	if c.enableTrace {
		if err = client.Use(tracing.NewPlugin(c.tracingOption...)); err != nil {
			return fmt.Errorf("failed enable tracing plugin: %v", err)
		}
	}

	if c.enableMetrics {
		if err = client.Use(prometheus.New(c.prometheusConfig)); err != nil {
			return fmt.Errorf("failed enable prometheus metrics: %v", err)
		}
	}

	// 注册读写分离
	if c.enableDbResolver {
		masterDriver := createDriver(c.masterDSN, c.masterDSN)

		var replicaDrivers []gorm.Dialector
		for _, replicaDSN := range c.replicaDsns {
			replicaClient := createDriver(c.driverName, replicaDSN)
			replicaDrivers = append(replicaDrivers, replicaClient)
		}

		if err = client.Use(dbresolver.Register(dbresolver.Config{
			Sources:  []gorm.Dialector{masterDriver},
			Replicas: replicaDrivers,
			Policy:   dbresolver.RandomPolicy{},
		})); err != nil {
			panic(err)
		}
	}

	sqlDB, _ := c.DB.DB()
	if sqlDB != nil {
		if c.maxIdleConns != nil {
			sqlDB.SetMaxIdleConns(*c.maxIdleConns)
		}
		if c.maxOpenConns != nil {
			sqlDB.SetMaxOpenConns(*c.maxOpenConns)
		}
		if c.connMaxLifetime != nil {
			sqlDB.SetConnMaxLifetime(*c.connMaxLifetime)
		}
	}

	// 运行数据库迁移工具
	if c.enableMigrate {
		if err = c.doAutoMigrate(); err != nil {
			return err
		}
	}

	return nil
}

// doAutoMigrate 执行自动迁移
func (c *Client) doAutoMigrate() error {
	if err := c.AutoMigrate(
		c.getMigrateModels()...,
	); err != nil {
		return fmt.Errorf("failed creating schema resources: %v", err)
	}

	return nil
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

		//case "oracle":
		//	return oracle.Open(dsn)
	}
}

func createGormClient(driverName, dsn string, cfg *gorm.Config) (*gorm.DB, error) {
	driver := createDriver(driverName, dsn)
	if driver == nil {
		return nil, fmt.Errorf("unsupported database driver: %s", driverName)
	}

	client, err := gorm.Open(driver, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed opening connection to db: %v", err)
	}

	return client, nil
}
