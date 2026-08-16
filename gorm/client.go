package gorm

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tx7do/go-wind/log"

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

type gormLogger struct {
	cfg logger.Config
}

func NewGormLogger(l log.Logger) logger.Interface {
	// 默认 Warn：仅记录慢查询与错误。此前默认 Info 会把含插值参数的完整
	// SQL（含 INSERT/UPDATE 的值）打进日志，模型携带敏感字段时构成泄露。
	return NewGormLoggerWithLevel(l, logger.Warn)
}

// NewGormLoggerWithLevel 以指定等级创建 gorm logger。
// logger.Info 会记录所有 SQL（含插值参数），仅应在低敏环境显式开启。
func NewGormLoggerWithLevel(l log.Logger, level logger.LogLevel) logger.Interface {
	if l != nil {
		log.SetLogger(l.With("module", "gorm"))
	}

	return &gormLogger{
		cfg: logger.Config{
			SlowThreshold:             100 * time.Millisecond,
			LogLevel:                  level,
			IgnoreRecordNotFoundError: true,
			Colorful:                  false,
		},
	}
}

func (l *gormLogger) LogMode(level logger.LogLevel) logger.Interface {
	clone := *l
	clone.cfg.LogLevel = level
	return &clone
}

func (l *gormLogger) Info(ctx context.Context, msg string, data ...any) {
	if l.cfg.LogLevel < logger.Info {
		return
	}
	log.Info(ctx, fmt.Sprintf(msg, data...))
}

func (l *gormLogger) Warn(ctx context.Context, msg string, data ...any) {
	if l.cfg.LogLevel < logger.Warn {
		return
	}
	log.Warn(ctx, fmt.Sprintf(msg, data...))
}

func (l *gormLogger) Error(ctx context.Context, msg string, data ...any) {
	if l.cfg.LogLevel < logger.Error {
		return
	}
	log.Error(ctx, fmt.Sprintf(msg, data...))
}

func (l *gormLogger) Trace(ctx context.Context, begin time.Time, fc func() (sql string, rowsAffected int64), err error) {
	if l.cfg.LogLevel == logger.Silent {
		return
	}

	elapsed := time.Since(begin)
	sql, rows := fc()
	base := fmt.Sprintf("elapsed=%s rows=%d sql=%s", elapsed, rows, sql)

	if err != nil && l.cfg.LogLevel >= logger.Error {
		if errors.Is(err, gorm.ErrRecordNotFound) && l.cfg.IgnoreRecordNotFoundError {
			return
		}
		log.Error(ctx, fmt.Sprintf("[GORM] %s err=%v", base, err))
		return
	}

	if l.cfg.SlowThreshold > 0 && elapsed > l.cfg.SlowThreshold && l.cfg.LogLevel >= logger.Warn {
		log.Warn(ctx, fmt.Sprintf("[GORM][SLOW] %s threshold=%s", base, l.cfg.SlowThreshold))
		return
	}

	if l.cfg.LogLevel >= logger.Info {
		log.Info(ctx, fmt.Sprintf("[GORM] %s", base))
	}
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

	migrateModels    []any
	getMigrateModels GetMigrateModelsFunc

	gormCfg *gorm.Config
	mixins  []Mixin

	ctx context.Context

	// 钩子
	beforeOpen []func(*gorm.DB) error
	afterOpen  []func(*gorm.DB) error

	// logger helper
	logger log.Logger

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

func (c *Client) resolveMigrateModels() []any {
	var out []any

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
