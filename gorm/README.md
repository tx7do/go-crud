# GORM Package

基于 GORM 的通用数据访问层封装，提供完整的 CRUD 操作、分页、过滤、排序和缓存功能。

## 特性

- ✅ **泛型支持** - 完全基于 Go 泛型，类型安全
- ✅ **完整 CRUD** - 创建、查询、更新、删除操作
- ✅ **多种分页** - 支持 Offset、Page、Token 三种分页方式
- ✅ **结构化过滤** - 支持复杂的过滤表达式
- ✅ **灵活排序** - 支持多字段排序
- ✅ **字段选择** - 支持 FieldMask 选择返回字段
- ✅ **Redis 缓存** - 内置缓存支持，防击穿保护
- ✅ **DTO/Entity 映射** - 自动 DTO 和 Entity 转换
- ✅ **多数据库支持** - MySQL、PostgreSQL、SQLite、SQL Server、ClickHouse 等

## 快速开始

### 1. 安装依赖

```bash
go get github.com/tx7do/go-crud/gorm
```

### 2. 定义 Entity 和 DTO

**Entity (数据库模型):**

```go
type UserEntity struct {
    ID   uint64 `gorm:"primaryKey;autoIncrement"`
    Name string `gorm:"column:name;type:varchar(100)"`
    Age  int    `gorm:"column:age"`
    Email string `gorm:"column:email;type:varchar(200)"`
}

func (UserEntity) TableName() string {
    return "users"
}
```

**DTO (Protobuf 消息):**

```protobuf
syntax = "proto3";

package user;

option go_package = "github.com/example/user;user";

message User {
  uint64 id = 1;
  string name = 2;
  int32 age = 3;
  string email = 4;
}
```

### 3. 创建 Repository

```go
import (
    "github.com/tx7do/go-utils/mapper"
    "github.com/tx7do/go-crud/gorm"
)

// 创建 Mapper
m := mapper.NewCopierMapper[User, UserEntity]()

// 创建 Repository
repo := gorm.NewRepository[User, UserEntity](m)
```

### 4. 基本 CRUD 操作

```go
import (
    "context"
    "gorm.io/gorm"
)

ctx := context.Background()
db := /* your gorm.DB instance */

// 创建
newUser := &User{
    Name:  "John Doe",
    Age:   30,
    Email: "john@example.com",
}
created, err := repo.Create(ctx, db, newUser, nil)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Created user with ID: %d\n", created.Id)

// 查询单条
user, err := repo.Get(ctx, db.Where("id = ?", 1), nil)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("User: %+v\n", user)

// 更新
user.Age = 31
updated, err := repo.Update(ctx, db, user, nil)
if err != nil {
    log.Fatal(err)
}

// 删除
affected, err := repo.Delete(ctx, db.Where("id = ?", 1), false)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Deleted %d records\n", affected)
```

## 高级功能

### 分页查询

#### Offset 分页

```go
import paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"

page := uint32(1)
pageSize := uint32(10)
req := &paginationV1.PagingRequest{
    Page:     &page,
    PageSize: &pageSize,
}

result, err := repo.ListWithPaging(ctx, db, req)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Total: %d, Items: %d\n", result.Total, len(result.Items))
for _, user := range result.Items {
    fmt.Printf("User: %+v\n", user)
}
```

#### 带过滤和排序的分页

```go
// 过滤条件
filterExpr := &paginationV1.FilterExpr{
    // 构建过滤表达式
}

// 排序
sorting := []*paginationV1.Sorting{
    {Field: "age", Direction: paginationV1.Sorting_DESC},
}

req := &paginationV1.PagingRequest{
    Page:     &page,
    PageSize: &pageSize,
    Sorting:  sorting,
}

result, err := repo.ListWithPaging(ctx, db, req)
```

#### 字段选择（FieldMask）

```go
import "google.golang.org/protobuf/types/known/fieldmaskpb"

// 只返回 name 和 email 字段
fieldMask := &fieldmaskpb.FieldMask{
    Paths: []string{"name", "email"},
}

req := &paginationV1.PagingRequest{
    Page:      &page,
    PageSize:  &pageSize,
    FieldMask: fieldMask,
}

result, err := repo.ListWithPaging(ctx, db, req)
```

### 缓存功能

#### 启用缓存

```go
import (
    "time"
    "github.com/redis/go-redis/v9"
)

redisClient := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
})

// 配置缓存：单条缓存 10 分钟，列表缓存 5 分钟
repo.WithCache(redisClient, "user:", 10*time.Minute, 5*time.Minute)
```

#### 带缓存的查询

```go
// 按 ID 查询（带缓存）
user, err := repo.GetByIDWithCache(ctx, db, 1, nil)

// 分页列表查询（带缓存）
result, err := repo.ListWithPagingCache(ctx, db, req)
```

#### 带缓存的写操作

```go
// 创建（自动失效相关缓存）
created, err := repo.CreateWithCache(ctx, db, newUser, nil)

// 更新（自动失效相关缓存）
updated, err := repo.UpdateWithCache(ctx, db, user, nil)

// 删除（自动失效相关缓存）
affected, err := repo.DeleteWithCache(ctx, db, 1, false)
```

### 计数查询

```go
// 基本计数
count, err := repo.Count(ctx, db.Where("age > ?", 18), nil)

// 带选项的计数
opts := &gorm.CountOptions{
    Distinct: "user_id",  // 去重计数
    Timeout:  5 * time.Second,
}
count, err := repo.CountWithOptions(ctx, db, whereSelectors, opts)
```

### 存在性检查

```go
exists, err := repo.Exists(ctx, db.Where("email = ?", "john@example.com"))
if exists {
    fmt.Println("User exists")
}
```

## GORM Client

### 创建客户端

```go
import "github.com/tx7do/go-crud/gorm"

// MySQL
client, err := gorm.NewClient(
    gorm.WithDriverName("mysql"),
    gorm.WithMasterDSN("user:password@tcp(localhost:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local"),
    gorm.WithEnableTrace(),
    gorm.WithEnableMetrics(),
)

// PostgreSQL
client, err := gorm.NewClient(
    gorm.WithDriverName("postgres"),
    gorm.WithMasterDSN("host=localhost user=postgres password=secret dbname=mydb port=5432 sslmode=disable"),
)

// SQLite (内存)
client, err := gorm.NewClient(
    gorm.WithDriverName("sqlite"),
    gorm.WithMasterDSN(":memory:"),
)
```

### 读写分离

```go
client, err := gorm.NewClient(
    gorm.WithDriverName("mysql"),
    gorm.WithMasterDSN("master-dsn"),
    gorm.WithReplicaDsns([]string{
        "replica1-dsn",
        "replica2-dsn",
    }),
    gorm.WithEnableDbResolver(),
)
```

### 自动迁移

```go
type UserModel struct {
    ID   uint64 `gorm:"primaryKey"`
    Name string
}

client, err := gorm.NewClient(
    gorm.WithDriverName("sqlite"),
    gorm.WithMasterDSN(":memory:"),
    gorm.WithEnableMigrate(),
    gorm.WithMigrateModels(&UserModel{}),
)
```

### 连接池配置

```go
maxIdleConns := 10
maxOpenConns := 100
connMaxLifetime := time.Hour

client, err := gorm.NewClient(
    gorm.WithDriverName("mysql"),
    gorm.WithMasterDSN(dsn),
    gorm.WithMaxIdleConns(maxIdleConns),
    gorm.WithMaxOpenConns(maxOpenConns),
    gorm.WithConnMaxLifetime(connMaxLifetime),
)
```

## API 参考

### Repository 方法

#### 查询方法

- `Get(ctx, db, viewMask)` - 查询单条记录
- `Only(ctx, db, viewMask)` - Get 的别名
- `ListWithPaging(ctx, db, req)` - 分页列表查询
- `ListWithPagination(ctx, db, req)` - 通用分页列表查询
- `Count(ctx, db, whereSelectors)` - 计数
- `CountWithOptions(ctx, db, whereSelectors, opts)` - 带选项的计数
- `Exists(ctx, db, whereSelectors)` - 存在性检查

#### 创建方法

- `Create(ctx, db, dto, createMask)` - 创建单条记录
- `CreateX(ctx, db, dto, createMask)` - 创建但不返回结果
- `BatchCreate(ctx, db, dtos, createMask)` - 批量创建

#### 更新方法

- `Update(ctx, db, dto, updateMask)` - 更新记录并返回
- `UpdateX(ctx, db, dto, updateMask)` - 更新但不返回结果

#### 删除方法

- `Delete(ctx, db, notSoftDelete)` - 删除记录

#### 缓存方法

- `WithCache(redisClient, prefix, singleTTL, listTTL)` - 配置缓存
- `WithCacheFromRedis(redisClient, prefix, ttl)` - 简化缓存配置
- `GetWithCache(ctx, db, viewMask)` - 带缓存的单条查询
- `GetByIDWithCache(ctx, db, id, viewMask)` - 按 ID 带缓存查询
- `ListWithPagingCache(ctx, db, req)` - 带缓存的分页查询
- `CreateWithCache(ctx, db, dto, createMask)` - 创建并失效缓存
- `UpdateWithCache(ctx, db, dto, updateMask)` - 更新并失效缓存
- `DeleteWithCache(ctx, db, id, notSoftDelete)` - 删除并失效缓存

### Client 配置选项

- `WithDriverName(name)` - 设置数据库驱动
- `WithMasterDSN(dsn)` - 设置主库 DSN
- `WithReplicaDsns(dsns)` - 设置从库 DSNs
- `WithGormDB(db)` - 直接传入 GORM DB 实例
- `WithEnableTrace()` - 启用 OpenTelemetry 追踪
- `WithEnableMetrics()` - 启用 Prometheus 指标
- `WithEnableMigrate()` - 启用自动迁移
- `WithEnableDbResolver()` - 启用读写分离
- `WithMigrateModels(models...)` - 注册迁移模型
- `WithMaxIdleConns(n)` - 设置最大空闲连接数
- `WithMaxOpenConns(n)` - 设置最大打开连接数
- `WithConnMaxLifetime(d)` - 设置连接最大生命周期
- `WithBeforeOpen(fn)` - 注册打开前钩子
- `WithAfterOpen(fn)` - 注册打开后钩子

## 支持的数据库

| 数据库 | 驱动名称 | DSN 示例 |
|--------|---------|----------|
| MySQL | `mysql` | `user:pass@tcp(host:3306)/db?charset=utf8mb4` |
| PostgreSQL | `postgres` | `host=localhost user=postgres dbname=mydb` |
| SQLite | `sqlite` | `file.db` 或 `:memory:` |
| SQL Server | `sqlserver` | `sqlserver://user:pass@host/db` |
| ClickHouse | `clickhouse` | `clickhouse://user:pass@host:9000/db` |
| BigQuery | `bigquery` | `bigquery://project/dataset` |
| GaussDB | `gaussdb` | `host=localhost user=gaussdb dbname=mydb` |

## 最佳实践

### 1. 使用事务

```go
err := db.Transaction(func(tx *gorm.DB) error {
    // 在事务中执行操作
    _, err := repo.Create(ctx, tx, user, nil)
    if err != nil {
        return err
    }
    
    // 其他操作...
    
    return nil
})
```

### 2. 合理使用缓存

```go
// 热点数据：较长 TTL
repo.WithCache(redisClient, "hot:", 30*time.Minute, 10*time.Minute)

// 实时数据：较短 TTL
repo.WithCache(redisClient, "realtime:", 1*time.Minute, 30*time.Second)
```

### 3. 错误处理

```go
user, err := repo.Get(ctx, db.Where("id = ?", 1), nil)
if err != nil {
    if errors.Is(err, gorm.ErrRecordNotFound) {
        // 记录不存在
        return nil, nil
    }
    // 其他错误
    return nil, err
}
```

### 4. 性能优化

```go
// 使用字段选择减少数据传输
fieldMask := &fieldmaskpb.FieldMask{
    Paths: []string{"id", "name"},
}
user, err := repo.Get(ctx, db, fieldMask)

// 使用索引优化查询
db.IndexHint("USE INDEX", "idx_user_email").Where("email = ?", email)
```

## 测试

运行测试：

```bash
go test -v ./gorm/...
```

运行缓存测试：

```bash
go test -v ./gorm -run TestRepository.*Cache
```

运行基准测试：

```bash
go test -bench=BenchmarkRepository -benchmem ./gorm
```

## 示例项目

查看完整示例：[repository_test.go](./repository_test.go)

## 依赖

- `gorm.io/gorm` - GORM ORM 框架
- `github.com/tx7do/go-utils/mapper` - DTO/Entity 映射
- `github.com/redis/go-redis/v9` - Redis 客户端（缓存）
- `github.com/tx7do/go-crud/api` - Protobuf 定义
- `github.com/tx7do/go-crud/cache` - 缓存支持

## 许可证

本项目采用 MIT 许可证。
