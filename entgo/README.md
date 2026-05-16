# Ent.go Package

基于 [Ent](https://ent.ariga.io/) 的通用数据访问层封装，提供完整的 CRUD 操作、分页、过滤、排序、树形查询和缓存功能。

## 特性

- ✅ **泛型支持** - 完全基于 Go 泛型，类型安全
- ✅ **完整 CRUD** - 创建、查询、更新、删除操作
- ✅ **多种分页** - 支持 Offset、Page、Token 三种分页方式
- ✅ **结构化过滤** - 支持复杂的过滤表达式
- ✅ **灵活排序** - 支持多字段排序
- ✅ **字段选择** - 支持 FieldMask 选择返回字段
- ✅ **Redis 缓存** - 内置缓存支持，防击穿保护
- ✅ **树形查询** - 支持树形结构的递归查询和路径计算
- ✅ **DTO/Entity 映射** - 自动 DTO 和 Entity 转换
- ✅ **事务支持** - 完善的事务管理和清理机制
- ✅ **OpenTelemetry** - 内置追踪和指标支持
- ✅ **多数据库支持** - MySQL、PostgreSQL、SQLite

## 快速开始

### 1. 安装依赖

```bash
go get github.com/tx7do/go-crud/entgo
```

### 2. 定义 Ent Schema

```go
// ent/schema/user.go
package schema

import (
    "entgo.io/ent"
    "entgo.io/ent/schema/field"
)

type User struct {
    ent.Schema
}

func (User) Fields() []ent.Field {
    return []ent.Field{
        field.Uint64("id"),
        field.String("name").MaxLen(100),
        field.Int("age").Optional(),
        field.String("email").MaxLen(200).Unique(),
    }
}
```

### 3. 生成 Ent 代码

```bash
go generate ./ent
```

### 4. 定义 DTO (Protobuf)

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

### 5. 创建 Repository

```go
import (
    "github.com/tx7do/go-utils/mapper"
    "github.com/tx7do/go-crud/entgo"
)

// 创建 Mapper
m := mapper.NewCopierMapper[User, ent.User]()

// 创建 Repository
repo := entgo.NewRepository[
    *ent.UserQuery, *ent.UserSelect,
    *ent.UserCreate, []*ent.UserCreate,
    *ent.UserUpdate, *ent.UserUpdateOne,
    *ent.UserDelete,
    *ent.Predicate,
    User, ent.User,
](m)
```

### 6. 基本 CRUD 操作

```go
import (
    "context"
    "github.com/example/ent"
)

ctx := context.Background()
client := /* your ent.Client instance */

// 创建
newUser := &User{
    Name:  "John Doe",
    Age:   30,
    Email: "john@example.com",
}
created, err := repo.Create(ctx, client.User.Create(), newUser, nil)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Created user with ID: %d\n", created.Id)

// 查询单条
user, err := repo.Get(ctx, client.User.Query().Where(user.ID(1)), nil)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("User: %+v\n", user)

// 更新
user.Age = 31
updated, err := repo.Update(ctx, client.User.UpdateOneID(user.Id), user, nil)
if err != nil {
    log.Fatal(err)
}

// 删除
affected, err := repo.Delete(ctx, client.User.Delete().Where(user.ID(1)))
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

result, err := repo.ListWithPaging(ctx, 
    client.User.Query(),  // 列表查询 builder
    client.User.Query(),  // 计数查询 builder
    req)
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

result, err := repo.ListWithPaging(ctx, 
    client.User.Query(),
    client.User.Query(),
    req)
```

#### 通用分页请求（PaginationRequest）

```go
import "github.com/tx7do/go-crud/api/gen/go/pagination/v1"

req := &paginationV1.PaginationRequest{
    Limit:  10,
    Offset: 0,
}

result, err := repo.ListWithPagination(ctx,
    client.User.Query(),
    client.User.Query(),
    req)
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

result, err := repo.ListWithPaging(ctx, 
    client.User.Query(),
    client.User.Query(),
    req)
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
user, err := repo.GetByIDWithCache(ctx, client.User.Query(), 1, nil)

// 分页列表查询（带缓存）
result, err := repo.ListWithPagingCache(ctx,
    client.User.Query(),
    client.User.Query(),
    req)

// 通用分页查询（带缓存）
result, err := repo.ListWithPaginationCache(ctx,
    client.User.Query(),
    client.User.Query(),
    paginationReq)
```

#### 带缓存的写操作

```go
// 创建（自动失效相关缓存）
created, err := repo.CreateWithCache(ctx, client.User.Create(), newUser, nil)

// 更新（自动失效相关缓存）
updated, err := repo.UpdateWithCache(ctx, client.User.UpdateOneID(user.Id), user, nil)

// 删除（自动失效相关缓存）
affected, err := repo.DeleteWithCache(ctx, client.User.Delete().Where(user.ID(1)))
```

### 树形查询

Ent.go 包提供了强大的树形结构支持，适用于菜单、分类、组织架构等场景。

#### 查询所有子节点

```go
// 递归查询某个节点的所有子节点 ID
childIDs, err := entgo.QueryAllChildrenIds(ctx, entClient, "menus", parentID)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Child IDs: %v\n", childIDs)
```

#### 计算树节点路径

```go
// 计算节点的路径（用于物化路径模式）
path := entgo.ComputeTreePath(parentPath, nodeID)
// 例如：parentPath="/1/2/", nodeID=3 => path="/1/2/3/"
```

#### 树形分页查询

```go
// 树形结构分页查询
result, err := repo.ListTreeWithPagingCache(ctx,
    client.Menu.Query(),
    client.Menu.Query(),
    req)

// 或使用 PaginationRequest
result, err := repo.ListTreeWithPaginationCache(ctx,
    client.Menu.Query(),
    client.Menu.Query(),
    paginationReq)
```

### 事务管理

```go
import "github.com/tx7do/go-crud/entgo"

// 使用事务
var txErr error
defer entgo.MakeTxCleanup(tx, &txErr)()

// 在事务中执行操作
created, err := repo.Create(ctx, tx.User.Create(), newUser, nil)
if err != nil {
    txErr = err
    return
}

// 其他操作...
updated, err := repo.Update(ctx, tx.User.UpdateOneID(created.Id), updatedUser, nil)
if err != nil {
    txErr = err
    return
}
```

### 计数和存在性检查

```go
// 计数
count, err := repo.Count(ctx, client.User.Query(), 
    func(s *sql.Selector) {
        s.Where(sql.EQ("age", 30))
    })

// 存在性检查
exists, err := repo.Exists(ctx, client.User.Query(),
    func(s *sql.Selector) {
        s.Where(sql.EQ("email", "john@example.com"))
    })
if exists {
    fmt.Println("User exists")
}
```

## Ent Client

### 创建客户端

```go
import (
    "github.com/tx7do/go-crud/entgo"
    "github.com/example/ent"
)

// MySQL
drv, err := entgo.CreateDriver("mysql", 
    "user:password@tcp(localhost:3306)/dbname?charset=utf8mb4&parseTime=True&loc=Local",
    true,  // enable trace
    true,  // enable metrics
)
if err != nil {
    log.Fatal(err)
}

client := ent.NewClient(ent.Driver(drv))
entClient := entgo.NewEntClient(client, drv)

// PostgreSQL
drv, err := entgo.CreateDriver("postgres",
    "host=localhost user=postgres password=secret dbname=mydb port=5432 sslmode=disable",
    true,
    true,
)

// SQLite (内存)
drv, err := entgo.CreateDriver("sqlite", ":memory:", false, false)
```

### 连接池配置

```go
// 设置连接池参数
entClient.SetConnectionOption(
    10,                    // max idle connections
    100,                   // max open connections
    time.Hour,             // connection max lifetime
)
```

### 关闭连接

```go
defer entClient.Close()
```

### 直接执行 SQL

```go
// 查询
var result []struct {
    ID   uint64
    Name string
}
err := entClient.Query(ctx, "SELECT id, name FROM users WHERE age > ?", []any{18}, &result)

// 执行
err := entClient.Exec(ctx, "UPDATE users SET age = ? WHERE id = ?", []any{30, 1}, nil)
```

## API 参考

### Repository 方法

#### 查询方法

- `Get(ctx, builder, viewMask)` - 查询单条记录
- `Only(ctx, builder, viewMask)` - Get 的别名
- `ListWithPaging(ctx, builder, countBuilder, req)` - 分页列表查询
- `ListWithPagination(ctx, builder, countBuilder, req)` - 通用分页列表查询
- `Count(ctx, builder, predicates...)` - 计数
- `Exists(ctx, builder, predicates...)` - 存在性检查

#### 创建方法

- `Create(ctx, builder, dto, createMask)` - 创建单条记录
- `CreateX(ctx, builder, dto, createMask)` - 创建但不返回结果
- `BatchCreate(ctx, builder, dtos, createMask)` - 批量创建

#### 更新方法

- `Update(ctx, builder, dto, updateMask)` - 更新记录并返回
- `UpdateX(ctx, builder, dto, updateMask)` - 更新但不返回结果

#### 删除方法

- `Delete(ctx, builder)` - 删除记录

#### 缓存方法

- `WithCache(redisClient, prefix, singleTTL, listTTL)` - 配置缓存
- `WithCacheFromRedis(redisClient, prefix, ttl)` - 简化缓存配置
- `GetWithCache(ctx, builder, viewMask)` - 带缓存的单条查询
- `GetByIDWithCache(ctx, builder, id, viewMask)` - 按 ID 带缓存查询
- `ListWithPagingCache(ctx, builder, countBuilder, req)` - 带缓存的分页查询
- `ListWithPaginationCache(ctx, builder, countBuilder, req)` - 带缓存的通用分页查询
- `ListTreeWithPagingCache(ctx, builder, countBuilder, req)` - 带缓存的树形分页查询
- `ListTreeWithPaginationCache(ctx, builder, countBuilder, req)` - 带缓存的树形通用分页查询
- `CreateWithCache(ctx, builder, dto, createMask)` - 创建并失效缓存
- `UpdateWithCache(ctx, builder, dto, updateMask)` - 更新并失效缓存
- `DeleteWithCache(ctx, builder)` - 删除并失效缓存

### EntClient 方法

- `Client()` - 获取底层 Ent Client
- `Driver()` - 获取 SQL Driver
- `DB()` - 获取 sql.DB 实例
- `Close()` - 关闭连接
- `Query(ctx, query, args, v)` - 执行查询
- `Exec(ctx, query, args, v)` - 执行命令
- `SetConnectionOption(maxIdle, maxOpen, maxLifetime)` - 设置连接池

### 工具函数

- `CreateDriver(driverName, dsn, enableTrace, enableMetrics)` - 创建数据库驱动
- `MakeTxCleanup(tx, errPtr)` - 创建事务清理函数
- `Rollback(tx, err)` - 回滚事务
- `QueryAllChildrenIds(ctx, client, table, parentID)` - 递归查询子节点 ID
- `SyncSequence(ctx, client, schema, table, column)` - 同步序列（PostgreSQL）
- `ComputeTreePath(parentPath, nodeID)` - 计算树节点路径
- `QuoteIdent(dialect, ident)` - 引用标识符
- `EscapeLiteral(s)` - 转义字符串字面量
- `ValidateSchemaTableColumn(schema, table, column)` - 校验标识符

## 支持的数据库

| 数据库 | 驱动名称 | DSN 示例 |
|--------|---------|----------|
| MySQL | `mysql` | `user:pass@tcp(host:3306)/db?charset=utf8mb4` |
| PostgreSQL | `postgres` | `host=localhost user=postgres dbname=mydb` |
| SQLite | `sqlite` | `file.db` 或 `:memory:` |

## 最佳实践

### 1. 使用 Mixin

Ent 的 Mixin 可以复用字段和行为：

```go
import "github.com/tx7do/go-crud/entgo/mixin"

type User struct {
    ent.Schema
}

func (User) Mixin() []ent.Mixin {
    return []ent.Mixin{
        mixin.TimeMixin{},     // 自动添加 created_at, updated_at
        mixin.SoftDeleteMixin{}, // 软删除支持
    }
}
```

### 2. 使用 Interceptor

Interceptor 可以在查询前后执行逻辑：

```go
import "github.com/tx7do/go-crud/entgo/interceptor"

// 审计日志
client.Use(interceptor.AuditLogger())

// 数据权限
client.Use(interceptor.DataScope(viewer))
```

### 3. 合理使用缓存

```go
// 热点数据：较长 TTL
repo.WithCache(redisClient, "hot:", 30*time.Minute, 10*time.Minute)

// 实时数据：较短 TTL
repo.WithCache(redisClient, "realtime:", 1*time.Minute, 30*time.Second)
```

### 4. 树形结构优化

对于大型树形结构，建议使用物化路径（Materialized Path）：

```go
// 在 Schema 中添加 path 字段
field.String("path").Default("/"),

// 创建时计算路径
path := entgo.ComputeTreePath(parent.Path, newNode.ID)
```

### 5. 错误处理

```go
user, err := repo.Get(ctx, client.User.Query().Where(user.ID(1)), nil)
if err != nil {
    if ent.IsNotFound(err) {
        // 记录不存在
        return nil, nil
    }
    // 其他错误
    return nil, err
}
```

### 6. 性能优化

```go
// 使用字段选择减少数据传输
fieldMask := &fieldmaskpb.FieldMask{
    Paths: []string{"id", "name"},
}
user, err := repo.Get(ctx, builder, fieldMask)

// 使用索引优化查询
builder.Modify(func(s *sql.Selector) {
    s.Where(sql.EQ("email", email))
})
```

## 测试

运行测试：

```bash
go test -v ./entgo/...
```

运行缓存测试：

```bash
go test -v ./entgo -run TestRepository.*Cache
```

运行树形查询测试：

```bash
go test -v ./entgo -run TestTree
```

## 示例项目

查看完整示例：
- [repository_test.go](./repository_test.go) - Repository 测试示例
- [tree_test.go](./tree_test.go) - 树形查询测试示例
- [client_test.go](./client_test.go) - Client 测试示例

## 依赖

- `entgo.io/ent` - Ent ORM 框架
- `github.com/tx7do/go-utils/mapper` - DTO/Entity 映射
- `github.com/redis/go-redis/v9` - Redis 客户端（缓存）
- `github.com/tx7do/go-crud/api` - Protobuf 定义
- `github.com/tx7do/go-crud/cache` - 缓存支持
- `go.opentelemetry.io/otel` - OpenTelemetry 追踪

## 与 GORM 包的区别

| 特性 | Ent.go | GORM |
|------|--------|------|
| ORM 类型 | Code Generation | Reflection-based |
| 类型安全 | ✅ 编译时检查 | ⚠️ 运行时检查 |
| 查询构建器 | 强类型 | 链式调用 |
| 关系处理 | 自动生成 | 手动配置 |
| 迁移工具 | 内置 | 需要插件 |
| 学习曲线 | 较陡 | 平缓 |
| 性能 | 更好 | 良好 |

## 许可证

本项目采用 MIT 许可证。
