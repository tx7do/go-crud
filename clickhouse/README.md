# ClickHouse Package

基于 [ClickHouse](https://clickhouse.com/) 的列式数据库访问层封装，提供完整的数据写入、查询、分页、过滤和批量插入功能。

## 特性

- ✅ **泛型支持** - 完全基于 Go 泛型，类型安全
- ✅ **高性能列式存储** - 专为 OLAP 场景优化
- ✅ **完整 CRUD** - 创建、查询、更新、删除操作
- ✅ **多种分页** - 支持 Offset、Page、Token 三种分页方式
- ✅ **结构化过滤** - 支持复杂的过滤表达式
- ✅ **灵活排序** - 支持多字段排序
- ✅ **字段选择** - 支持 FieldMask 选择返回字段
- ✅ **批量插入** - 高效的批量数据写入（BatchInsert、AsyncInsert）
- ✅ **异步插入** - 支持异步写入模式，提升吞吐量
- ✅ **连接池管理** - 自动管理连接生命周期
- ✅ **DTO/Entity 映射** - 自动 DTO 和 Entity 转换
- ✅ **go-wind 日志集成** - 内置日志支持

## Docker 部署

### Pull Image

```bash
docker pull bitnami/clickhouse:latest
```

### 启动容器

```bash
docker run -itd \
    --name clickhouse-server \
    --network=app-tier \
    -p 8123:8123 \
    -p 9000:9000 \
    -p 9004:9004 \
    -e ALLOW_EMPTY_PASSWORD=no \
    -e CLICKHOUSE_ADMIN_USER=default \
    -e CLICKHOUSE_ADMIN_PASSWORD=123456 \
    bitnami/clickhouse:latest
```

### 端口说明

| 端口 | 协议 | 说明 |
|------|------|------|
| 8123 | HTTP | HTTP 接口（Web UI、REST API） |
| 9000 | Native | 原生 TCP 协议（推荐） |
| 9004 | HTTPS | HTTPS 接口 |

### 访问 Web UI

打开浏览器访问：<http://localhost:8123/play>

使用以下凭据登录：
- 用户名：`default`
- 密码：`123456`

### Docker Compose 部署

```yaml
version: '3.8'

services:
  clickhouse:
    image: bitnami/clickhouse:latest
    container_name: clickhouse-server
    ports:
      - "8123:8123"
      - "9000:9000"
      - "9004:9004"
    environment:
      - ALLOW_EMPTY_PASSWORD=no
      - CLICKHOUSE_ADMIN_USER=default
      - CLICKHOUSE_ADMIN_PASSWORD=123456
      - CLICKHOUSE_DB=my_database
    volumes:
      - clickhouse_data:/bitnami/clickhouse
    networks:
      - app-tier

volumes:
  clickhouse_data:

networks:
  app-tier:
    driver: bridge
```

## 快速开始

### 1. 安装依赖

```bash
go get github.com/tx7do/go-crud/clickhouse
```

### 2. 定义 Entity 和 DTO

**Entity (ClickHouse 表结构):**

```go
type EventEntity struct {
    EventID   uint64    `ch:"event_id" json:"event_id"`
    UserID    uint64    `ch:"user_id" json:"user_id"`
    EventType string    `ch:"event_type" json:"event_type"`
    Timestamp time.Time `ch:"timestamp" json:"timestamp"`
    Properties string   `ch:"properties" json:"properties"`
}
```

**注意：** ClickHouse 使用 `ch` 标签映射字段名。

**DTO (Protobuf 消息):**

```protobuf
syntax = "proto3";

package events;

option go_package = "github.com/example/events;events";

import "google/protobuf/timestamp.proto";

message Event {
  uint64 event_id = 1;
  uint64 user_id = 2;
  string event_type = 3;
  google.protobuf.Timestamp timestamp = 4;
  string properties = 5;
}
```

### 3. 创建 Client

```go
import (
    "github.com/tx7do/go-crud/clickhouse"
)

// 方式 1：使用 DSN
client, err := clickhouse.NewClient(
    clickhouse.WithDsn("clickhouse://default:123456@localhost:9000/my_database"),
)

// 方式 2：使用配置选项
client, err := clickhouse.NewClient(
    clickhouse.WithAddresses("localhost:9000"),
    clickhouse.WithUsername("default"),
    clickhouse.WithPassword("123456"),
    clickhouse.WithDatabase("my_database"),
    clickhouse.WithDialTimeout(10 * time.Second),
    clickhouse.WithReadTimeout(30 * time.Second),
    clickhouse.WithMaxOpenConns(100),
    clickhouse.WithMaxIdleConns(10),
)

if err != nil {
    log.Fatal(err)
}
defer client.Close()
```

### 4. 创建 Repository

```go
import (
    "github.com/tx7do/go-utils/mapper"
    "github.com/tx7do/go-crud/clickhouse"
)

// 创建 Mapper
m := mapper.NewCopierMapper[Event, EventEntity]()

// 创建 Repository
repo := clickhouse.NewRepository[Event, EventEntity](
    client,
    m,
    "events",  // 表名
    logger,
)
```

### 5. 写入数据

#### 单条插入

```go
ctx := context.Background()

event := &Event{
    EventId:   1,
    UserId:    1001,
    EventType: "page_view",
    Timestamp: timestamppb.Now(),
    Properties: `{"page": "/home"}`,
}

err := repo.Create(ctx, event)
if err != nil {
    log.Fatal(err)
}
```

#### 批量插入（推荐）

```go
events := []any{
    &Event{EventId: 1, UserId: 1001, EventType: "page_view", ...},
    &Event{EventId: 2, UserId: 1002, EventType: "click", ...},
    &Event{EventId: 3, UserId: 1003, EventType: "purchase", ...},
}

err := repo.BatchCreate(ctx, events)
if err != nil {
    log.Fatal(err)
}
```

#### 异步插入（高性能）

```go
// 异步插入单条
event := &Event{...}
err := client.AsyncInsert(ctx, "events", event, false)  // wait=false

// 异步批量插入
events := []any{...}
err := client.AsyncInsertMany(ctx, "events", events, false)
```

**注意：** `wait=false` 表示不等待服务器确认，性能更高但可能丢失数据。

---

### 6. 查询数据

#### 基本分页查询

```go
import paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"

page := uint32(1)
pageSize := uint32(10)
req := &paginationV1.PagingRequest{
    Page:     &page,
    PageSize: &pageSize,
}

result, err := repo.ListWithPaging(ctx, req)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Total: %d, Items: %d\n", result.Total, len(result.Items))
for _, event := range result.Items {
    fmt.Printf("Event: %+v\n", event)
}
```

#### 带过滤的查询

```go
// 查询特定用户的事件
filterExpr := &paginationV1.FilterExpr{
    Conditions: []*paginationV1.FilterCondition{
        {
            Field: "user_id",
            Op:    paginationV1.Operator_EQ,
            Value: &paginationV1.FilterCondition_ValueOneof{Value: "1001"},
        },
    },
}

req := &paginationV1.PagingRequest{
    Page:          &page,
    PageSize:      &pageSize,
    FilteringType: &paginationV1.PagingRequest_FilterExpr{
        FilterExpr: filterExpr,
    },
}

result, err := repo.ListWithPaging(ctx, req)
```

#### 带排序的查询

```go
// 按时间降序排列
sorting := []*paginationV1.Sorting{
    {Field: "timestamp", Direction: paginationV1.Sorting_DESC},
}

req := &paginationV1.PagingRequest{
    Page:     &page,
    PageSize: &pageSize,
    Sorting:  sorting,
}

result, err := repo.ListWithPaging(ctx, req)
```

#### 字段选择（FieldMask）

```go
import "google.golang.org/protobuf/types/known/fieldmaskpb"

// 只返回 event_id 和 event_type
fieldMask := &fieldmaskpb.FieldMask{
    Paths: []string{"event_id", "event_type"},
}

req := &paginationV1.PagingRequest{
    Page:      &page,
    PageSize:  &pageSize,
    FieldMask: fieldMask,
}

result, err := repo.ListWithPaging(ctx, req)
```

---

### 7. 直接执行 SQL 查询

#### 自定义查询

```go
var results []any
err := client.Query(ctx, func() any {
    return &EventEntity{}
}, &results, 
    "SELECT * FROM events WHERE user_id = ? AND timestamp > ? ORDER BY timestamp DESC LIMIT 100",
    1001, time.Now().Add(-24*time.Hour),
)

if err != nil {
    log.Fatal(err)
}
```

#### 查询单行

```go
var event EventEntity
err := client.QueryRow(ctx, &event,
    "SELECT * FROM events WHERE event_id = ?",
    1,
)
```

#### 计数查询

```go
total, err := repo.Count(ctx, "user_id = ?", 1001)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Total events: %d\n", total)
```

---

### 8. 使用 BatchInserter（流式批量插入）

```go
import "github.com/tx7do/go-crud/clickhouse"

// 创建批量插入器
columns := []string{"event_id", "user_id", "event_type", "timestamp", "properties"}
inserter, err := clickhouse.NewBatchInserter(ctx, client.conn, "events", 1000, columns)
if err != nil {
    log.Fatal(err)
}
defer inserter.Close()  // 确保剩余数据被提交

// 流式添加数据
for i := 0; i < 10000; i++ {
    event := &EventEntity{
        EventID:   uint64(i),
        UserID:    1001,
        EventType: "page_view",
        Timestamp: time.Now(),
        Properties: `{"page": "/home"}`,
    }
    
    if err := inserter.Add(event); err != nil {
        log.Printf("Failed to add event: %v", err)
    }
}

// 手动刷新（可选，Close 会自动刷新）
if err := inserter.Flush(); err != nil {
    log.Printf("Failed to flush: %v", err)
}
```

**优点：**
- 自动分批提交（达到 batchSize 时）
- 线程安全（内部使用 mutex）
- 支持上下文取消
- 流式处理大数据集

---

## API 参考

### Client 方法

#### 连接管理

- `NewClient(opts...)` - 创建客户端
- `Close()` - 关闭连接
- `GetServerVersion()` - 获取服务器版本
- `CheckConnection(ctx)` - 检查连接状态

#### 数据写入

- `Insert(ctx, tableName, data)` - 插入单条数据
- `InsertMany(ctx, tableName, data)` - 批量插入多条
- `AsyncInsert(ctx, tableName, data, wait)` - 异步插入单条
- `AsyncInsertMany(ctx, tableName, data, wait)` - 异步批量插入
- `BatchInsert(ctx, tableName, data)` - 使用 PrepareBatch 批量插入
- `BatchStructs(ctx, query, data)` - 批量插入结构体

#### 数据查询

- `Query(ctx, creator, results, query, args...)` - 执行查询
- `QueryRow(ctx, dest, query, args...)` - 查询单行
- `Select(ctx, dest, query, args...)` - SELECT 查询封装
- `Exec(ctx, query, args...)` - 执行非查询语句

#### 工具方法

- `Count(ctx, baseWhere, whereArgs...)` - 计数查询（Repository 方法）

### Repository 方法

#### 查询方法

- `ListWithPaging(ctx, req)` - 分页列表查询（PagingRequest）
- `ListWithPagination(ctx, req)` - 分页列表查询（PaginationRequest）
- `Count(ctx, baseWhere, whereArgs...)` - 计数

#### 创建方法

- `Create(ctx, dto)` - 插入单条记录
- `BatchCreate(ctx, dtos)` - 批量插入

### Client 配置选项

- `WithOptions(config)` - 使用完整配置
- `WithDsn(dsn)` - 使用 DSN 字符串
- `WithAddresses(addrs...)` - 设置地址列表
- `WithUsername(username)` - 设置用户名
- `WithPassword(password)` - 设置密码
- `WithDatabase(database)` - 设置数据库名
- `WithScheme(scheme)` - 设置协议（http/https/native）
- `WithTLSConfig(tls)` - 设置 TLS 配置
- `WithHttpProxy(proxy)` - 设置 HTTP 代理
- `WithLogger(logger)` - 设置日志器
- `WithDebug(debug)` - 启用调试模式
- `WithDialTimeout(timeout)` - 设置拨号超时
- `WithReadTimeout(timeout)` - 设置读取超时
- `WithConnMaxLifetime(lifetime)` - 设置连接最大生命周期
- `WithMaxIdleConns(n)` - 设置最大空闲连接数
- `WithMaxOpenConns(n)` - 设置最大打开连接数
- `WithBlockBufferSize(size)` - 设置块缓冲区大小
- `WithCompressionMethod(method)` - 设置压缩方法（none/zstd/lz4/gzip等）
- `WithCompressionLevel(level)` - 设置压缩级别
- `WithMaxCompressionBuffer(size)` - 设置最大压缩缓冲区
- `WithConnectionOpenStrategy(strategy)` - 设置连接打开策略（in_order/round_robin/random）

### BatchInserter 方法

- `NewBatchInserter(ctx, conn, table, batchSize, columns)` - 创建批量插入器
- `Add(row)` - 添加数据行（达到 batchSize 自动提交）
- `Flush()` - 强制提交当前批次
- `Close()` - 关闭并提交剩余数据

## ClickHouse 特性

### 列式存储优势

| 特性 | 说明 | 适用场景 |
|------|------|----------|
| **高压缩率** | 同类型数据连续存储，压缩率高 | 海量数据存储 |
| **快速聚合** | 只需读取相关列，聚合速度快 | OLAP 分析 |
| **向量化执行** | SIMD 指令加速计算 | 复杂查询 |
| **稀疏索引** | 主键索引占用空间小 | 大范围扫描 |

### 推荐的表引擎

#### MergeTree（最常用）

```sql
CREATE TABLE events (
    event_id UInt64,
    user_id UInt64,
    event_type String,
    timestamp DateTime,
    properties String
) ENGINE = MergeTree()
ORDER BY (user_id, timestamp)
PRIMARY KEY user_id;
```

#### ReplacingMergeTree（去重）

```sql
CREATE TABLE users (
    user_id UInt64,
    name String,
    updated_at DateTime
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id;
```

#### SummingMergeTree（预聚合）

```sql
CREATE TABLE daily_stats (
    date Date,
    user_id UInt64,
    page_views UInt32,
    clicks UInt32
) ENGINE = SummingMergeTree()
ORDER BY (date, user_id);
```

### 数据类型映射

| Go 类型 | ClickHouse 类型 | 说明 |
|---------|----------------|------|
| `uint8` | `UInt8` | 无符号 8 位整数 |
| `uint16` | `UInt16` | 无符号 16 位整数 |
| `uint32` | `UInt32` | 无符号 32 位整数 |
| `uint64` | `UInt64` | 无符号 64 位整数 |
| `int8` | `Int8` | 有符号 8 位整数 |
| `int16` | `Int16` | 有符号 16 位整数 |
| `int32` | `Int32` | 有符号 32 位整数 |
| `int64` | `Int64` | 有符号 64 位整数 |
| `float32` | `Float32` | 32 位浮点数 |
| `float64` | `Float64` | 64 位浮点数 |
| `string` | `String` | 字符串 |
| `time.Time` | `DateTime` / `Date` | 日期时间 |
| `[]byte` | `String` | 二进制数据 |
| `bool` | `UInt8` | 布尔值（0/1） |

## 最佳实践

### 1. 选择合适的排序键

```sql
-- ✅ 好的做法：高频查询字段作为排序键
ORDER BY (user_id, timestamp)

-- ❌ 避免：随机字段作为排序键
ORDER BY (event_id)
```

**原因：** ClickHouse 使用稀疏索引，排序键决定了数据的物理存储顺序和索引效率。

---

### 2. 批量插入优于单条插入

```go
// ✅ 好的做法：批量插入（每秒可插入数十万条）
events := make([]any, 0, 1000)
for i := 0; i < 1000; i++ {
    events = append(events, &Event{...})
}
err := repo.BatchCreate(ctx, events)

// ❌ 避免：循环单条插入（性能差）
for i := 0; i < 1000; i++ {
    err := repo.Create(ctx, &Event{...})  // 慢！
}
```

**性能对比：**
- 单条插入：~1,000 条/秒
- 批量插入：~100,000 条/秒
- 异步插入：~500,000 条/秒

---

### 3. 使用异步插入提升吞吐量

```go
// 高吞吐场景：不等待确认
err := client.AsyncInsert(ctx, "events", event, false)

// 重要数据：等待确认
err := client.AsyncInsert(ctx, "events", event, true)
```

---

### 4. 合理设置分区键

```sql
-- 按天分区（适合时间序列数据）
CREATE TABLE events (
    ...
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (user_id, timestamp);

-- 按月分区（适合长期存储）
PARTITION BY toYYYYMM(timestamp)
```

**好处：**
- 快速删除旧数据（DROP PARTITION）
- 查询时跳过无关分区
- 并行处理不同分区

---

### 5. 使用物化视图预聚合

```sql
-- 创建物化视图
CREATE MATERIALIZED VIEW daily_events_mv
ENGINE = SummingMergeTree()
ORDER BY (date, user_id)
AS SELECT
    toDate(timestamp) AS date,
    user_id,
    count() AS event_count,
    sumIf(1, event_type = 'page_view') AS page_views,
    sumIf(1, event_type = 'click') AS clicks
FROM events
GROUP BY date, user_id;

-- 查询预聚合数据（快 100 倍）
SELECT * FROM daily_events_mv WHERE date = '2024-01-01';
```

---

### 6. 避免频繁的小批量插入

```go
// ✅ 好的做法：积累到一定数量再插入
batch := make([]any, 0, 1000)
for event := range eventChan {
    batch = append(batch, event)
    if len(batch) >= 1000 {
        repo.BatchCreate(ctx, batch)
        batch = batch[:0]
    }
}

// ❌ 避免：每条数据都插入
for event := range eventChan {
    repo.Create(ctx, event)  // 频繁小批量，性能差
}
```

---

### 7. 使用采样查询优化大数据集

```sql
-- 采样 10% 的数据（速度快 10 倍）
SELECT count() FROM events SAMPLE 0.1;

-- 在 Go 中使用
rows, err := client.Query(ctx, query, args...)
```

---

### 8. 监控查询性能

```go
// 启用调试模式查看查询详情
client, err := clickhouse.NewClient(
    clickhouse.WithDsn(dsn),
    clickhouse.WithDebug(true),  // 打印 SQL 和性能信息
)
```

---

## 性能优化

### 1. 连接池配置

```go
client, err := clickhouse.NewClient(
    clickhouse.WithDsn(dsn),
    clickhouse.WithMaxOpenConns(100),    // 根据并发调整
    clickhouse.WithMaxIdleConns(10),     // 保持少量空闲连接
    clickhouse.WithConnMaxLifetime(time.Hour),  // 定期重建连接
)
```

### 2. 压缩配置

```go
client, err := clickhouse.NewClient(
    clickhouse.WithDsn(dsn),
    clickhouse.WithCompressionMethod("zstd"),  // 高压缩率
    clickhouse.WithCompressionLevel(3),         // 平衡速度和压缩率
)
```

**压缩方法对比：**

| 方法 | 压缩率 | 速度 | 适用场景 |
|------|--------|------|----------|
| `none` | 无 | 最快 | CPU 受限 |
| `lz4` | 低 | 快 | 默认推荐 |
| `zstd` | 高 | 中等 | 存储受限 |
| `gzip` | 高 | 慢 | 归档数据 |

### 3. 批量大小调优

```go
// 根据数据大小调整批量
inserter, _ := clickhouse.NewBatchInserter(ctx, conn, "events", 1000, columns)
// 小数据：1000-5000
// 大数据：100-1000
```

### 4. 异步插入缓冲

```go
// 使用 AsyncInsert 时，客户端内部有缓冲区
// 可通过环境变量调整：
// CLICKHOUSE_ASYNC_INSERT_MAX_DATA_SIZE=10485760  (10MB)
// CLICKHOUSE_ASYNC_INSERT_BUSY_TIMEOUT=200        (200ms)
```

---

## 常见错误处理

### 错误类型

```go
import "github.com/tx7do/go-crud/clickhouse"

// 检查具体错误类型
if errors.Is(err, clickhouse.ErrClientNotInitialized) {
    // 客户端未初始化
} else if errors.Is(err, clickhouse.ErrQueryExecutionFailed) {
    // 查询执行失败
} else if errors.Is(err, clickhouse.ErrBatchPrepareFailed) {
    // 批量准备失败
}
```

### 常见错误及解决方案

| 错误 | 原因 | 解决方案 |
|------|------|----------|
| `connection refused` | ClickHouse 未启动 | 检查服务状态 |
| `unknown column` | 字段名不匹配 | 检查 `ch` 标签 |
| `type mismatch` | 类型不兼容 | 检查 Go 类型与 CH 类型映射 |
| `timeout exceeded` | 查询超时 | 增加 `ReadTimeout` 或优化查询 |
| `memory limit exceeded` | 内存不足 | 减少批量大小或使用采样 |

---

## 测试

运行测试：

```bash
go test -v ./clickhouse/...
```

运行特定测试：

```bash
go test -v ./clickhouse -run TestRepository
go test -v ./clickhouse -run TestClient
go test -v ./clickhouse -run TestBatch
```

---

## 示例项目

查看完整示例：
- [repository_test.go](./repository_test.go) - Repository 测试示例
- [client_test.go](./client_test.go) - Client 测试示例
- [utils_test.go](./utils_test.go) - 工具函数测试示例

---

## 依赖

- `github.com/ClickHouse/clickhouse-go/v2` - ClickHouse Go 客户端
- `github.com/tx7do/go-utils/mapper` - DTO/Entity 映射
- `github.com/tx7do/go-crud/api` - Protobuf 定义
- `github.com/tx7do/go-wind` - go-wind 框架（日志）

---

## 与其他包的对比

| 特性 | ClickHouse | MySQL/PostgreSQL | MongoDB |
|------|-----------|------------------|---------|
| 数据模型 | 列式存储 | 行式存储 | 文档存储 |
| 写入性能 | 极高（批量） | 中等 | 高 |
| 查询性能 | OLAP 极快 | OLTP 快 | 中等 |
| 聚合查询 | 极快 | 中等 | 慢 |
| 事务支持 | 有限 | 完整 | 完整 |
| 适用场景 | 分析、日志、指标 | 业务数据 | 灵活 schema |

---

## 常见问题 FAQ

### Q: ClickHouse 适合什么场景？

**A:** 
- ✅ 日志分析、用户行为分析、监控指标
- ✅ 海量数据聚合查询（亿级数据秒级响应）
- ✅ 时间序列数据分析
- ❌ 不适合高频事务处理、频繁单条更新

### Q: 为什么批量插入比单条插入快？

**A:** 
- ClickHouse 是列式存储，批量写入时可以按列组织数据
- 减少网络往返次数
- 更好地利用压缩和索引

### Q: 如何处理数据更新？

**A:** 
- ClickHouse 不适合频繁更新
- 使用 `ReplacingMergeTree` 引擎实现最终一致性
- 或者使用 `ALTER TABLE ... UPDATE`（不推荐，性能差）

### Q: 如何删除旧数据？

**A:** 
```sql
-- 方式 1：删除分区（最快）
ALTER TABLE events DROP PARTITION '20240101';

-- 方式 2：TTL 自动过期
CREATE TABLE events (
    ...
) ENGINE = MergeTree()
ORDER BY timestamp
TTL timestamp + INTERVAL 90 DAY;
```

### Q: 如何优化慢查询？

**A:**
- 检查是否使用了排序键字段
- 添加合适的索引（`INDEX` 子句）
- 使用采样查询（`SAMPLE`）
- 创建物化视图预聚合
- 避免 `SELECT *`，只查询需要的列

---

## 相关资源

- [ClickHouse 官方文档](https://clickhouse.com/docs/)
- [ClickHouse Go Client](https://github.com/ClickHouse/clickhouse-go)
- [ClickHouse 性能优化指南](https://clickhouse.com/docs/en/operations/performance)

---

## 许可证

本项目采用 MIT 许可证。
