# Apache Doris Package

基于 [Apache Doris](https://doris.apache.org/) 的 OLAP 数据库访问层封装，提供完整的数据写入、查询、分页、过滤、排序和 Stream Load 功能。

## 特性

- ✅ **泛型支持** - 完全基于 Go 泛型，类型安全
- ✅ **高性能 OLAP** - 专为实时分析场景优化
- ✅ **MySQL 协议兼容** - 使用标准 MySQL 驱动连接
- ✅ **完整 CRUD** - 创建、查询、更新、删除操作
- ✅ **多种分页** - 支持 Offset、Page、Token 三种分页方式
- ✅ **结构化过滤** - 支持复杂的过滤表达式
- ✅ **灵活排序** - 支持多字段排序
- ✅ **字段选择** - 支持 FieldMask 选择返回字段
- ✅ **Stream Load** - HTTP 接口实时导入大量数据
- ✅ **批量插入** - 高效的批量数据写入
- ✅ **Session 管理** - 会话级变量控制（内存限制、时区等）
- ✅ **事务支持** - 在同一连接上设置 session 并执行事务
- ✅ **DTO/Entity 映射** - 自动 DTO 和 Entity 转换
- ✅ **Kratos 日志集成** - 内置日志支持

## 什么是 Doris

Apache Doris（前身为 Apache Incubator 中的 Doris，企业名为 Palo）是一款面向实时分析（OLAP）的分布式列式数据库，主要特点包括：

- 兼容 MySQL 协议：支持通过 MySQL 客户端或驱动连接（便于集成现有工具）。
- 高性能分析：支持列式存储、向量化执行、MPP 调度（多副本分布式查询引擎）以提高分析查询吞吐。
- 多种写入方式：支持批量导入、Stream Load（实时导入）、Broker Load 等多种数据接入方式。
- 丰富的部署模式：单机快速试验、集群部署（FE/BE）及云环境部署方案。

适合用例：交互式分析、仪表盘、近实时的 OLAP 场景，例如日志/事件分析、时序分析与 BI 报表。

## 架构概览

- FE（Frontend）：负责元数据管理、解析 SQL、全局调度，提供对外的 SQL/HTTP 接口（例如 Stream Load 的 HTTP 接口通常暴露在 FE 上）。
- BE（Backend）：负责数据存储（列式存储）、实际的查询执行（扫描、聚合等）。

通常一个集群会有多个 FE（高可用）和多个 BE（水平扩展存储和计算）。

## 关键特性

- MySQL 协议支持 -> 可用 MySQL 驱动/客户端访问。
- Stream Load -> 通过 HTTP 将 CSV/JSON/其他格式数据实时推送到表中（常用于近实时导入）。
- 支持会话级变量（SET SESSION ...），可以控制 query plan、内存限制、时区等执行行为。
- 支持事务语义（有限度，主要用于小范围原子操作，注意并非全部场景下的强一致 OLTP）。

## 快速开始

### 1. 安装依赖

```bash
go get github.com/tx7do/go-crud/doris
```

### 2. 定义 Entity 和 DTO

**Entity (Doris 表结构):**

```go
type EventEntity struct {
    EventID   uint64    `db:"event_id" json:"event_id"`
    UserID    uint64    `db:"user_id" json:"user_id"`
    EventType string    `db:"event_type" json:"event_type"`
    Timestamp time.Time `db:"timestamp" json:"timestamp"`
    Properties string   `db:"properties" json:"properties"`
}
```

**注意：** Doris 使用 `db` 标签映射字段名（sqlx 标准）。

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
    "github.com/tx7do/go-crud/doris"
)

// 方式 1：使用 DSN
client, err := doris.NewClient(
    doris.WithDSN("user:pass@tcp(host:9030)/dbname"),
    doris.WithMaxOpenConns(100),
    doris.WithMaxIdleConns(10),
    doris.WithConnMaxLifetime(time.Hour),
)

// 方式 2：配置 Stream Load
client, err := doris.NewClient(
    doris.WithDSN("user:pass@tcp(fe-host:9030)/dbname"),
    doris.WithStreamLoadEndpoint("http://fe-host:8030"),
    doris.WithStreamLoadAuth("user", "password"),
    doris.WithStreamLoadTimeout(5*time.Minute),
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
    "github.com/tx7do/go-crud/doris"
)

// 创建 Mapper
m := mapper.NewCopierMapper[Event, EventEntity]()

// 创建 Repository
repo := doris.NewRepository[Event, EventEntity](
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

#### Stream Load（高性能实时导入）

```go
import (
    "strings"
    "github.com/tx7do/go-crud/doris"
)

// CSV 格式数据
csvData := strings.NewReader("1,1001,page_view,2024-01-01 12:00:00,{\"page\":\"/home\"}\n" +
                              "2,1002,click,2024-01-01 12:01:00,{\"page\":\"/product\"}\n")

params := map[string]string{
    "columns": "event_id,user_id,event_type,timestamp,properties",
    "format":  "csv",
    "label":   "load_20240101_001",  // 唯一标签，用于去重
}

body, status, err := client.StreamLoad(ctx, "mydb", "events", params, csvData)
if err != nil {
    log.Fatalf("Stream load failed: %v, status: %d", err, status)
}

fmt.Printf("Stream load response: %s\n", string(body))
```

**Stream Load 参数说明：**

| 参数 | 说明 | 示例 |
|------|------|------|
| `columns` | 列名列表 | `"id,name,age"` |
| `format` | 数据格式 | `csv`, `json`, `parquet` |
| `label` | 导入标签（唯一） | `"load_001"` |
| `column_separator` | 列分隔符 | `","` (CSV) |
| `line_delimiter` | 行分隔符 | `"\n"` |
| `max_filter_ratio` | 最大过滤比例 | `"0.1"` (10%) |
| `strict_mode` | 严格模式 | `"true"` |
| `timezone` | 时区 | `"Asia/Shanghai"` |

**JSON 格式示例：**

```go
jsonData := strings.NewReader(`{"event_id":1,"user_id":1001,"event_type":"page_view"}
{"event_id":2,"user_id":1002,"event_type":"click"}`)

params := map[string]string{
    "format": "json",
    "label":  "load_json_001",
}

body, status, err := client.StreamLoad(ctx, "mydb", "events", params, jsonData)
```

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

### 7. Session 管理

Doris 支持会话级变量，可以控制查询行为（内存限制、时区、SQL 模式等）。

#### 设置 Session 变量

```go
// 在连接池上设置（不保证后续查询使用同一连接）
vars := map[string]string{
    "exec_mem_limit": "4G",           // 限制查询内存
    "time_zone":      "Asia/Shanghai", // 设置时区
    "sql_select_limit": "10000",      // 限制返回行数
}

err := client.SetSessionVars(ctx, vars)
```

**注意：** `SetSessionVars` 在连接池上执行，SET 仅影响执行该语句的物理连接。如果需要保证后续查询受 session 影响，请使用 `RunWithSession` 或 `WithTxWithSession`。

#### 在单一连接上执行（推荐）

```go
vars := map[string]string{
    "exec_mem_limit": "4G",
    "time_zone":      "Asia/Shanghai",
}

err := client.RunWithSession(ctx, vars, func(ctx context.Context, conn *sql.Conn) error {
    // 在这个连接上的所有查询都受 session 变量影响
    var count uint64
    err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM events").Scan(&count)
    return err
})
```

#### 在事务中设置 Session

```go
vars := map[string]string{
    "exec_mem_limit": "4G",
    "time_zone":      "Asia/Shanghai",
}

err := client.WithTxWithSession(ctx, vars, nil, func(tx *sql.Tx) error {
    // 在事务内执行受 session 影响的语句
    _, err := tx.ExecContext(ctx, "INSERT INTO events (...) VALUES (...)")
    if err != nil {
        return err
    }
    
    _, err = tx.ExecContext(ctx, "UPDATE stats SET count = count + 1")
    return err
})
```

**常用 Session 变量：**

| 变量 | 说明 | 示例 |
|------|------|------|
| `exec_mem_limit` | 单次查询内存限制 | `"4G"`, `"1024M"` |
| `time_zone` | 时区 | `"Asia/Shanghai"`, `"UTC"` |
| `sql_select_limit` | 默认返回行数限制 | `"10000"` |
| `enable_profile` | 开启查询分析 | `"true"`, `"false"` |
| `query_timeout` | 查询超时（秒） | `"300"` |
| `parallel_fragment_exec_instance_num` | 并行度 | `"4"` |

---

### 8. 事务管理

#### 基本事务

```go
err := client.WithTx(ctx, nil, func(tx *sqlx.Tx) error {
    // 执行多个操作
    _, err := tx.ExecContext(ctx, "INSERT INTO events (...) VALUES (...)")
    if err != nil {
        return err
    }
    
    _, err = tx.ExecContext(ctx, "UPDATE stats SET count = count + 1")
    return err
})

if err != nil {
    log.Printf("Transaction failed: %v", err)
}
```

#### 手动控制事务

```go
tx, err := client.BeginTx(ctx, nil)
if err != nil {
    log.Fatal(err)
}

// 确保回滚
defer func() {
    if p := recover(); p != nil {
        _ = tx.Rollback()
        panic(p)
    }
}()

// 执行操作
_, err = tx.ExecContext(ctx, "INSERT INTO ...")
if err != nil {
    _ = tx.Rollback()
    return err
}

// 提交
if err = tx.Commit(); err != nil {
    return err
}
```

---

### 9. 直接执行 SQL 查询

#### 自定义查询

```go
var events []EventEntity
err := client.SelectContext(ctx, &events,
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
err := client.GetContext(ctx, &event,
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

## API 参考

### Client 方法

#### 连接管理

- `NewClient(opts...)` - 创建客户端
- `Close()` - 关闭连接
- `DB()` - 获取底层 *sqlx.DB

#### 数据写入

- `Insert(ctx, table, entity)` - 插入单条记录
- `BatchInsert(ctx, table, columns, rows)` - 批量插入（二维数组）
- `BatchInsertStruct(ctx, table, structArr)` - 批量插入结构体切片
- `BatchInsertProto(ctx, table, protoArr)` - 批量插入 Protobuf 消息切片
- `StreamLoad(ctx, db, table, params, data)` - Stream Load HTTP 导入

#### 数据查询

- `Exec(query, args...)` - 执行非查询语句
- `ExecContext(ctx, query, args...)` - 带 Context 执行
- `Get(dest, query, args...)` - 查询单行
- `GetContext(ctx, dest, query, args...)` - 带 Context 查询单行
- `Select(dest, query, args...)` - 查询多行
- `SelectContext(ctx, dest, query, args...)` - 带 Context 查询多行
- `Query(ctx, creator, results, query, args...)` - 通用查询（creator 模式）

#### Session 管理

- `SetSession(ctx, stmt, args...)` - 设置单个 session 变量
- `SetSQLMode(ctx, mode)` - 设置 SQL 模式
- `SetSessionVars(ctx, vars)` - 设置多个 session 变量
- `WithSessionConn(ctx, sessionStmts, fn)` - 在单一连接上执行 session 语句和回调
- `RunWithSession(ctx, vars, fn)` - 设置 session 并在同一连接上执行回调

#### 事务管理

- `BeginTx(ctx, opts)` - 开始事务
- `WithTx(ctx, opts, fn)` - 在事务中执行回调（自动 Commit/Rollback）
- `RunInTx(ctx, opts, fn)` - WithTx 的别名
- `BeginTxWithSession(ctx, vars, opts)` - 设置 session 并开始事务
- `WithTxWithSession(ctx, vars, opts, fn)` - 设置 session、开始事务并执行回调

### Repository 方法

#### 查询方法

- `ListWithPaging(ctx, req)` - 分页列表查询（PagingRequest）
- `ListWithPagination(ctx, req)` - 分页列表查询（PaginationRequest）
- `Count(ctx, baseWhere, whereArgs...)` - 计数

#### 创建方法

- `Create(ctx, dto)` - 插入单条记录
- `BatchCreate(ctx, dtos)` - 批量插入

### Client 配置选项

- `WithDSN(dsn)` - 设置 DSN（必填）
- `WithDB(db)` - 注入现有的 *sqlx.DB
- `WithLogger(logger)` - 设置日志器
- `WithMaxOpenConns(n)` - 设置最大打开连接数
- `WithMaxIdleConns(n)` - 设置最大空闲连接数
- `WithConnMaxLifetime(d)` - 设置连接最大生命周期

#### Stream Load 配置

- `WithStreamLoadEndpoint(endpoint)` - 设置 FE 地址（如 `http://fe-host:8030`）
- `WithStreamLoadAuth(username, password)` - 设置认证凭据
- `WithHTTPClient(hc)` - 注入自定义 http.Client
- `WithStreamLoadTimeout(d)` - 设置超时时间
- `WithStreamLoadMethod(method)` - 设置 HTTP 方法（POST/PUT，默认 PUT）

## Doris 特性

### 架构概览

- **FE (Frontend)**：负责元数据管理、SQL 解析、全局调度，提供 SQL/HTTP 接口
- **BE (Backend)**：负责数据存储（列式存储）、实际查询执行

通常一个集群会有多个 FE（高可用）和多个 BE（水平扩展存储和计算）。

### 关键特性

| 特性 | 说明 |
|------|------|
| MySQL 协议兼容 | 可用 MySQL 驱动/客户端访问 |
| Stream Load | 通过 HTTP 实时推送 CSV/JSON 数据到表中 |
| Session 变量 | 控制 query plan、内存限制、时区等 |
| 事务语义 | 有限度的事务支持（小范围原子操作） |
| 向量化执行 | SIMD 指令加速计算 |
| MPP 调度 | 多副本分布式查询引擎 |

### 适用的场景

- ✅ 交互式分析、仪表盘
- ✅ 近实时的 OLAP 场景
- ✅ 日志/事件分析
- ✅ 时序分析与 BI 报表
- ❌ 高频事务处理（OLTP）
- ❌ 频繁单条更新

---

## 最佳实践

### 1. 使用 Stream Load 进行大数据量导入

```go
// ✅ 好的做法：Stream Load（每秒可导入数十万条）
csvData := strings.NewReader(largeCSVData)
params := map[string]string{
    "columns": "event_id,user_id,event_type,timestamp",
    "format":  "csv",
    "label":   fmt.Sprintf("load_%d", time.Now().Unix()),
}
body, status, err := client.StreamLoad(ctx, "mydb", "events", params, csvData)

// ❌ 避免：循环单条插入（性能差）
for _, event := range events {
    repo.Create(ctx, event)  // 慢！
}
```

**性能对比：**
- 单条插入：~1,000 条/秒
- 批量插入：~10,000 条/秒
- Stream Load：~500,000 条/秒

---

### 2. 合理设置 Session 变量

```go
// ✅ 好的做法：在单一连接上设置 session
vars := map[string]string{
    "exec_mem_limit": "4G",
    "time_zone":      "Asia/Shanghai",
}

err := client.RunWithSession(ctx, vars, func(ctx context.Context, conn *sql.Conn) error {
    // 所有查询都受 session 影响
    var count uint64
    return conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM events").Scan(&count)
})

// ❌ 避免：在连接池上设置（不保证后续查询使用同一连接）
client.SetSessionVars(ctx, vars)
result, _ := client.SelectContext(ctx, &events, "SELECT ...")  // 可能不受 session 影响
```

---

### 3. 批量插入优化

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

### 4. 使用唯一 Label 防止重复导入

```go
// ✅ 好的做法：使用唯一 label
label := fmt.Sprintf("load_%s_%d", time.Now().Format("20060102"), uniqueID)
params := map[string]string{
    "label": label,
}

// Doris 会根据 label 去重，相同 label 的导入只执行一次
body, status, err := client.StreamLoad(ctx, "mydb", "events", params, data)
```

---

### 5. 监控 Stream Load 结果

```go
body, status, err := client.StreamLoad(ctx, "mydb", "events", params, data)
if err != nil {
    log.Fatalf("Stream load failed: %v", err)
}

// 解析响应 JSON
var result map[string]any
json.Unmarshal(body, &result)

fmt.Printf("Status: %v\n", result["Status"])        // "Success" or "Fail"
fmt.Printf("Loaded Rows: %v\n", result["LoadedRows"]) // 成功导入的行数
fmt.Printf("Filtered Rows: %v\n", result["FilteredRows"]) // 过滤的行数
fmt.Printf("Error URL: %v\n", result["ErrorURL"])     // 错误详情 URL
```

---

### 6. 合理设置内存限制

```go
// 大查询：增加内存限制
vars := map[string]string{
    "exec_mem_limit": "8G",
}

// 小查询：减少内存限制
vars := map[string]string{
    "exec_mem_limit": "1G",
}

err := client.RunWithSession(ctx, vars, func(ctx context.Context, conn *sql.Conn) error {
    // 执行查询
    return client.SelectContext(ctx, &results, "SELECT ...")
})
```

---

### 7. 使用事务保证原子性

```go
// ✅ 好的做法：相关操作放在同一事务中
err := client.WithTx(ctx, nil, func(tx *sqlx.Tx) error {
    // 插入事件
    _, err := tx.ExecContext(ctx, "INSERT INTO events (...) VALUES (...)")
    if err != nil {
        return err
    }
    
    // 更新统计
    _, err = tx.ExecContext(ctx, "UPDATE daily_stats SET count = count + 1 WHERE date = ?", today)
    return err
})

if err != nil {
    log.Printf("Transaction failed, all changes rolled back")
}
```

---

### 8. 设置查询超时

```go
// 防止慢查询占用资源
vars := map[string]string{
    "query_timeout": "60",  // 60 秒超时
}

ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
defer cancel()

err := client.RunWithSession(ctx, vars, func(ctx context.Context, conn *sql.Conn) error {
    return client.SelectContext(ctx, &results, "SELECT ...")
})
```

---

## Docker 快速部署

下面给出快速启动 FE/BE 的示例（同仓库原始内容）：

### Docker run

```bash
docker pull apache/doris:be-4.0.4
docker pull apache/doris:fe-4.0.4

docker network create --driver bridge --subnet=172.20.80.0/24 doris-network

mkdir -p /data/fe/{doris-meta,conf,log}
mkdir -p /data/be/{storage,conf,log}
chmod -R 777 /data/fe /data/be

docker run -itd \
    --name=doris-fe \
    --env FE_SERVERS="fe1:172.20.80.2:9010" \
    --env FE_ID=1 \
    -p 8030:8030 \
    -p 9030:9030 \
    -p 9010:9010 \
    -v /data/fe/doris-meta:/opt/apache-doris/fe/doris-meta \
    -v /data/fe/conf:/opt/apache-doris/fe/conf \
    -v /data/fe/log:/opt/apache-doris/fe/log \
    --network=doris-network \
    --ip=172.20.80.2 \
    apache/doris:fe-4.0.4

docker run -itd \
    --name=doris-be \
    --env FE_SERVERS="fe1:172.20.80.2:9010" \
    --env BE_ADDR="172.20.80.3:9050" \
    -p 8040:8040 \
    -p 9050:9050 \
    -v /data/be/storage:/opt/apache-doris/be/storage \
    -v /data/be/conf:/opt/apache-doris/be/conf \
    -v /data/be/log:/opt/apache-doris/be/log \
    --network=doris-network \
    --ip=172.20.80.3 \
    apache/doris:be-4.0.4
```

### Docker compose

```yaml
version: "3"
networks:
  custom_network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.80.0/24

services:
  fe:
    image: apache/doris:fe-${DORIS_QUICK_START_VERSION}
    hostname: fe
    ports:
      - 8030:8030
      - 9030:9030
      - 9010:9010
    environment:
      - FE_SERVERS=fe1:172.20.80.2:9010
      - FE_ID=1
    networks:
      custom_network:
        ipv4_address: 172.20.80.2

  be:
    image: apache/doris:be-${DORIS_QUICK_START_VERSION}
    hostname: be
    ports:
      - 8040:8040
      - 9050:9050
    environment:
      - FE_SERVERS=fe1:172.20.80.2:9010
      - BE_ADDR=172.20.80.3:9050
    depends_on:
      - fe
    networks:
      custom_network:
        ipv4_address: 172.20.80.3
```

## 测试

运行测试：

```bash
go test -v ./doris/...
```

运行特定测试：

```bash
go test -v ./doris -run TestRepository
go test -v ./doris -run TestClient
go test -v ./doris -run TestStreamLoad
```

---

## 示例项目

查看完整示例：
- [repository_test.go](./repository_test.go) - Repository 测试示例
- [client_test.go](./client_test.go) - Client 测试示例
- [utils_test.go](./utils_test.go) - 工具函数测试示例
- [finances.sql](./finances.sql) - SQL 建表示例

---

## 依赖

- `github.com/go-sql-driver/mysql` - MySQL 驱动（Doris 使用 MySQL 协议）
- `github.com/jmoiron/sqlx` - SQL 扩展库
- `github.com/tx7do/go-utils/mapper` - DTO/Entity 映射
- `github.com/tx7do/go-crud/api` - Protobuf 定义
- `github.com/go-kratos/kratos/v2` - Kratos 框架（日志）

---

## 与其他包的对比

| 特性 | Doris | ClickHouse | MySQL/PostgreSQL |
|------|-------|-----------|------------------|
| 数据模型 | 列式存储 | 列式存储 | 行式存储 |
| 写入性能 | 高（Stream Load） | 极高（批量） | 中等 |
| 查询性能 | OLAP 快 | OLAP 极快 | OLTP 快 |
| 聚合查询 | 快 | 极快 | 中等 |
| 事务支持 | 有限 | 有限 | 完整 |
| 实时导入 | Stream Load | AsyncInsert | INSERT |
| 适用场景 | 实时分析、BI | 日志、指标 | 业务数据 |

---

## 常见问题 FAQ

### Q: Doris 适合什么场景？

**A:** 
- ✅ 实时分析、BI 报表、仪表盘
- ✅ 日志/事件分析、用户行为分析
- ✅ 时序数据分析
- ❌ 不适合高频事务处理、频繁单条更新

### Q: Stream Load 和批量插入有什么区别？

**A:** 
- **Stream Load**：HTTP 接口，适合大数据量（百万级），性能最优
- **批量插入**：MySQL 协议，适合中小数据量（千级），实现简单

### Q: 为什么 Session 变量有时不生效？

**A:** 
因为连接池会从池中获取不同的物理连接。请使用 `RunWithSession` 或 `WithTxWithSession` 保证在同一连接上执行。

```go
// ✅ 正确：保证 session 生效
err := client.RunWithSession(ctx, vars, func(ctx context.Context, conn *sql.Conn) error {
    // 在这个连接上的所有查询都受 session 影响
    return client.SelectContext(ctx, &results, "SELECT ...")
})

// ❌ 错误：session 可能不生效
client.SetSessionVars(ctx, vars)
client.SelectContext(ctx, &results, "SELECT ...")  // 可能使用不同连接
```

### Q: 如何处理 Stream Load 失败？

**A:** 
检查返回的 JSON 响应中的 `ErrorURL` 字段，访问该 URL 查看详细错误信息。

```go
body, status, err := client.StreamLoad(ctx, "mydb", "events", params, data)
if err != nil {
    var result map[string]any
    json.Unmarshal(body, &result)
    
    if errorURL, ok := result["ErrorURL"].(string); ok {
        log.Printf("Check error details at: %s", errorURL)
    }
}
```

### Q: 如何优化慢查询？

**A:**
- 增加 `exec_mem_limit` 内存限制
- 增加 `parallel_fragment_exec_instance_num` 并行度
- 创建合适的索引（Rollup Table）
- 避免 `SELECT *`，只查询需要的列
- 使用分区表跳过无关分区

### Q: 如何删除旧数据？

**A:** 
```sql
-- 方式 1：DELETE（性能较差）
DELETE FROM events WHERE timestamp < '2024-01-01';

-- 方式 2：DROP 分区（推荐，速度快）
ALTER TABLE events DROP PARTITION p202401;

-- 方式 3：TTL 自动过期（建表时指定）
CREATE TABLE events (
    ...
) ENGINE=OLAP
PARTITION BY RANGE(timestamp) (...)
DISTRIBUTED BY HASH(user_id) BUCKETS 10
PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.end" = "-90"  -- 保留最近 90 天
);
```

---

## 参考链接

- [Apache Doris 官方网站](https://doris.apache.org/)
- [Stream Load 文档](https://doris.apache.org/docs/zh-CN/latest/administrator-guide/import/stream-load/)
- [Doris GitHub](https://github.com/apache/doris)
- [Session 变量文档](https://doris.apache.org/docs/zh-CN/admin-manual/system-administration/session-variables/)

---

## 许可证

本项目采用 MIT 许可证。 
