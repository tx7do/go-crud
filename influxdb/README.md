# InfluxDB Package

基于 [InfluxDB 3.x](https://docs.influxdata.com/influxdb/v3/) 的时序数据访问层封装，提供完整的数据写入、查询、分页、过滤和排序功能。

## 特性

- ✅ **泛型支持** - 完全基于 Go 泛型，类型安全
- ✅ **时序数据优化** - 专为时间序列数据设计
- ✅ **多种查询方式** - 支持 InfluxQL、SQL、Flux 查询语言
- ✅ **结构化过滤** - 支持复杂的过滤表达式
- ✅ **灵活排序** - 支持多字段排序
- ✅ **字段选择** - 支持 FieldMask 选择返回字段
- ✅ **多种分页** - 支持 Offset、Page、Token 三种分页方式
- ✅ **批量写入** - 高效批量插入数据点
- ✅ **参数化查询** - 防止注入攻击
- ✅ **DTO/Point 映射** - 自动 DTO 和 InfluxDB Point 转换
- ✅ **Kratos 日志集成** - 内置日志支持

## Docker 部署

### Pull Image

```bash
docker pull bitnami/influxdb:latest
```

### InfluxDB 2.x

```bash
docker run -itd \
    --name influxdb2-server \
    -p 8086:8086 \
    -e INFLUXDB_HTTP_AUTH_ENABLED=true \
    -e INFLUXDB_ADMIN_USER=admin \
    -e INFLUXDB_ADMIN_USER_PASSWORD=123456789 \
    -e INFLUXDB_ADMIN_USER_TOKEN=admintoken123 \
    -e INFLUXDB_DB=my_database \
    bitnami/influxdb:2.7.11
```

创建管理员用户 SQL 脚本：

```sql
create user "admin" with password '123456789' with all privileges
```

管理后台: <http://localhost:8086/>

### InfluxDB 3.x

```bash
# InfluxDB 3.x 服务器
docker run -itd \
    --name influxdb3-server \
    -p 8181:8181 \
    -e INFLUXDB_NODE_ID=0 \
    -e INFLUXDB_HTTP_PORT_NUMBER=8181 \
    -e INFLUXDB_HTTP_AUTH_ENABLED=true \
    -e INFLUXDB_CREATE_ADMIN_TOKEN=yes \
    -e INFLUXDB_DB=my_database \
    bitnami/influxdb:latest

# InfluxDB Explorer 管理后台
docker run -itd \
  --name influxdb3-explorer \
  -p 8888:80 \
  -p 8889:8888 \
  quay.io/influxdb/influxdb3-explorer:latest \
  --mode=admin
```

这个版本分离出来一个管理后台 InfluxDB Explorer：<http://localhost:8888/>

在管理后台填写：`http://host.docker.internal:8181`

## 快速开始

### 1. 安装依赖

```bash
go get github.com/tx7do/go-crud/influxdb
```

### 2. 定义 DTO (Protobuf)

```protobuf
syntax = "proto3";

package metrics;

option go_package = "github.com/example/metrics;metrics";

import "google/protobuf/timestamp.proto";

message SensorData {
  string sensor_id = 1;
  double temperature = 2;
  double humidity = 3;
  google.protobuf.Timestamp timestamp = 4;
}
```

### 3. 创建 Client

```go
import (
    "github.com/tx7do/go-crud/influxdb"
)

// 创建 InfluxDB 客户端
client, err := influxdb.NewClient(
    influxdb.WithHost("http://localhost:8181"),
    influxdb.WithToken("your-auth-token"),
    influxdb.WithOrganization("my-org"),
    influxdb.WithDatabase("my-database"),
    influxdb.WithQueryTimeout(30 * time.Second),
    influxdb.WithWriteTimeout(30 * time.Second),
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
    "github.com/tx7do/go-crud/influxdb"
)

// 创建 Mapper（需要实现 Point <-> DTO 转换逻辑）
m := mapper.NewCopierMapper[SensorData, influxdb3.Point]()

// 创建 Repository
repo := influxdb.NewRepository[SensorData, influxdb3.Point](
    client,
    "sensor_data",  // measurement 名称
    logger,
)
```

### 5. 写入数据

#### 单条插入

```go
import "github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"

ctx := context.Background()

// 创建数据点
point := influxdb3.NewPoint(
    "sensor_data",
    map[string]string{
        "sensor_id": "sensor-001",
    },
    map[string]interface{}{
        "temperature": 25.5,
        "humidity":    60.2,
    },
    time.Now(),
)

// 插入数据
err := repo.Create(ctx, &SensorData{
    SensorId:    "sensor-001",
    Temperature: 25.5,
    Humidity:    60.2,
    Timestamp:   timestamppb.Now(),
})
if err != nil {
    log.Fatal(err)
}
```

#### 批量插入

```go
points := []*influxdb3.Point{
    influxdb3.NewPoint("sensor_data", tags1, fields1, time1),
    influxdb3.NewPoint("sensor_data", tags2, fields2, time2),
    influxdb3.NewPoint("sensor_data", tags3, fields3, time3),
}

err := client.BatchInsert(ctx, points)
if err != nil {
    log.Fatal(err)
}
```

### 6. 查询数据

#### 基本查询

```go
import paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"

page := uint32(1)
pageSize := uint32(10)
req := &paginationV1.PagingRequest{
    Page:     &page,
    PageSize: &pageSize,
}

results, total, err := repo.ListWithPaging(ctx, req)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Total: %d, Results: %d\n", total, len(results))
for _, data := range results {
    fmt.Printf("Sensor: %s, Temp: %.2f\n", data.SensorId, data.Temperature)
}
```

#### 带过滤的查询

```go
// 构建过滤条件
filterExpr := &paginationV1.FilterExpr{
    // 例如：temperature > 25 AND sensor_id = 'sensor-001'
}

req := &paginationV1.PagingRequest{
    Page:         &page,
    PageSize:     &pageSize,
    FilteringType: &paginationV1.PagingRequest_FilterExpr{
        FilterExpr: filterExpr,
    },
}

results, total, err := repo.ListWithPaging(ctx, req)
```

#### 带排序的查询

```go
// 按时间降序排列
sorting := []*paginationV1.Sorting{
    {Field: "time", Direction: paginationV1.Sorting_DESC},
}

req := &paginationV1.PagingRequest{
    Page:     &page,
    PageSize: &pageSize,
    Sorting:  sorting,
}

results, total, err := repo.ListWithPaging(ctx, req)
```

#### 字段选择（FieldMask）

```go
import "google.golang.org/protobuf/types/known/fieldmaskpb"

// 只返回 sensor_id 和 temperature
fieldMask := &fieldmaskpb.FieldMask{
    Paths: []string{"sensor_id", "temperature"},
}

req := &paginationV1.PagingRequest{
    Page:      &page,
    PageSize:  &pageSize,
    FieldMask: fieldMask,
}

results, total, err := repo.ListWithPaging(ctx, req)
```

### 7. 直接执行查询

#### InfluxQL 查询

```go
query := `SELECT * FROM sensor_data WHERE time > now() - 1h`
iterator, err := client.Query(ctx, query)
if err != nil {
    log.Fatal(err)
}

for iterator.Next() {
    row := iterator.Value()
    fmt.Printf("Row: %v\n", row)
}
```

#### SQL 查询（InfluxDB 3.x）

```go
query := `SELECT * FROM sensor_data WHERE time > NOW() - INTERVAL '1 hour'`
iterator, err := client.ExecSQLQuery(ctx, query)
if err != nil {
    log.Fatal(err)
}

for iterator.Next() {
    row := iterator.Value()
    fmt.Printf("Row: %v\n", row)
}
```

#### 参数化查询

```go
filters := map[string]interface{}{
    "sensor_id": "sensor-001",
    "temperature": 25.0,
}
operators := map[string]string{
    "sensor_id": "=",
    "temperature": ">",
}
fields := []string{"sensor_id", "temperature", "humidity", "time"}

iterator, err := client.QueryWithParams(ctx, "sensor_data", filters, operators, fields)
if err != nil {
    log.Fatal(err)
}
```

### 8. 计数和存在性检查

```go
// 计数
query := `SELECT COUNT(*) FROM sensor_data WHERE time > now() - 1h`
count, err := client.Count(ctx, query)
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Count: %d\n", count)

// 存在性检查
query := `SELECT * FROM sensor_data WHERE sensor_id = 'sensor-001' LIMIT 1`
exists, err := client.Exist(ctx, query)
if err != nil {
    log.Fatal(err)
}
if exists {
    fmt.Println("Data exists")
}
```

## API 参考

### Client 方法

#### 连接管理

- `NewClient(opts...)` - 创建客户端
- `Close()` - 关闭连接
- `ServerVersion()` - 获取服务器版本

#### 数据写入

- `Insert(ctx, point)` - 插入单条数据
- `BatchInsert(ctx, points)` - 批量插入数据
- `WritePointsStrict(ctx, points)` - 严格类型批量写入
- `WritePoints(ctx, pts)` - 通用批量写入

#### 数据查询

- `Query(ctx, query)` - 执行 InfluxQL 查询
- `QueryWithParams(ctx, table, filters, operators, fields)` - 参数化查询
- `ExecInfluxQLQuery(ctx, query, opts...)` - 执行 InfluxQL 查询（底层）
- `ExecSQLQuery(ctx, query, opts...)` - 执行 SQL 查询（InfluxDB 3.x）

#### 工具方法

- `Count(ctx, query)` - 计数查询
- `Exist(ctx, query)` - 存在性检查

### Repository 方法

#### 查询方法

- `ListWithPaging(ctx, req)` - 分页列表查询（PagingRequest）
- `ListWithPagination(ctx, req)` - 分页列表查询（PaginationRequest）

#### 创建方法

- `Create(ctx, dto)` - 插入单条记录

### Client 配置选项

- `WithOptions(config)` - 使用完整配置
- `WithHost(host)` - 设置主机地址
- `WithToken(token)` - 设置认证 Token
- `WithOrganization(org)` - 设置组织名称
- `WithDatabase(db)` - 设置数据库名称
- `WithTLSConfig(tls)` - 设置 TLS 配置
- `WithLogger(logger)` - 设置日志器
- `WithWriteTimeout(timeout)` - 设置写入超时
- `WithQueryTimeout(timeout)` - 设置查询超时
- `WithIdleConnectionTimeout(timeout)` - 设置空闲连接超时
- `WithMaxIdleConnections(max)` - 设置最大空闲连接数
- `WithAuthScheme(scheme)` - 设置认证方案

### 工具函数

#### 查询构建

- `BuildQuery(table, filters, operators, fields)` - 构建查询语句和参数
- `BuildQueryWithParams(table, filters, operators, fields)` - 构建参数化查询

#### Point 数据提取

- `GetPointTag(point, name)` - 获取字符串 Tag
- `GetBoolPointTag(point, name)` - 获取布尔 Tag
- `GetUint32PointTag(point, name)` - 获取 Uint32 Tag
- `GetUint64PointTag(point, name)` - 获取 Uint64 Tag
- `GetEnumPointTag[T](point, name, valueMap)` - 获取枚举 Tag
- `GetTimestampField(point, name)` - 获取时间戳 Field
- `GetUint32Field(point, name)` - 获取 Uint32 Field

#### 类型转换

- `BoolToString(value)` - 布尔转字符串
- `Uint64ToString(value)` - Uint64 转字符串
- `ConvertAnyToPointsSafe(pts)` - 安全转换任意类型为 Points
- `numericToInt64(v)` - 数值转 int64

## 支持的查询语言

| 查询语言 | 说明 | 适用版本 |
|---------|------|----------|
| InfluxQL | InfluxDB 传统查询语言 | 2.x, 3.x |
| SQL | 标准 SQL 语法 | 3.x |
| Flux | 新一代函数式查询语言 | 2.x, 3.x |

## 数据模型概念

### Measurement（测量）

类似于关系数据库中的表，存储时序数据。

```go
// 例如：sensor_data, cpu_usage, memory_stats
measurement := "sensor_data"
```

### Tags（标签）

索引的键值对，用于高效过滤和分组。

```go
tags := map[string]string{
    "sensor_id": "sensor-001",
    "location":  "building-a",
    "floor":     "3",
}
```

### Fields（字段）

实际的数据值，不被索引。

```go
fields := map[string]interface{}{
    "temperature": 25.5,
    "humidity":    60.2,
    "pressure":    1013.25,
}
```

### Timestamp（时间戳）

每条数据的时间点。

```go
timestamp := time.Now()
```

## 最佳实践

### 1. 合理设计 Tags 和 Fields

```go
// ✅ 好的设计：高频查询的字段作为 Tag
tags := map[string]string{
    "sensor_id": "sensor-001",    // 经常用于过滤
    "location":  "building-a",    // 经常用于分组
}
fields := map[string]interface{}{
    "temperature": 25.5,          // 数值数据
    "humidity":    60.2,
}

// ❌ 避免：将高基数数据作为 Tag
tags := map[string]string{
    "request_id": "uuid-12345",  // 每个请求都不同，会导致性能问题
}
```

### 2. 批量写入优化

```go
// ✅ 批量写入比单条写入性能好很多
points := make([]*influxdb3.Point, 0, 1000)
for i := 0; i < 1000; i++ {
    point := influxdb3.NewPoint(...)
    points = append(points, point)
}
err := client.BatchInsert(ctx, points)

// ❌ 避免循环单条插入
for i := 0; i < 1000; i++ {
    err := client.Insert(ctx, point)  // 慢！
}
```

### 3. 使用时间范围过滤

```go
// ✅ 总是添加时间范围过滤
query := `SELECT * FROM sensor_data WHERE time > now() - 1h`

// ❌ 避免全表扫描
query := `SELECT * FROM sensor_data`
```

### 4. 选择合适的保留策略

```go
// 在 InfluxDB 中配置数据保留策略
// 例如：保留最近 30 天的数据
CREATE RETENTION POLICY "30_days" ON "mydb" DURATION 30d REPLICATION 1 DEFAULT
```

### 5. 使用连续查询进行预聚合

```go
// 预先计算每小时平均值
CREATE CONTINUOUS QUERY "hourly_avg" ON "mydb"
BEGIN
  SELECT MEAN(temperature) INTO "hourly_temps" FROM "sensor_data" GROUP BY time(1h), sensor_id
END
```

### 6. 错误处理

```go
result, total, err := repo.ListWithPaging(ctx, req)
if err != nil {
    if errors.Is(err, influxdb.ErrInfluxDBQueryFailed) {
        // 查询失败
        log.Errorf("Query failed: %v", err)
        return nil, 0, err
    }
    // 其他错误
    return nil, 0, err
}
```

### 7. 性能优化

```go
// 使用字段选择减少数据传输
fieldMask := &fieldmaskpb.FieldMask{
    Paths: []string{"sensor_id", "temperature"},
}

// 使用合适的分页大小
pageSize := uint32(100)  // 根据实际需求调整

// 使用索引友好的查询
filters := map[string]interface{}{
    "sensor_id": "sensor-001",  // Tag 字段，有索引
}
```

## 测试

运行测试：

```bash
go test -v ./influxdb/...
```

运行特定测试：

```bash
go test -v ./influxdb -run TestRepository
go test -v ./influxdb -run TestUtils
```

## 示例项目

查看完整示例：
- [repository_test.go](./repository_test.go) - Repository 测试示例
- [client_test.go](./client_test.go) - Client 测试示例
- [utils_test.go](./utils_test.go) - 工具函数测试示例

## 依赖

- `github.com/InfluxCommunity/influxdb3-go/v2` - InfluxDB 3.x Go 客户端
- `github.com/tx7do/go-utils/mapper` - DTO 映射
- `github.com/tx7do/go-crud/api` - Protobuf 定义
- `github.com/go-kratos/kratos/v2` - Kratos 框架（日志）

## 与关系数据库的区别

| 特性 | InfluxDB | 关系数据库 |
|------|----------|-----------|
| 数据类型 | 时序数据 | 通用数据 |
| 写入性能 | 极高 | 中等 |
| 查询性能 | 时间范围查询快 | 复杂 JOIN 快 |
| 数据模型 | Measurement + Tags + Fields | 表 + 列 |
| 索引 | Tags 自动索引 | 需要手动创建索引 |
| 聚合 | 内置时间聚合函数 | 需要手动编写 |
| 保留策略 | 自动过期删除 | 需要手动清理 |
| 适用场景 | IoT、监控、指标 | 业务数据、事务 |

## 常见问题

### Q: 如何选择 Tags 和 Fields？

**A:** 
- **Tags**: 用于过滤、分组的元数据（如 sensor_id、location），会被索引
- **Fields**: 实际的测量值（如 temperature、humidity），不会被索引

### Q: InfluxDB 2.x 和 3.x 有什么区别？

**A:**
- **2.x**: 使用 Flux 查询语言，适合复杂的数据处理
- **3.x**: 支持 SQL 和 InfluxQL，性能更好，更适合大规模部署

### Q: 如何处理大量历史数据？

**A:**
- 使用保留策略（Retention Policy）自动清理旧数据
- 使用连续查询（Continuous Query）预聚合数据
- 考虑降采样（Downsampling）策略

### Q: 如何提高写入性能？

**A:**
- 使用批量写入（BatchInsert）而不是单条插入
- 合理设计 Tags，避免高基数
- 调整批处理大小和频率
- 使用异步写入

## 许可证

本项目采用 MIT 许可证。
