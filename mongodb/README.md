# MongoDB Package

基于 [MongoDB](https://www.mongodb.com/) 的文档数据库访问层封装，提供完整的数据写入、查询、分页、过滤和排序功能。

## 特性

- ✅ **泛型支持** - 完全基于 Go 泛型，类型安全
- ✅ **灵活文档模型** - Schema-less，适合半结构化数据
- ✅ **完整 CRUD** - 创建、查询、更新、删除操作
- ✅ **多种分页** - 支持 Offset、Page、Token 三种分页方式
- ✅ **结构化过滤** - 支持复杂的过滤表达式
- ✅ **灵活排序** - 支持多字段排序
- ✅ **字段选择** - 支持 FieldMask 选择返回字段
- ✅ **聚合查询** - 支持 MongoDB Aggregation Pipeline
- ✅ **索引优化** - 自动利用 MongoDB 索引
- ✅ **DTO/Document 映射** - 自动 DTO 和 Document 转换
- ✅ **go-wind 日志集成** - 内置日志支持

## 概念对比

| MongoDB存储结构 | RDBMS存储结构   |
|-------------|-------------|
| database    | database    |
| collection  | table       |
| document    | row         |
| field       | column      |
| index       | 索引          |
| primary key | primary key |

## Docker 部署

### 下载镜像

```bash
docker pull bitnami/mongodb:latest
docker pull bitnami/mongodb-exporter:latest
```

### 带密码安装

```bash
docker run -itd \
    --name mongodb-server \
    -p 27017:27017 \
    -e MONGODB_ROOT_USER=root \
    -e MONGODB_ROOT_PASSWORD=123456 \
    -e MONGODB_USERNAME=test \
    -e MONGODB_PASSWORD=123456 \
    -e MONGODB_DATABASE=finances \
    bitnami/mongodb:latest
```

### 不带密码安装

```bash
docker run -itd \
    --name mongodb-server \
    -p 27017:27017 \
    -e ALLOW_EMPTY_PASSWORD=yes \
    bitnami/mongodb:latest
```

### 注意事项

有两点需要注意：

1. **权限问题**：如果需要映射数据卷，需要把本地路径的所有权改到1001：
   ```bash
   sudo chown -R 1001:1001 data/db
   ```
   否则会报错：`mkdir: cannot create directory '/bitnami/mongodb': Permission denied`

2. **硬件兼容性**：从MongoDB 5.0开始，有些机器运行会报错：`Illegal instruction`，这是因为机器硬件不支持 **AVX 指令集** 的缘故，没办法，MongoDB降级吧。
   ```bash
   docker pull bitnami/mongodb:4.4
   ```

### Docker Compose 部署

```yaml
version: '3.8'

services:
  mongodb:
    image: bitnami/mongodb:latest
    container_name: mongodb-server
    ports:
      - "27017:27017"
    environment:
      - MONGODB_ROOT_USER=root
      - MONGODB_ROOT_PASSWORD=123456
      - MONGODB_USERNAME=test
      - MONGODB_PASSWORD=123456
      - MONGODB_DATABASE=mydb
    volumes:
      - mongodb_data:/bitnami/mongodb
    networks:
      - app-tier

volumes:
  mongodb_data:

networks:
  app-tier:
    driver: bridge
```

## 快速开始

### 1. 安装依赖

```bash
go get github.com/tx7do/go-crud/mongodb
```

### 2. 定义 Document 和 DTO

**Document (MongoDB 文档):**

```go
type UserDocument struct {
    ID        primitive.ObjectID `bson:"_id,omitempty" json:"id"`
    UserID    uint64             `bson:"user_id" json:"user_id"`
    Name      string             `bson:"name" json:"name"`
    Email     string             `bson:"email" json:"email"`
    Age       int                `bson:"age" json:"age"`
    CreatedAt time.Time          `bson:"created_at" json:"created_at"`
}
```

**注意：** MongoDB 使用 `bson` 标签映射字段名。

**DTO (Protobuf 消息):**

```protobuf
syntax = "proto3";

package users;

option go_package = "github.com/example/users;users";

import "google/protobuf/timestamp.proto";

message User {
  string id = 1;
  uint64 user_id = 2;
  string name = 3;
  string email = 4;
  int32 age = 5;
  google.protobuf.Timestamp created_at = 6;
}
```

### 3. 创建 Client

```go
import (
    "github.com/tx7do/go-crud/mongodb"
)

// 方式 1：使用 URI
client, err := mongodb.NewClient(
    mongodb.WithURI("mongodb://localhost:27017"),
    mongodb.WithDatabase("mydb"),
    mongodb.WithTimeout(10*time.Second),
)

// 方式 2：带认证
client, err := mongodb.NewClient(
    mongodb.WithURI("mongodb://test:123456@localhost:27017/mydb"),
    mongodb.WithDatabase("mydb"),
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
    "github.com/tx7do/go-crud/mongodb"
)

// 创建 Mapper
m := mapper.NewCopierMapper[User, UserDocument]()

// 创建 Repository
repo := mongodb.NewRepository[User, UserDocument](
    client,
    "users",  // collection 名称
    m,
    logger,
)
```

### 5. 写入数据

#### 插入单条文档

```go
ctx := context.Background()

user := &User{
    UserId:    1001,
    Name:      "John Doe",
    Email:     "john@example.com",
    Age:       30,
    CreatedAt: timestamppb.Now(),
}

err := repo.Create(ctx, user)
if err != nil {
    log.Fatal(err)
}
```

#### 批量插入

```go
users := []*User{
    {UserId: 1001, Name: "Alice", Email: "alice@example.com"},
    {UserId: 1002, Name: "Bob", Email: "bob@example.com"},
    {UserId: 1003, Name: "Charlie", Email: "charlie@example.com"},
}

err := repo.BatchCreate(ctx, users)
if err != nil {
    log.Fatal(err)
}
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

results, total, err := repo.ListWithPaging(ctx, req)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Total: %d, Items: %d\n", total, len(results))
for _, user := range results {
    fmt.Printf("User: %+v\n", user)
}
```

#### 带过滤的查询

```go
// 查询年龄大于 18 的用户
filterExpr := &paginationV1.FilterExpr{
    Conditions: []*paginationV1.FilterCondition{
        {
            Field: "age",
            Op:    paginationV1.Operator_GT,
            Value: &paginationV1.FilterCondition_ValueOneof{Value: "18"},
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

results, total, err := repo.ListWithPaging(ctx, req)
```

#### 带排序的查询

```go
// 按创建时间降序排列
sorting := []*paginationV1.Sorting{
    {Field: "created_at", Direction: paginationV1.Sorting_DESC},
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

// 只返回 name 和 email
fieldMask := &fieldmaskpb.FieldMask{
    Paths: []string{"name", "email"},
}

req := &paginationV1.PagingRequest{
    Page:      &page,
    PageSize:  &pageSize,
    FieldMask: fieldMask,
}

results, total, err := repo.ListWithPaging(ctx, req)
```

---

### 7. 更新和删除

#### 更新文档

```go
// 更新用户年龄
user.Age = 31
err := repo.Update(ctx, user)
if err != nil {
    log.Fatal(err)
}
```

#### 删除文档

```go
// 根据 ID 删除
err := repo.Delete(ctx, bson.M{"user_id": 1001})
if err != nil {
    log.Fatal(err)
}
```

---

### 8. 直接使用 Client

#### 查询单个文档

```go
var user UserDocument
err := client.FindOne(ctx, "users", bson.M{"user_id": 1001}, &user)
if err != nil {
    log.Fatal(err)
}
```

#### 查询多个文档

```go
var users []UserDocument
err := client.Find(ctx, "users", bson.M{"age": bson.M{"$gt": 18}}, &users)
if err != nil {
    log.Fatal(err)
}
```

#### 计数

```go
count, err := client.Count(ctx, "users", bson.M{"age": bson.M{"$gt": 18}})
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Count: %d\n", count)
```

#### 存在性检查

```go
exists, err := client.Exist(ctx, "users", bson.M{"email": "john@example.com"})
if err != nil {
    log.Fatal(err)
}
if exists {
    fmt.Println("User exists")
}
```

---

## API 参考

### Client 方法

#### 连接管理

- `NewClient(opts...)` - 创建客户端
- `Close()` - 关闭连接
- `CheckConnect()` - 检查连接状态

#### 数据写入

- `InsertOne(ctx, collection, document)` - 插入单个文档
- `InsertMany(ctx, collection, documents)` - 插入多个文档

#### 数据查询

- `FindOne(ctx, collection, filter, result)` - 查询单个文档
- `Find(ctx, collection, filter, results)` - 查询多个文档
- `Count(ctx, collection, filter)` - 统计文档数量
- `Exist(ctx, collection, filter)` - 检查文档是否存在

#### 数据更新

- `UpdateOne(ctx, collection, filter, update)` - 更新单个文档
- `UpdateMany(ctx, collection, filter, update)` - 更新多个文档
- `FindOneAndUpdate(ctx, collection, filter, update, result, opts...)` - 查找并更新，返回更新后的文档

#### 数据删除

- `DeleteOne(ctx, collection, filter)` - 删除单个文档
- `DeleteMany(ctx, collection, filter)` - 删除多个文档

### Repository 方法

#### 查询方法

- `ListWithPaging(ctx, req)` - 分页列表查询（PagingRequest）
- `ListWithPagination(ctx, req)` - 分页列表查询（PaginationRequest）
- `Count(ctx, qb)` - 计数

#### 创建方法

- `Create(ctx, dto)` - 插入单个文档
- `BatchCreate(ctx, dtos)` - 批量插入

#### 更新方法

- `Update(ctx, dto)` - 更新单个文档

#### 删除方法

- `Delete(ctx, filter)` - 删除文档

### Client 配置选项

- `WithURI(uri)` - 设置 MongoDB URI（必填）
- `WithDatabase(db)` - 设置数据库名称
- `WithTimeout(d)` - 设置默认超时时间（默认 10 秒）
- `WithOptions(opts)` - 添加额外的 ClientOptions
- `WithLogger(logger)` - 设置日志器

---

## MongoDB 特性

### Schema-less 文档模型

MongoDB 是 NoSQL 数据库，使用 BSON（Binary JSON）格式存储数据，不需要预先定义表结构。

**优势：**
- ✅ 灵活的数据模型，适合半结构化数据
- ✅ 支持嵌套对象和数组
- ✅ 易于扩展字段
- ❌ 缺少强类型约束，需要应用层验证

### 索引优化

```javascript
// 创建单字段索引
db.users.createIndex({ "email": 1 })

// 创建复合索引
db.users.createIndex({ "age": 1, "created_at": -1 })

// 创建唯一索引
db.users.createIndex({ "user_id": 1 }, { unique: true })

// 创建 TTL 索引（自动过期）
db.sessions.createIndex({ "last_accessed": 1 }, { expireAfterSeconds: 3600 })
```

### 聚合查询

```go
pipeline := mongo.Pipeline{
    {{"$match", bson.M{"age": bson.M{"$gt": 18}}}},
    {{"$group", bson.M{
        "_id": "$city",
        "count": bson.M{"$sum": 1},
        "avg_age": bson.M{"$avg": "$age"},
    }}},
    {{"$sort", bson.M{"count": -1}}},
}

cursor, err := client.Database("mydb").Collection("users").Aggregate(ctx, pipeline)
```

---

## 最佳实践

### 1. 使用索引优化查询性能

```go
// ✅ 好的做法：在常用查询字段上创建索引
// MongoDB Shell
db.users.createIndex({ "email": 1 })
db.users.createIndex({ "user_id": 1 }, { unique: true })

// 查询时会自动利用索引
var user UserDocument
err := client.FindOne(ctx, "users", bson.M{"email": "john@example.com"}, &user)

// ❌ 避免：没有索引的全表扫描
// 如果 email 字段没有索引，每次查询都会扫描整个集合
```

**性能对比：**
- 有索引：~1ms（百万级数据）
- 无索引：~500ms（百万级数据）

---

### 2. 合理设计文档结构

```go
// ✅ 好的做法：嵌入相关数据（一对一或一对少）
type OrderDocument struct {
    ID         primitive.ObjectID `bson:"_id,omitempty"`
    OrderID    uint64             `bson:"order_id"`
    UserID     uint64             `bson:"user_id"`
    Items      []OrderItem        `bson:"items"`       // 嵌入订单项
    TotalPrice float64            `bson:"total_price"`
}

type OrderItem struct {
    ProductID uint64  `bson:"product_id"`
    Name      string  `bson:"name"`
    Price     float64 `bson:"price"`
    Quantity  int     `bson:"quantity"`
}

// ❌ 避免：过度嵌入（一对多且数量大）
type UserDocument struct {
    ID     primitive.ObjectID `bson:"_id,omitempty"`
    UserID uint64             `bson:"user_id"`
    Orders []OrderDocument    `bson:"orders"`  // 订单可能很多，不应嵌入
}
```

**原则：**
- 一对一或一对少（< 100）：使用嵌入
- 一对多且数量大：使用引用（单独集合）

---

### 3. 批量插入优化

```go
// ✅ 好的做法：批量插入
documents := make([]interface{}, 0, 1000)
for _, user := range users {
    documents = append(documents, user)
}
result, err := client.InsertMany(ctx, "users", documents)

// ❌ 避免：循环单条插入
for _, user := range users {
    client.InsertOne(ctx, "users", user)  // 慢！
}
```

**性能对比：**
- 单条插入：~1,000 条/秒
- 批量插入：~10,000 条/秒

---

### 4. 使用投影减少数据传输

```go
// ✅ 好的做法：只返回需要的字段
opts := optionsV2.Find().SetProjection(bson.M{
    "name":  1,
    "email": 1,
})

cursor, err := collection.Find(ctx, filter, opts)

// ❌ 避免：返回所有字段
// 如果文档很大，会浪费带宽和内存
```

---

### 5. 设置合理的超时时间

```go
// ✅ 好的做法：根据操作类型设置超时
client, err := mongodb.NewClient(
    mongodb.WithURI("mongodb://localhost:27017"),
    mongodb.WithDatabase("mydb"),
    mongodb.WithTimeout(5*time.Second),  // 默认 5 秒
)

// 对于复杂查询，可以增加超时
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()

// ❌ 避免：不设置超时，可能导致请求挂起
```

---

### 6. 处理 ObjectId

```go
import "go.mongodb.org/mongo-driver/v2/bson/primitive"

// ✅ 好的做法：使用 ObjectID 作为主键
type UserDocument struct {
    ID     primitive.ObjectID `bson:"_id,omitempty"`
    UserID uint64             `bson:"user_id"`
}

// 创建新文档时自动生成 ID
user := &UserDocument{
    ID:     primitive.NewObjectID(),  // 自动生成
    UserID: 1001,
}

// 根据 ID 查询
objID, _ := primitive.ObjectIDFromHex("507f191e810c19729de860ea")
var user UserDocument
err := client.FindOne(ctx, "users", bson.M{"_id": objID}, &user)
```

---

### 7. 使用 Upsert 实现“存在则更新，不存在则插入”

```go
// ✅ 好的做法：使用 UpdateOne + Upsert
filter := bson.M{"user_id": 1001}
update := bson.M{
    "$set": bson.M{
        "name":  "John Doe",
        "email": "john@example.com",
    },
}

opts := optionsV2.Update().SetUpsert(true)
result, err := client.UpdateOne(ctx, "users", filter, update, opts)

if result.UpsertedCount > 0 {
    fmt.Println("Inserted new document")
} else if result.ModifiedCount > 0 {
    fmt.Println("Updated existing document")
}
```

---

### 8. 监控慢查询

```javascript
// 启用 Profiling
db.setProfilingLevel(1, { slowOpThresholdMs: 100 })

// 查看慢查询
db.system.profile.find().sort({ ts: -1 }).limit(10)
```

---

## 测试

运行测试：

```bash
go test -v ./mongodb/...
```

运行特定测试：

```bash
go test -v ./mongodb -run TestRepository
go test -v ./mongodb -run TestClient
```

---

## 示例项目

查看完整示例：
- [repository_test.go](./repository_test.go) - Repository 测试示例
- [client_test.go](./client_test.go) - Client 测试示例

---

## 依赖

- `go.mongodb.org/mongo-driver/v2` - MongoDB Go 驱动（v2）
- `github.com/tx7do/go-utils/mapper` - DTO/Document 映射
- `github.com/tx7do/go-crud/api` - Protobuf 定义
- `github.com/tx7do/go-wind` - go-wind 框架（日志）

---

## 与其他包的对比

| 特性 | MongoDB | MySQL/PostgreSQL | ClickHouse/Doris |
|------|---------|------------------|------------------|
| 数据模型 | 文档（BSON） | 关系表（行式） | 列式存储 |
| Schema | Schema-less | 严格 Schema | 严格 Schema |
| 写入性能 | 高 | 中等 | 极高 |
| 查询性能 | OLTP 快 | OLTP 快 | OLAP 极快 |
| 聚合查询 | 灵活 | 标准 SQL | 超快 |
| 事务支持 | 单文档 ACID | 完整 ACID | 有限 |
| 嵌套对象 | 原生支持 | 需要 JOIN | 不支持 |
| 适用场景 | 半结构化数据 | 业务数据 | 分析数据 |

---

## 常见问题 FAQ

### Q: MongoDB 适合什么场景？

**A:** 
- ✅ 半结构化数据（如 JSON、日志、用户行为）
- ✅ 快速迭代的开发场景（Schema 灵活）
- ✅ 需要嵌套对象和数组的数据
- ✅ 内容管理系统、社交网络、实时分析
- ❌ 不适合复杂的事务处理
- ❌ 不适合需要强一致性的金融系统

### Q: 什么时候使用嵌入，什么时候使用引用？

**A:** 
- **嵌入**：一对一或一对少（< 100），经常一起查询
- **引用**：一对多且数量大，或者需要独立更新

```go
// 嵌入：订单和订单项（一对少）
type Order struct {
    Items []OrderItem `bson:"items"`
}

// 引用：用户和订单（一对多）
type User struct {
    UserID uint64 `bson:"user_id"`
}
// 订单单独存储在 orders 集合中，通过 user_id 引用
```

### Q: 如何优化慢查询？

**A:**
- 创建合适的索引（单字段、复合、唯一索引）
- 使用投影只返回需要的字段
- 避免全表扫描（确保查询条件有索引）
- 使用 Profiling 监控慢查询
- 合理设计文档结构（避免过度嵌入）

### Q: ObjectId 和自定义 ID 有什么区别？

**A:** 
- **ObjectId**：MongoDB 自动生成，包含时间戳，全局唯一
- **自定义 ID**：应用层控制，便于与外部系统集成

```go
// 使用 ObjectId（推荐）
type User struct {
    ID   primitive.ObjectID `bson:"_id,omitempty"`
    Name string             `bson:"name"`
}

// 使用自定义 ID
type User struct {
    UserID uint64 `bson:"user_id"`
    Name   string `bson:"name"`
}
```

### Q: 如何处理并发更新冲突？

**A:** 使用乐观锁或版本号：

```go
type Document struct {
    ID      primitive.ObjectID `bson:"_id,omitempty"`
    Version int                `bson:"version"`
    Data    string             `bson:"data"`
}

// 更新时检查版本号
filter := bson.M{"_id": docID, "version": currentVersion}
update := bson.M{
    "$set": bson.M{"data": newData},
    "$inc": bson.M{"version": 1},
}

result, err := client.UpdateOne(ctx, "collection", filter, update)
if result.MatchedCount == 0 {
    // 版本冲突，需要重试
}
```

### Q: 如何实现数据过期自动删除？

**A:** 使用 TTL 索引：

```javascript
// 创建 TTL 索引，3600 秒后自动删除
db.sessions.createIndex(
    { "last_accessed": 1 },
    { expireAfterSeconds: 3600 }
)
```

### Q: 如何备份和恢复数据？

**A:** 
```bash
# 备份
mongodump --uri="mongodb://localhost:27017/mydb" --out=/backup

# 恢复
mongorestore --uri="mongodb://localhost:27017/mydb" /backup/mydb
```

---

## 参考链接

- [MongoDB 官方网站](https://www.mongodb.com/)
- [MongoDB Go Driver](https://www.mongodb.com/docs/drivers/go/)
- [MongoDB 文档](https://www.mongodb.com/docs/)
- [MongoDB 索引指南](https://www.mongodb.com/docs/manual/indexes/)
- [MongoDB 聚合框架](https://www.mongodb.com/docs/manual/aggregation/)

---

## 许可证

本项目采用 MIT 许可证。
