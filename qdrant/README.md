# Qdrant Package

基于官方 `github.com/qdrant/go-client`（gRPC）的 Qdrant 数据访问层，
与 entgo / gorm / mongodb 等模块同级的泛型 DAL 封装。

Qdrant 是纯向量数据库：数据面为「点（Point）= ID + 向量 + 载荷（Payload）」，
检索面为 kNN 最近邻。本模块按官方多租户实践对 tenant_id 载荷字段建立
整数索引，并在读写路径上强制租户隔离。

## 特性

- **统一向量检索契约**：`SearchByVector(ctx, *vector.Query)` 接入跨引擎的
  `github.com/tx7do/go-crud/vector` 契约（Field/Vector/TopK/MinScore/Filter），
  分数统一为「相似度分，越大越相似」（Qdrant 的 Euclid 距离在客户端换算为
  `1/(1+d)`，Cosine/Dot 原生相似度直接透传）；
- **泛型仓库**：`Repository[DTO, ENTITY]` + `mapper.CopierMapper`，
  DTO 与存储实体解耦；
- **多租户强制**（fail-closed）：实体嵌入 `qdrant/mixin.TenantID` 即启用，
  写入强制覆盖 tenant_id，可注入 Filter 的路径服务端注入 `tenant_id` 匹配
  条件，按 ID 直取的路径客户端校验租户；
- **实体 ↔ 载荷转换**：反射编解码（json 标签命名、匿名嵌入拍平、整数
  类型不经 JSON 浮点化），向量字段走独立 Vectors 通道。

## Docker 部署

```bash
docker pull qdrant/qdrant
docker run -p 6333:6333 -p 6334:6334 -v $(pwd)/qdrant_storage:/qdrant/storage qdrant/qdrant
```

- HTTP API：`http://localhost:6333`（Dashboard：`http://localhost:6333/dashboard`）
- gRPC：`localhost:6334`（本模块使用）

## 快速开始

### 1. 安装依赖

```bash
go get github.com/tx7do/go-crud/qdrant
```

### 2. 定义 Entity（含租户 mixin）与 DTO

```go
import (
	"github.com/tx7do/go-crud/qdrant/mixin"
)

// Entity：ID/UUID 字段（数值或字符串）成为点 ID；[]float32 /
// vector.Float32Vector 字段成为向量；其余导出字段进入载荷。
// 嵌入 mixin.TenantID 即启用租户隔离（载荷字段 tenant_id）。
type DocEntity struct {
	ID      uint64
	Title   string
	Emb     []float32
	mixin.TenantID
}

type DocDTO struct {
	ID    uint64
	Title string
}
```

### 3. 创建 Client 与 Collection

```go
import (
	qdrant "github.com/tx7do/go-crud/qdrant"
	"github.com/tx7do/go-crud/vector"
)

cli, err := qdrant.NewClient(
	qdrant.WithHost("localhost"),
	qdrant.WithPort(6334),
)
if err != nil { panic(err) }
defer cli.Close()

// 创建向量集合（维度 4，余弦度量——度量在集合创建时固定）。
if err := cli.CreateVectorCollection(ctx, "docs", 4, vector.MetricCosine); err != nil {
	panic(err)
}
// 多租户实践：对 tenant_id 载荷建立整数索引（过滤检索走索引而非全扫）。
if err := cli.CreatePayloadIndex(ctx, "docs", "tenant_id", qdrant.PayloadIndexInteger); err != nil {
	panic(err)
}
```

### 4. 创建 Repository

```go
mapperObj := mapper.NewCopierMapper[DocDTO, DocEntity]()   // github.com/tx7do/go-utils/mapper
repo := qdrant.NewRepository[DocDTO, DocEntity](cli, "docs", mapperObj, logger)
```

### 5. 写入 / 读取

```go
// 写入：租户业务视图下 tenant_id 被强制覆盖为当前 Viewer 的租户。
_, err := repo.Create(ctx, &DocDTO{ID: 1, Title: "hello"})

// 按点 ID 取回（租户不匹配与不存在同构返回 ErrPointNotFound）。
dto, err := repo.Get(ctx, 1)

// 计数（租户视图下仅计本租户的点）。
n, err := repo.Count(ctx, nil)
```

### 6. 向量检索（统一契约）

```go
import "github.com/tx7do/go-crud/vector"

res, err := repo.SearchByVector(ctx, &vector.Query{
	Vector: []float32{0.1, 0.2, 0.3, 0.4}, // 与集合维度一致
	TopK:   10,
	MinScore: 0.5, // 可选：统一分数空间下限（客户端过滤）
})
if err != nil { panic(err) }
for _, hit := range res.Hits {
	fmt.Println(hit.Score, hit.Value.Title)
}
```

> `vector.Query.Field` 在本模块忽略：本模块创建的集合只含单一匿名向量，
> `QueryPoints.Using` 保持未设置。`NumCandidates` 映射到
> `SearchParams.HnswEf`（近期版本服务端多数配置下会忽略该值自行调优）。

## 租户隔离

实体嵌入 `qdrant/mixin.TenantID`（载荷字段 `tenant_id`，json 标签命名）
即自动启用，语义与 entgo TenantPrivacy 一致：

| 路径 | 机制 |
| --- | --- |
| Create / BatchCreate | `viewer.EnforceOnScopedInstance` 强制覆盖 tenant_id |
| DeleteByFilter / Count / Exists / SearchByVector | `InjectTenantFilterIntoQdrantFilter` 服务端注入 `tenant_id` 匹配条件（原有 Filter 经 FilterAsCondition 包装后 AND 合并，语义保留） |
| Get / GetByUUID / DeleteByIDs / DeleteByUUIDs | GetPoints 协议无 Filter 字段，客户端按取回载荷校验/过滤租户（不匹配与不存在同构，避免存在性泄露） |

缺 ViewerContext → 中止（fail-closed）；平台/系统视图 → 放行。

## API 参考

### Client

| 方法 | 说明 |
| --- | --- |
| `NewClient(opts...)` | 创建客户端（`WithHost` / `WithPort` / `WithAPIKey` / `WithTLS` / `WithTLSConfig` / `WithQdrantClient`） |
| `Close()` | 关闭连接 |
| `CheckConnect() bool` | 连接探针 |
| `HasCollection(ctx, name)` | 集合存在性 |
| `CreateVectorCollection(ctx, name, dims, metric)` | 创建向量集合（度量固定） |
| `DropCollection(ctx, name)` / `DropVectorCollection` | 删除集合 |
| `CreatePayloadIndex(ctx, coll, field, kind)` | 载荷索引（Keyword / Integer） |

### Repository

| 方法 | 说明 |
| --- | --- |
| `Create(ctx, dto)` / `BatchCreate(ctx, dtos)` | 点 Upsert（ID + 向量 + 载荷三通道） |
| `Get(ctx, id)` / `GetByUUID(ctx, uuid)` | 按点 ID 取回（客户端租户校验） |
| `DeleteByIDs(ctx, ids)` / `DeleteByUUIDs(ctx, uuids)` | 按点 ID 删除（租户先过滤后删） |
| `DeleteByFilter(ctx, filter)` | 按 Qdrant Filter 删除（必须显式给出 Filter；租户注入） |
| `Count(ctx, filter)` / `Exists(ctx, filter)` | 计数 / 存在性（租户注入） |
| `SearchByVector(ctx, *vector.Query)` | 统一契约向量检索 |

> 不提供 ListWithPaging：Qdrant 的 Scroll 按 ID 游标推进，与
> offset/page/token 分页契约不兼容；向量检索本身即 TopK 语义。

## 局限与边界

- 命名向量（CreateVectorName）与多向量检索（Using）不在支持范围——
  本模块的集合只含单一匿名向量；
- 分数不透传 `ScoreThreshold`（其原生方向随度量翻转），`MinScore` 在
  统一分数空间客户端过滤；
- 向量数据不回读：Get/Search 只还原载荷，向量字段保持零值；
- 实体缺 id/uuid 字段或缺向量字段时，写入报错
  （`ErrInvalidPointID` / `ErrInvalidRequest`）。

## 测试

```bash
# 离线单测（默认）
go test ./...

# 集成测试（需本地 Qdrant，环境变量门禁）
KRATOS_IT=1 go test ./... -run Integration
```

集成测试覆盖租户隔离全链路：写入强制落租户 → 计数/取回/检索按租户隔离 →
跨租户删除被过滤。

## 依赖

| 模块 | 用途 |
| --- | --- |
| `github.com/qdrant/go-client` | 官方 gRPC 客户端（v1.19.3） |
| `github.com/tx7do/go-crud/vector` | 跨引擎向量检索契约 |
| `github.com/tx7do/go-crud/viewer` | 租户强制决策 |
| `github.com/tx7do/go-utils/mapper` | DTO ↔ Entity 映射 |
