# Milvus Package

基于官方 `github.com/milvus-io/milvus-sdk-go/v2`（classic，gRPC）的
Milvus 数据访问层，与 entgo / gorm / mongodb 等模块同级的泛型 DAL 封装。

Milvus 是纯向量数据库：数据面为「行 = 主键 + 标量列 + 向量列」，检索面为
kNN 最近邻。本模块按官方多租户实践将 tenant_id 列标记为 partition key
（按租户值路由分区，隔离与过滤性能均获益），并在读写路径上强制租户隔离。

> 本模块针对 Milvus 2.4+；新版 `client.NewClient`（v2.5+ 分布式客户端）
> 暂不在适配范围。

## 特性

- **统一向量检索契约**：`SearchByVector(ctx, *vector.Query)` 接入跨引擎的
  `github.com/tx7do/go-crud/vector` 契约，分数统一为「相似度分，越大越
  相似」（Milvus 的 L2 距离在客户端换算为 `1/(1+d)`，COSINE/IP 原生
  相似度直接透传）；
- **泛型仓库**：`Repository[DTO, ENTITY]` + `mapper.CopierMapper`，
  DTO 与存储实体解耦；
- **多租户强制**（fail-closed）：实体嵌入 `milvus/mixin.TenantID` 即启用，
  写入强制覆盖 tenant_id，表达式路径服务端注入 `tenant_id == tid` 谓词，
  按主键直取的路径客户端校验租户列；
- **实体 ↔ 列转换**：schema 由实体反射构建（拍平匿名嵌入），写入复用
  官方 `entity.AnyToColumns`；读侧因官方 parseCandidates 跳过匿名嵌入，
  由本模块的对称还原器填充（含 tenant_id 与主键列）。

## Docker 部署

```bash
# Milvus standalone（官方 compose）
wget https://github.com/milvus-io/milvus/releases/download/v2.4.17/milvus-standalone-docker-compose.yml -O docker-compose.yml
docker compose up -d
```

- gRPC：`localhost:19530`（本模块使用）
- Attu 管理界面：`http://localhost:9091`（按 compose 配置）

## 快速开始

### 1. 安装依赖

```bash
go get github.com/tx7do/go-crud/milvus
```

### 2. 定义 Entity（含租户 mixin）与 DTO

```go
import (
	"github.com/tx7do/go-crud/milvus/mixin"
)

// Entity：主键字段（int64 → Int64 主键，string → VarChar 主键）；
// []float32 向量字段（命名 float32 切片类型不被 SDK 列构造器接受）；
// 其余导出字段映射为标量列（无符号整数无对应类型，报错）。
// 嵌入 mixin.TenantID 即启用租户隔离（Int64 列 tenant_id，
// 集合创建时标记为 partition key）。
type DocEntity struct {
	ID    int64
	Title string
	Emb   []float32
	mixin.TenantID
}

type DocDTO struct {
	ID    int64
	Title string
}
```

### 3. 创建 Client 与 Repository、建集合

```go
import (
	milvus "github.com/tx7do/go-crud/milvus"
	"github.com/tx7do/go-crud/vector"
)

cli, err := milvus.NewClient(milvus.WithAddress("localhost:19530"))
if err != nil { panic(err) }
defer cli.Close()

mapperObj := mapper.NewCopierMapper[DocDTO, DocEntity]()
repo := milvus.NewRepository[DocDTO, DocEntity](cli, "docs", mapperObj, logger)

// 按实体映射建集合：schema 构建 → AUTOINDEX 向量索引（度量固定）→ 加载。
// 集合创建依赖实体类型，故为仓库方法而非客户端方法。
if err := repo.CreateCollection(ctx, 4, vector.MetricCosine); err != nil {
	panic(err)
}
```

### 4. 写入 / 读取

```go
// 写入：租户业务视图下 tenant_id 被强制覆盖为当前 Viewer 的租户。
_, err := repo.Create(ctx, &DocDTO{ID: 1, Title: "hello"})

// 按主键取回（租户不匹配与不存在同构返回 ErrPointNotFound）。
dto, err := repo.Get(ctx, 1)

// 按表达式查询 / 计数（租户视图下自动注入 tenant_id 谓词，仅见本租户行）。
rows, err := repo.QueryByExpr(ctx, "title == \"hello\"")
n, err := repo.Count(ctx, "")
```

### 5. 向量检索（统一契约）

```go
import "github.com/tx7do/go-crud/vector"

res, err := repo.SearchByVector(ctx, &vector.Query{
	Field: "Emb",                  // 向量字段名（单一向量字段时可省略）
	Vector: []float32{0.1, 0.2, 0.3, 0.4}, // 与建集合维度一致
	TopK:   10,
	MinScore: 0.5, // 可选：统一分数空间下限（客户端过滤）
})
if err != nil { panic(err) }
for _, hit := range res.Hits {
	fmt.Println(hit.Score, hit.Value.Title)
}
```

> 检索参数固定 AUTOINDEX（本模块集合一律以 AUTOINDEX 建索引），
> `NumCandidates` 不透传（Milvus 无对应参数）。度量在索引创建时固定，
> 查询期传入的 `Metric` 仅用于分数语义换算，不改写索引。

## 租户隔离

实体嵌入 `milvus/mixin.TenantID`（Int64 列，名称固定 tenant_id，
partition key）即自动启用，语义与 entgo TenantPrivacy 一致：

| 路径 | 机制 |
| --- | --- |
| Create / BatchCreate | `viewer.EnforceOnScopedInstance` 强制覆盖 tenant_id |
| QueryByExpr / DeleteByExpr / Count / Exists / SearchByVector | `InjectTenantFilterIntoExpr` 服务端注入 `tenant_id == tid` 谓词（数值化拼接，无注入面；与调用方表达式括号包裹后 AND 合并） |
| Get / GetByUUID / DeleteByIDs / DeleteByUUIDs | QueryByPks 协议无过滤表达式，客户端按取回行的 tenant_id 列校验/过滤（不匹配与不存在同构，避免存在性泄露） |

缺 ViewerContext → 中止（fail-closed）；平台/系统视图 → 放行。

## API 参考

### Client

| 方法 | 说明 |
| --- | --- |
| `NewClient(opts...)` | 创建客户端（`WithAddress` / `WithUsername` / `WithPassword` / `WithAPIKey` / `WithDBName`）。覆写官方默认拨号选项：去掉 `grpc.WithBlock()`（离线永久阻塞）改为惰性连接，保留 2GB 接收上限 |
| `Close()` | 关闭连接 |
| `CheckConnect() bool` | 连接探针 |
| `HasCollection(ctx, name)` / `DropCollection(ctx, name)` | 集合存在性 / 删除 |

### Repository

| 方法 | 说明 |
| --- | --- |
| `CreateCollection(ctx, dims, metric)` | 按实体映射建集合（schema + AUTOINDEX + 加载） |
| `HasCollection(ctx)` / `DropCollection(ctx)` | 本仓库集合的存在性 / 删除 |
| `Create(ctx, dto)` / `BatchCreate(ctx, dtos)` | 列 Upsert（主键由调用方给定，非自增） |
| `Get(ctx, id)` / `GetByUUID(ctx, uuid)` | 按主键取回（客户端租户校验；UUID 含引号/反斜杠拒绝——官方 PKs2Expr 无转义） |
| `QueryByExpr(ctx, expr)` | 按表达式查询（租户注入；剔除租户不匹配行） |
| `DeleteByIDs(ctx, ids)` / `DeleteByUUIDs(ctx, uuids)` | 按主键删除（租户先过滤后删） |
| `DeleteByExpr(ctx, expr)` | 按表达式删除（表达式经注入后须非空，拒绝全表裸删） |
| `Count(ctx, expr)` / `Exists(ctx, expr)` | 计数（优先服务端 count(*) 聚合，不支持时回退主键列计数）/ 存在性（租户注入） |
| `SearchByVector(ctx, *vector.Query)` | 统一契约向量检索 |

> 不提供 ListWithPaging：Milvus 无排序/游标分页能力；向量检索本身即
> TopK 语义。

## 局限与边界

- 向量字段仅接受裸 `[]float32`；`vector.Float32Vector`（pgvector 文本
  方言的命名类型）会被官方列构造器拒绝，建集合时报错；
- 无符号整数实体字段报错（Milvus 无对应类型，静默截断比报错更糟）；
- VarChar 列 max_length 固定 65535（引擎上限）；
- 向量数据不回读：Query/Search 只还原标量列与主键，向量字段保持零值；
- 集合创建依赖实体类型（泛型），因此是仓库方法而非客户端方法；
- 多向量字段集合：检索时 `vector.Query.Field` 必填。

## 测试

```bash
# 离线单测（默认）
go test ./...

# 集成测试（需本地 Milvus，环境变量门禁）
KRATOS_IT=1 go test ./... -run Integration
```

集成测试覆盖租户隔离全链路：写入强制落租户 → 计数/取回/表达式查询/检索
按租户隔离 → 跨租户删除被过滤。

## 依赖

| 模块 | 用途 |
| --- | --- |
| `github.com/milvus-io/milvus-sdk-go/v2` | 官方客户端（v2.4.2，classic） |
| `github.com/tx7do/go-crud/vector` | 跨引擎向量检索契约 |
| `github.com/tx7do/go-crud/viewer` | 租户强制决策 |
| `github.com/tx7do/go-utils/mapper` | DTO ↔ Entity 映射 |
