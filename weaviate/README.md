# Weaviate Package

基于官方 `github.com/weaviate/weaviate-go-client/v4` 的 Weaviate 数据访问层，
与 entgo / gorm / mongodb 等模块同级的泛型 DAL 封装。

Weaviate 是向量数据库：数据面为「对象 = UUID + 属性 + 向量」，检索面为
kNN（nearVector）。本模块按 JSON 语义映射实体属性（属性通道）、以 UUID 为
对象身份（ID 通道）、向量字段独立成通道，并在读写路径上强制租户隔离。

> 本模块针对 Weaviate 1.27+（GraphQL Get/Aggregate、批量删除
> DELETE /v1/batch/objects 语义）。

## 特性

- **统一向量检索契约**：`SearchByVector(ctx, *vector.Query)` 接入跨引擎的
  `github.com/tx7do/go-crud/vector` 契约，分数统一为「相似度分，越大越
  相似」（cosine 距离 → 1-d，dot 距离 → -d，l2-squared → 1/(1+d)）；
- **泛型仓库**：`Repository[DTO, ENTITY]` + `mapper.CopierMapper`，DTO 与
  存储实体解耦；
- **多租户强制**（fail-closed）：实体嵌入 `weaviate/mixin.TenantID` 即启用，
  写入强制覆盖 tenant_id 属性，可注入条件的路径服务端注入 tenant_id 相等
  条件（并保证 tenant_id 属性的可过滤索引），按 UUID 直取的路径客户端校验
  属性租户；
- **三通道映射**：属性（JSON 语义，json 标签 + 匿名嵌入拍平）/ UUID
  （id/uuid 命名的 string 字段，不进属性表）/ 向量（[]float32 字段，
  不进属性表）；
- **离线可测**：客户端经 `Config.ConnectionClient` 注入自定义 HTTP
  transport，仓库层协议交互可全链路离线锁定。

## Docker 部署

```bash
# 本地开发（匿名访问、自带向量）
docker run -d --name weaviate-it -p 127.0.0.1:8080:8080 \
  -e AUTHENTICATION_ANONYMOUS_ACCESS_ENABLED=true \
  -e PERSISTENCE_DATA_PATH=/var/lib/weaviate \
  -e DEFAULT_VECTORIZER_MODULE=none \
  -e CLUSTER_HOSTNAME=node1 \
  semitechnologies/weaviate:latest
```

- REST/GraphQL：`localhost:8080`（本模块使用；不启用 gRPC 通道）

## 快速开始

### 1. 安装依赖

```bash
go get github.com/tx7do/go-crud/weaviate
```

### 2. 定义 Entity（含租户 mixin）与 DTO

```go
import (
	"github.com/tx7do/go-crud/weaviate/mixin"
)

// Entity：属性名一律经 json 标签显式小写（weaviate 的 GraphQL 读侧会把
// 大写开头的属性名自动小写化，本模块在建 schema 时强制小写开头以保证
// 写入/查询/返回三处命名一致）；id/uuid 命名的 string 字段承载对象 UUID
//（服务端生成或调用方给定，不进属性表）；[]float32 字段为向量通道；
// 嵌入 mixin.TenantID 即启用租户隔离（int 属性 tenant_id）。
type DocEntity struct {
	UUID  string   `json:"-"`
	Title string   `json:"title"`
	Tags  []string `json:"tags"`
	Emb   []float32 `json:"-"`
	mixin.TenantID
}

type DocDTO struct {
	UUID  string
	Title string
}
```

### 3. 创建 Client 与 Repository、建集合

```go
import (
	weaviate "github.com/tx7do/go-crud/weaviate"
	"github.com/tx7do/go-crud/vector"
)

cli, err := weaviate.NewClient(
	weaviate.WithHost("localhost:8080"),
	// 默认 http；https 场景：weaviate.WithScheme("https")
	// 认证头：weaviate.WithHeaders(map[string]string{"Authorization": "Bearer ..."})
)
if err != nil { panic(err) }
defer cli.Close()

mapperObj := mapper.NewCopierMapper[DocDTO, DocEntity]()
repo := weaviate.NewRepository[DocDTO, DocEntity](cli, "Doc", mapperObj, logger)

// 按实体映射建集合：属性 schema + 度量落 vectorIndexConfig.distance
//（建库期固定）。向量维度无需声明（由数据决定），故无 dims 参数。
// 集合创建依赖实体类型，故为仓库方法而非客户端方法。
if err := repo.CreateCollection(ctx, vector.MetricCosine); err != nil {
	panic(err)
}
```

### 4. 写入 / 读取

```go
// 写入：租户业务视图下 tenant_id 被强制覆盖为当前 Viewer 的租户；
// UUID 缺省由服务端生成并回读到 Entity.UUID。
dto, err := repo.Create(ctx, &DocDTO{Title: "hello"})

// 按 UUID 直取（租户不匹配与不存在同构返回 ErrPointNotFound）。
got, err := repo.GetByUUID(ctx, dto.UUID)

// 按属性条件查询 / 计数（租户视图下自动注入 tenant_id 相等条件）。
rows, err := repo.Query(ctx,
	wvFilters.Where().WithPath([]string{"title"}).
		WithOperator(wvFilters.Equal).WithValueText("hello"), 10)
n, err := repo.Count(ctx, nil)
```

### 5. 向量检索（统一契约）

```go
import "github.com/tx7do/go-crud/vector"

res, err := repo.SearchByVector(ctx, &vector.Query{
	Vector:   []float32{0.1, 0.2, 0.3, 0.4},
	TopK:     10,
	MinScore: 0.5, // 可选：统一分数空间下限（客户端过滤）
})
if err != nil { panic(err) }
for _, hit := range res.Hits {
	fmt.Println(hit.Score, hit.Value.Title)
}
```

> 检索走 GraphQL nearVector（本模块不启用 gRPC 通道）；度量在
> CreateCollection 时固定，查询期传入的 `Metric` 仅用于分数语义换算。
> weaviate 的 certainty 语义（(1+cos)/2）与统一相似度分不同，不透传。

## 租户隔离

实体嵌入 `weaviate/mixin.TenantID`（int 属性，名称固定 tenant_id）即自动
启用，语义与 entgo TenantPrivacy 一致：

| 路径 | 机制 |
| --- | --- |
| Create / BatchCreate | `viewer.EnforceOnScopedInstance` 强制覆盖 tenant_id 属性 |
| Query / Count / Exists / DeleteByUUIDs / DeleteByFilter / SearchByVector | `InjectTenantFilter` 服务端注入 tenant_id 相等条件（与调用方条件 And 合并） |
| GetByUUID | 对象 API 无条件参数，客户端按取回属性校验租户（不匹配与不存在同构，避免存在性泄露） |

缺 ViewerContext → 中止（fail-closed）；平台/系统视图 → 放行。

> Weaviate 原生多租户（collection tenants，租户级硬隔离 + 每租户独立
> 存储/冷热分层）不在本模块适配面内——本模块与其余引擎保持同一编程模型
> （属性级隔离 + fail-closed）；需要租户级硬隔离的场景建议直接使用官方
> 客户端的 tenants API。

## API 参考

### Client

| 方法 | 说明 |
| --- | --- |
| `NewClient(opts...)` | 创建客户端（`WithHost` / `WithScheme` / `WithHeaders` / `WithHTTPClient` 覆写 transport / `WithWeaviateClient` 注入替身）。惰性连接（不做启动探测），离线环境亦可构建 |
| `Close()` | 关闭客户端（本模块不启用 gRPC 通道，幂等返回） |
| `CheckConnect() bool` | 就绪探针（/.well-known/ready） |
| `HasCollection(ctx, name)` / `DropCollection(ctx, name)` | 集合存在性 / 删除 |

### Repository

| 方法 | 说明 |
| --- | --- |
| `CreateCollection(ctx, metric)` | 按实体映射建集合（属性 schema + 度量固定；tenant_id 强制可过滤索引） |
| `Create(ctx, dto)` / `BatchCreate(ctx, dtos)` | 对象写入（UUID 缺省服务端生成并回读；批量行走级错误上抛） |
| `GetByUUID(ctx, uuid)` | 按 UUID 直取（客户端租户校验；404 折叠 ErrPointNotFound） |
| `Get(ctx, id)` / `DeleteByIDs(ctx, ids)` | 不支持：对象身份是 UUID 字符串（恒返回 ErrInvalidRequest） |
| `Query(ctx, filter, limit)` | GraphQL Get 条件查询（租户注入；limit<=0 用服务端默认） |
| `Count(ctx, filter)` / `Exists(ctx, filter)` | GraphQL Aggregate meta.count / 存在性（租户注入） |
| `DeleteByUUIDs(ctx, uuids)` / `DeleteByFilter(ctx, filter)` | 批量删除（id ContainsAny / 显式条件，均与租户条件 And 合并；计数取响应成功数） |
| `SearchByVector(ctx, *vector.Query)` | 统一契约向量检索（nearVector，kNN TopK） |

> 无条件查询（filter 为 nil）必须省略 where 子句——`WithWhere(nil)` 会生成
> 非法的空 where（"Unexpected empty IN ()"），本模块已内置该规避。

## 局限与边界

- **属性名小写开头强制**：weaviate GraphQL 读侧自动小写化大写开头的属性
  名（"Title"→"title"），本模块在建 schema 时拒绝大写开头的属性名（实体
  字段请用小写 json 标签），保证写入/查询/返回三处命名一致；
- **属性类型**：string→text、bool→boolean、有符号整数→int、float→number、
  基本类型切片→对应数组类型；无符号整数实体字段报错（weaviate int 为
  int64，静默截断比报错更糟）；指针字段按元素类型落 schema（mixin 的
  *uint32 → int）；
- **tenant_id 属性漏声明的坑**：weaviate 会按写入 JSON 值自动推断未声明
  属性的类型（浮点），导致后续 valueInt 过滤被拒——本模块建 schema 时
  显式声明（int）并标记可过滤索引；
- **无数值 ID 通道**：对象身份是 UUID 字符串，`Get` / `DeleteByIDs` 恒返回
  `ErrInvalidRequest`；
- **无 ListWithPaging**：调用方经 `Query` 的 filter/limit 组合。

## 测试

```bash
# 离线单测（默认；fake transport 全链路覆盖）
go test ./...

# 集成测试（需本地 Weaviate 1.27+，环境变量门禁）
KRATOS_IT=1 go test ./... -run Integration
```

集成测试覆盖租户隔离全链路：写入强制落租户与 UUID 回读 → 计数/查询按租户
隔离 → 跨租户取回同构未找到 → 跨租户删除被过滤 → 向量检索租户限定与
分数换算 → 平台视图全量清场。

## 依赖

| 模块 | 用途 |
| --- | --- |
| `github.com/weaviate/weaviate-go-client/v4` | 官方客户端（v4.16.1，REST/GraphQL） |
| `github.com/tx7do/go-crud/vector` | 跨引擎向量检索契约 |
| `github.com/tx7do/go-crud/viewer` | 租户强制决策 |
| `github.com/tx7do/go-utils/mapper` | DTO ↔ Entity 映射 |
