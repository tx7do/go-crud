# Neo4j Package

基于官方 `github.com/neo4j/neo4j-go-driver/v5` 的 Neo4j 数据访问层，与
entgo / gorm / mongodb 等模块同级的泛型 DAL 封装。

本模块把 label 当作行式存储的表使用：**节点 = 行，属性 = 列，element id =
服务端生成的行身份（字符串）**。仓库面为节点 CRUD 与属性谓词过滤；图遍历、
关系模式匹配等图查询不在仓库面内（见「局限与边界」）。

> 本模块针对 Neo4j 5.x（element id 语义自 5.x 引入）。

## 特性

- **泛型仓库**：`Repository[DTO, ENTITY]` + `mapper.CopierMapper`，DTO 与
  存储实体解耦；
- **参数化 Cypher**：所有语句的值一律经 `$参数` 传递（属性表整体走
  `$props` / `$rows`），标签经反引号包裹并拒绝含反引号的标签名——无文本
  拼接注入面；
- **多租户强制**（fail-closed）：实体嵌入 `neo4j/mixin.TenantID` 即启用，
  写入强制覆盖 tenant_id 属性，读取/删除路径在 WHERE 中服务端注入
  `tenant_id = $__tid` 谓词，按 element id 直取的路径叠加客户端属性校验；
- **element id 回读**：Create/BatchCreate 以 `RETURN elementId(n)` 回读
  服务端生成的节点身份到实体的 uuid 通道字段。

## Docker 部署

```bash
# 本地开发（无认证，仅测试用途）
docker run -d --name neo4j-it -p 127.0.0.1:7474:7474 -p 127.0.0.1:7687:7687 -e NEO4J_AUTH=none neo4j:5
```

- Bolt：`localhost:7687`（本模块使用）
- HTTP 管理界面：`http://localhost:7474`

## 快速开始

### 1. 安装依赖

```bash
go get github.com/tx7do/go-crud/neo4j
```

### 2. 定义 Entity（含租户 mixin）与 DTO

```go
import (
	"github.com/tx7do/go-crud/neo4j/mixin"
)

// Entity：uuid / element_id（大小写不敏感）命名的 string 字段承载节点
// 身份（服务端生成，不进属性表）；其余导出字段映射为节点属性——仅
// bool / string / 各宽度整数 / float（及基本类型切片）可编码；
// 映射/结构体/指针字段不进属性表（Neo4j 属性系统只收标量与标量数组）。
// 嵌入 mixin.TenantID 即启用租户隔离（int64 属性 tenant_id）。
type DocEntity struct {
	UUID  string
	Title string
	Tags  []string
	mixin.TenantID
}

type DocDTO struct {
	UUID  string
	Title string
}
```

### 3. 创建 Client 与 Repository

```go
import (
	neo4j "github.com/tx7do/go-crud/neo4j"
)

cli, err := neo4j.NewClient(
	neo4j.WithURI("bolt://localhost:7687"),
	// 无认证部署省略；有认证时：
	// neo4j.WithBasicAuth("neo4j", "password", ""),
)
if err != nil { panic(err) }
defer cli.Close()

mapperObj := mapper.NewCopierMapper[DocDTO, DocEntity]()
repo := neo4j.NewRepository[DocDTO, DocEntity](cli, "Doc", mapperObj, logger)
```

### 4. 写入 / 读取

```go
// 写入：租户业务视图下 tenant_id 被强制覆盖为当前 Viewer 的租户；
// element id 由 RETURN elementId(n) 回读到 Entity.UUID。
dto, err := repo.Create(ctx, &DocDTO{Title: "hello", Tags: []string{"a"}})

// 按 element id 取回（租户不匹配与不存在同构返回 ErrPointNotFound）。
got, err := repo.GetByUUID(ctx, dto.UUID)

// 按属性条件查询 / 计数（租户视图下自动注入 tenant_id 谓词）。
rows, err := repo.Query(ctx, &neo4j.Query{
	Where:  "n.age > $age",
	Params: map[string]any{"age": 18},
})
n, err := repo.Count(ctx, nil)
```

### 5. 删除

```go
// 按 element id 删除（预计数与删除同条件，防越权删；DETACH DELETE）。
n, err := repo.DeleteByUUIDs(ctx, []string{dto.UUID})

// 按属性条件删除（须显式给出条件，防误删全标签）。
n, err := repo.DeleteByWhere(ctx, &neo4j.Query{Where: "n.expired = $e", Params: map[string]any{"e": true}})
```

## 租户隔离

实体嵌入 `neo4j/mixin.TenantID`（int64 属性，名称固定 tenant_id）即自动
启用，语义与 entgo TenantPrivacy 一致：

| 路径 | 机制 |
| --- | --- |
| Create / BatchCreate | `viewer.EnforceOnScopedInstance` 强制覆盖 tenant_id 属性 |
| GetByUUID / Query / Count / Exists / DeleteByUUIDs / DeleteByWhere | `InjectTenantPredicate` 服务端注入 `n.\`tenant_id\` = $__tid` 谓词（参数化，与调用方 WHERE 括号包裹后 AND 合并） |
| GetByUUID（叠加） | `verifyTenantOnNode` 客户端校验取回节点的属性租户（纵深防御；不匹配与不存在同构，避免存在性泄露） |

缺 ViewerContext → 中止（fail-closed）；平台/系统视图 → 放行。

## API 参考

### Client

| 方法 | 说明 |
| --- | --- |
| `NewClient(opts...)` | 创建客户端（`WithURI` / `WithBasicAuth` / `WithNeo4jDriver` 注入替身驱动）。默认 `bolt://localhost:7687` 无认证 |
| `Close()` | 关闭驱动及连接池 |
| `CheckConnect() bool` | 连接探针（VerifyConnectivity） |

> label 无独立 create/drop 语义（随节点生灭），故无集合管理 API。

### Repository

| 方法 | 说明 |
| --- | --- |
| `Create(ctx, dto)` / `BatchCreate(ctx, dtos)` | 建节点（属性经 `$props` / `UNWIND $rows` 整体写入；element id 回读到 uuid 通道字段） |
| `GetByUUID(ctx, uuid)` | 按 element id 取回（租户谓词 + 客户端校验） |
| `Get(ctx, id)` / `DeleteByIDs(ctx, ids)` | 不支持：element id 是字符串，无数值身份通道（恒返回 ErrInvalidRequest） |
| `Query(ctx, *Query)` / `Count(ctx, *Query)` / `Exists(ctx, *Query)` | 按原生 WHERE 片段列出/计数/存在性（租户谓词注入） |
| `DeleteByUUIDs(ctx, uuids)` / `DeleteByWhere(ctx, *Query)` | 删节点（同一已注入条件先计数后删；DETACH DELETE） |

`Query` 为原生查询条件：`Where` 是 Cypher WHERE 片段（值一律经 `Params`
命名参数传递，不做文本拼接）。分页由调用方经片段自行组合 `SKIP/LIMIT`。

## 局限与边界

- **属性类型**：Neo4j 属性只收标量与标量数组——映射/结构体/指针字段不进
  属性表；写侧整数窄化为 int64（无符号做范围校验）、float32 无损提升为
  float64；读侧为精确类型还原，驱动属性值仅有 int64/float64/string/bool/
  数组，故 **int8/int16/int32/uint*/float32 字段可写不可读**（保持零值）；
- **无数值 ID 通道**：节点身份是 element id（如 `4:abc:123`），`Get` /
  `DeleteByIDs` 恒返回 `ErrInvalidRequest`；数值属性过滤请用 `Query`；
- **图遍历不在仓库面**：仓库面为节点 CRUD 与属性谓词；关系、路径、
  最短路径等图查询由调用方直连 driver 的 Cypher 会话表达（`DETACH DELETE`
  意味着删除节点时连带删除其关系——行语义下关系不是保留对象）；
- **无分页 API**：调用方经 `Query` 片段组合 `SKIP/LIMIT`；
- **无集合管理**：label 随节点生灭，无 create/drop 语义。

## 测试

```bash
# 离线单测（默认；fake driver 全链路覆盖）
go test ./...

# 集成测试（需本地 Neo4j 5，环境变量门禁）
KRATOS_IT=1 go test ./... -run Integration
```

集成测试覆盖租户隔离全链路：写入强制落租户与 element id 回读 → 计数/查询
按租户隔离 → 跨租户取回同构未找到 → 跨租户删除被预计数拦截 → 平台视图
全量清场。

## 依赖

| 模块 | 用途 |
| --- | --- |
| `github.com/neo4j/neo4j-go-driver/v5` | 官方驱动（v5.28.5） |
| `github.com/tx7do/go-crud/viewer` | 租户强制决策 |
| `github.com/tx7do/go-utils/mapper` | DTO ↔ Entity 映射 |
