# Cassandra Package

基于 [gocql](https://github.com/gocql/gocql) 的 Cassandra 数据访问层，与
entgo / gorm / mongodb 等模块同级的泛型 DAL 封装。

本模块按**行语义**使用 Cassandra：表须为简单主键（单一分区键、不含
clustering columns），一行 = 一个分区键实例，列 = 属性。客户端层提供裸
CQL 执行器（Exec/Query/ExecBatch），仓库层提供泛型 CRUD 与租户强制。

> 本模块针对 Cassandra 3.11 / 4.x / 5.x（CQL 协议稳定）。

## 特性

- **泛型仓库**：`Repository[DTO, ENTITY]` + `mapper.CopierMapper`，DTO 与
  存储实体解耦；
- **参数化 CQL**：所有语句的值一律经 `?` 位置占位符绑定；表名/键空间
  限定名/列名一律经标识符白名单校验——无文本拼接注入面；
- **Upsert 语义**：CQL INSERT 天然为同主键覆盖，与仓库 Create 契约一致；
- **多租户强制**（fail-closed）：实体嵌入 `cassandra/mixin.TenantID` 即
  启用，写入强制覆盖 tenant_id 列，可注入 WHERE 的路径服务端注入
  `tenant_id = ?` 谓词，主键直取路径客户端校验行租户；
- **离线可测**：gocql.Session 为具体类型无法替身，仓库只依赖模块内的
  `sessionExecutor` 接口（*Client 为默认实现），测试注入记录型替身即可
  全链路锁定仓库层。

## Docker 部署

```bash
docker run -d --name cassandra-it -p 127.0.0.1:9042:9042 -e CASSANDRA_CLUSTER_NAME=go-crud-it cassandra:5
```

- CQL：`localhost:9042`（容器引导约 1-2 分钟）

## 快速开始

### 1. 安装依赖

```bash
go get github.com/tx7do/go-crud/cassandra
```

### 2. 定义 Entity（含租户 mixin）与 DTO

```go
import (
	"github.com/tx7do/go-crud/cassandra/mixin"
)

// Entity：id/uuid（大小写不敏感）命名的字段承载行主键（简单主键）；
// 其余导出字段映射为普通列——cql 标签 name 控制列名（默认字段名）、
// "-" 跳过；可编码类型为 bool / 各宽度有符号整数 / float / string /
// time.Time / 基本类型切片 / string 键基本类型映射；无符号整数与
// 结构体字段在编码时报错（响亮失败优于静默丢列）。
// 嵌入 mixin.TenantID 即启用租户隔离（bigint 列 tenant_id）。
type DocEntity struct {
	ID    int64
	Title string   `cql:"name:title"`
	Tags  []string `cql:"name:tags"`
	mixin.TenantID
}

type DocDTO struct {
	ID    int64
	Title string
}
```

### 3. 建连接、建表（DDL）、创建仓库

```go
import (
	cassandra "github.com/tx7do/go-crud/cassandra"
)

cli, err := cassandra.NewCassandraClient(
	cassandra.WithHosts("127.0.0.1"),
	// Docker 端口映射后主机发现会失败，建议打开：
	cassandra.WithDisableInitialHostLookup(true),
	// 可选：cassandra.WithKeyspace("docs")（绑死后仓库可用裸表名）
)
if err != nil { panic(err) }
defer cli.Close()

// DDL 由调用方经 Client.Exec 表达（keyspace/表/索引均幂等）。
ctx := context.Background()
_ = cli.Exec(ctx, "CREATE KEYSPACE IF NOT EXISTS docs WITH replication = {'class':'SimpleStrategy','replication_factor':1}")
_ = cli.Exec(ctx, "CREATE TABLE IF NOT EXISTS docs.docs (id bigint PRIMARY KEY, title text, tags list<text>, tenant_id bigint)")
// 租户谓词的服务端过滤依赖二级索引（或查询置 AllowFiltering）：
_ = cli.Exec(ctx, "CREATE INDEX IF NOT EXISTS docs_docs_tenant ON docs.docs (tenant_id)")

mapperObj := mapper.NewCopierMapper[DocDTO, DocEntity]()
repo := cassandra.NewRepository[DocDTO, DocEntity](cli, "docs.docs", mapperObj, logger)
```

### 4. 写入 / 读取

```go
// 写入：租户业务视图下 tenant_id 被强制覆盖为当前 Viewer 的租户
//（CQL INSERT 天然 Upsert：同主键覆盖）。
_, err := repo.Create(ctx, &DocDTO{ID: 1, Title: "hello", Tags: []string{"a"}})

// 按主键取回（租户不匹配与不存在同构返回 ErrPointNotFound）。
got, err := repo.Get(ctx, 1)          // bigint/text 主键按实体字段类型分派
got, err = repo.GetByUUID(ctx, "…")   // 字符串主键（text/uuid 列）

// 按条件查询 / 计数（租户视图下自动注入 tenant_id = ? 谓词）。
rows, err := repo.Query(ctx, &cassandra.Query{
	Where: "title = ?", Args: []any{"hello"},
	// AllowFiltering: true, // 未建索引的非主键过滤需允许过滤
})
n, err := repo.Count(ctx, nil)
```

### 5. 删除

```go
// 按主键删除：先按租户预过滤主键（IN + 租户谓词），再按过滤后的主键
// 删除并回报计数（越权主键计 0）。
n, err := repo.DeleteByIDs(ctx, []uint64{1})
n, err = repo.DeleteByUUIDs(ctx, []string{"…"})

// 按条件删除：CQL 的 DELETE 不接受主键外条件——以同一（已注入租户）
// 条件先查出主键再删。
n, err := repo.DeleteByWhere(ctx, &cassandra.Query{Where: "expired = ?", Args: []any{true}})
```

## 租户隔离

实体嵌入 `cassandra/mixin.TenantID`（bigint 列，名称固定 tenant_id）即
自动启用，语义与 entgo TenantPrivacy 一致：

| 路径 | 机制 |
| --- | --- |
| Create / BatchCreate | `viewer.EnforceOnScopedInstance` 强制覆盖 tenant_id 列 |
| Query / Count / Exists / DeleteByWhere / DeleteByIDs / DeleteByUUIDs | `InjectTenantWhere` 服务端注入 `tenant_id = ?` 谓词（与调用方 WHERE 括号包裹后 AND 合并，参数按位置追加） |
| Get / GetByUUID | 主键直取路径客户端校验行租户（不匹配与不存在同构，避免存在性泄露）；删除路径的主键预过滤同源 |

缺 ViewerContext → 中止（fail-closed）；平台/系统视图 → 放行。

> **tenant_id 二级索引**：租户谓词命中非主键列——表须建二级索引
>（`CREATE INDEX … ON table (tenant_id)`）或查询置 `AllowFiltering`，
> 否则服务端拒绝。删除路径的预过滤 SELECT 因受主键 IN 约束（仅读指定
> 分区）恒追加 ALLOW FILTERING，无全表扫描风险。

## API 参考

### Client（裸 CQL 执行器）

| 方法 | 说明 |
| --- | --- |
| `NewCassandraClient(opts...)` | 建连（`WithHosts` / `WithUsername` / `WithPassword` / `WithKeyspace` / `WithTLSConfig` / `WithConsistency` / `WithDisableInitialHostLookup` 等） |
| `Exec(ctx, stmt, args...)` | 执行无结果集语句（含 DDL） |
| `Query(ctx, stmt, args...)` | 执行查询（返回 `*Rows`，迭代后须 Close） |
| `ExecBatch` / `ExecBatchBound` | 批量执行（语句对 / 绑定函数） |
| `Select(ctx, stmt, args...)` | 查询并以「列名 → 值」映射返回全部行（仓库内部通道） |
| `Session()` / `Close()` / `Closed()` | 透出 gocql 会话 / 关闭 / 状态 |

### Repository

| 方法 | 说明 |
| --- | --- |
| `Create(ctx, dto)` / `BatchCreate(ctx, dtos)` | INSERT Upsert（逐实体强制租户） |
| `Get(ctx, id)` / `GetByUUID(ctx, uuid)` | 按主键直取（客户端租户校验；主键类型与通道不匹配报 ErrInvalidRequest） |
| `Query(ctx, *Query)` / `Count(ctx, *Query)` / `Exists(ctx, *Query)` | 按原生 WHERE 片段列出/计数/存在性（租户谓词注入；count(*) AS row_count 回读） |
| `DeleteByIDs(ctx, ids)` / `DeleteByUUIDs(ctx, uuids)` | 主键租户预过滤后删除（计数取过滤结果） |
| `DeleteByWhere(ctx, *Query)` | 同条件先查主键后删（必须显式给出条件） |

> 不提供 SearchByVector：Cassandra 5 的 vector 类型与 ANN 不在本模块
> 适配面。分页由调用方经 Query 片段组合（`Rows.PageState` 已在 Client
> 层透出）。

## 局限与边界

- **简单主键约束**：行语义要求单一分区键、无 clustering columns——
  复合分区键/聚簇列场景请直接使用 Client 层裸 CQL；
- **属性类型**：无符号整数与结构体/指针字段不编码（响亮报错）；解码按
  gocql 列类型映射精确回填——`int` 列产出 Go `int`（非 int32）、
  `uuid` 列产出 `gocql.UUID`（特判回填 string 字段），类型不符的字段
  保持零值；
- **删除非原子**：按条件的删除是「先查主键再删」两步（CQL 限制），
  与 milvus 模块的先过滤后删同语义；
- **无 schema 管理 API**：DDL 经 Client.Exec 表达（见快速开始）。

## 测试

```bash
# 离线单测（默认；fake executor 全链路覆盖）
go test ./...

# 集成测试（需本地 Cassandra，环境变量门禁）
KRATOS_IT=1 go test ./... -run Integration
```

集成测试覆盖租户隔离全链路：写入强制落租户 → 计数/查询按租户隔离 →
跨租户取回同构未找到 → 跨租户删除被预过滤拦截 → 平台视图全量清场。

## ScyllaDB 兼容性

[ScyllaDB](https://www.scylladb.com/) 与 Cassandra 使用同一套 CQL 二进制协议，
本模块（gocql 驱动）**无需任何改动即可直连 ScyllaDB**——把 `WithHosts` 指向
ScyllaDB 节点即可。两点差异需要知晓：

- Scylla 官方维护的驱动是其 gocql 分叉（shard-aware 连接路由等性能优化）；
  本模块按生态通用性选择上游 gocql，连接 Scylla 功能完整，但不含分片感知优化。
- Scylla 不支持 Cassandra 的全部特性（如物化视图等），以其官方兼容矩阵为准。

## 依赖

| 模块 | 用途 |
| --- | --- |
| `github.com/gocql/gocql` | CQL 驱动（v1.7.0） |
| `github.com/tx7do/go-crud/viewer` | 租户强制决策 |
| `github.com/tx7do/go-utils/mapper` | DTO ↔ Entity 映射 |
