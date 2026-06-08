<p align="center">
  <h1 align="center">go-crud · 通用数据访问层工具库</h1>
  <p align="center">
    <strong>一套泛型 Repository 接口，统一驾驭 8 种数据存储引擎</strong>
  </p>
  <p align="center">
    <em>让数据操作不再是重复劳动，让每一行代码都聚焦业务价值</em>
  </p>
</p>

<p align="center">
  <a href="README.md">中文</a> · <a href="README_en.md">English</a> · <a href="README_ja.md">日本語</a>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.24+-00ADD8?style=flat-square&logo=Go" alt="Go Version" />
  <img src="https://img.shields.io/badge/License-MIT-green?style=flat-square" alt="License" />
  <img src="https://img.shields.io/badge/PRs-Welcome-brightgreen?style=flat-square" alt="PRs Welcome" />
  <img src="https://pkg.go.dev/badge/github.com/tx7do/go-crud.svg" alt="Go Reference" />
</p>

---

## 项目亮点

- **统一数据访问层**：一套泛型 Repository 接口，覆盖 GORM、Ent、MongoDB、ClickHouse、Apache Doris、Elasticsearch、OpenSearch、InfluxDB 八大数据引擎，告别重复 Boilerplate
- **三种分页策略**：Offset / Page / Token 三种分页模式，从传统 Web 分页到无限滚动，全场景覆盖
- **结构化过滤引擎**：29+ 种操作符，支持 AND/OR 多层嵌套，同时兼容 JSON 与 Google AIP 两种过滤语法，参数化查询杜绝 SQL 注入
- **Protocol Buffers 契约**：基于 Protobuf 定义标准化的分页、过滤、排序协议，天然适配 gRPC 微服务架构，接口即文档
- **Redis 缓存层**：内置 Cache-Aside 模式与 SingleFlight 防击穿机制，一行代码开启缓存，保护后端数据库
- **审计日志**：统一的 Auditor 接口，Context 注入、全链路操作追溯与数据变更记录
- **数据权限控制**：Viewer 上下文支持多租户隔离、五级数据范围（SELF / UNIT / USER / ALL / NONE），精细化的行级权限
- **完全类型安全**：基于 Go 1.24+ 泛型，DTO ↔ Entity 双向映射，编译时即可捕获类型错误
- **Upsert 支持**：GORM / ClickHouse / Doris 原生支持 Upsert（INSERT ON CONFLICT），冲突自动更新
- **树形查询**：Ent 模块内置树形结构组装，自动根据 ParentID 构建层级关系

---

## 支持的数据引擎

| 引擎 | 类型 | 状态 | 适用场景 |
|------|------|:----:|----------|
| [GORM](./gorm) | 关系型 ORM | ✅ | MySQL、PostgreSQL、SQLite、SQL Server 等主流关系型数据库 |
| [Ent](./entgo) | 关系型 ORM (Code Gen) | ✅ | MySQL、PostgreSQL、SQLite，编译时类型安全，Facebook 开源 |
| [MongoDB](./mongodb) | 文档数据库 | ✅ | 半结构化数据、灵活 Schema、内容管理 |
| [ClickHouse](./clickhouse) | 列式 OLAP | ✅ | 海量日志分析、指标聚合、用户行为分析、实时数仓 |
| [Apache Doris](./doris) | 列式 OLAP | ✅ | 实时 BI 报表、交互式分析、Stream Load 高速写入 |
| [Elasticsearch](./elasticsearch) | 搜索引擎 | ✅ | 全文检索、日志分析、高亮搜索、聚合分析 |
| [OpenSearch](./opensearch) | 搜索引擎 | ✅ | Elasticsearch 开源替代、向量检索、安全分析 |
| [InfluxDB](./influxdb) | 时序数据库 | ✅ | IoT 监控、DevOps 指标、时序数据分析 |
| [Cassandra](./cassandra) | 宽列数据库 | 🚧 | 高可用写入、跨数据中心复制（开发中） |

---

## 系统架构

```mermaid
graph TB
    subgraph API["API 契约层"]
        Proto["Protobuf 定义<br/>PagingRequest · PaginationRequest<br/>FilterExpr · Sorting · FieldMask"]
    end

    subgraph Infra["基础设施层"]
        Pagination["Pagination<br/>分页策略 · 过滤引擎 · 排序转换"]
        Cache["Cache<br/>Redis Cache-Aside · SingleFlight 防击穿"]
        Audit["Audit<br/>审计日志 · Context 注入 · 变更追踪"]
        Viewer["Viewer<br/>身份上下文 · 权限检查 · 五级数据范围"]
    end

    subgraph DAL["数据访问层"]
        GORM["GORM"]
        ENT["Ent"]
        Mongo["MongoDB"]
        CH["ClickHouse"]
        Doris["Apache Doris"]
        ES["Elasticsearch"]
        OS["OpenSearch"]
        Influx["InfluxDB"]
    end

    API --> Pagination
    Pagination --> DAL
    Cache --> DAL
    Audit --> DAL
    Viewer --> DAL
```

---

## 项目结构

```
go-crud/
├── api/                          # Protocol Buffers 契约定义与生成代码
│   ├── protos/pagination/v1/     # .proto 源文件 (PagingRequest / FilterExpr / Sorting)
│   └── gen/go/pagination/v1/     # buf 生成的 Go 代码
├── pagination/                   # 分页 · 过滤 · 排序核心工具包
│   ├── paginator/                # 分页器实现 (Page / Offset / Token)
│   ├── filter/                   # 过滤器转换器 (JSON 语法 / Google AIP 语法)
│   └── sorting/                  # 排序格式转换器
├── cache/                        # Redis 缓存层 (Cache-Aside + SingleFlight 防击穿)
├── audit/                        # 统一审计日志接口 (Auditor · Entry · Context)
├── viewer/                       # 查看者上下文 (身份 · 权限 · 五级数据范围)
├── gorm/                         # GORM 数据访问层 (CRUD · Upsert · 缓存 · 软删除)
├── entgo/                        # Ent 数据访问层 (CRUD · 树形查询 · 缓存 · 事务)
├── mongodb/                      # MongoDB 数据访问层 (CRUD · QueryBuilder)
├── clickhouse/                   # ClickHouse 数据访问层 (CRUD · 批量写入 · Upsert)
├── doris/                        # Apache Doris 数据访问层 (CRUD · Stream Load · SQL 查询)
├── elasticsearch/                # Elasticsearch 客户端与工具
├── opensearch/                   # OpenSearch 客户端与工具
├── influxdb/                     # InfluxDB 数据访问层 (Flux 查询)
└── cassandra/                    # Cassandra 数据访问层 (开发中)
```

---

## 核心功能

### 数据访问层

每个数据访问层模块均提供统一的泛型 Repository 封装，基于 `mapper.CopierMapper[DTO, ENTITY]` 实现 DTO ↔ Entity 双向自动映射：

| 能力 | GORM | Ent | MongoDB | ClickHouse | Doris | ES / OS | InfluxDB |
|------|:----:|:---:|:-------:|:----------:|:-----:|:-------:|:--------:|
| Create / Get / Update / Delete | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 分页查询 (Page / Offset / Token) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 结构化过滤 (29+ 操作符) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 多字段排序 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 字段选择 (FieldMask) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 批量写入 (BatchCreate) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Upsert (INSERT ON CONFLICT) | ✅ | — | — | ✅ | ✅ | — | — |
| 软删除 (SoftDelete) | ✅ | — | — | — | — | — | — |
| 计数 (Count / CountWithOptions) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| 存在性检查 (Exists) | ✅ | ✅ | ✅ | — | — | — | ✅ |
| Redis 缓存 | ✅ | ✅ | — | — | — | — | — |
| 树形查询 | — | ✅ | — | — | — | — | — |
| 事务支持 | ✅ | ✅ | — | — | ✅ | — | — |
| Stream Load | — | — | — | — | ✅ | — | — |
| SQL 原生查询 | — | — | — | — | ✅ | ✅ | — |

### 过滤操作符

基于 Protobuf 定义的结构化过滤引擎，支持 29+ 种操作符：

| 分类 | 操作符 |
|------|--------|
| 基本比较 | `EQ` `NEQ` `GT` `GTE` `LT` `LTE` |
| 模糊匹配 | `LIKE` `ILIKE` `NOT_LIKE` |
| 集合操作 | `IN` `NIN` |
| 空值判断 | `IS_NULL` `IS_NOT_NULL` |
| 范围与正则 | `BETWEEN` `REGEXP` `IREGEXP` |
| 字符串操作 | `CONTAINS` `STARTS_WITH` `ENDS_WITH` `ICONTAINS` `ISTARTS_WITH` `IENDS_WITH` |
| JSON / 数组 | `JSON_CONTAINS` `ARRAY_CONTAINS` `EXISTS` |
| 全文检索 | `SEARCH` `EXACT` `IEXACT` |

支持 `AND` / `OR` 多层嵌套组合，通过 `FilterExpr` 表达任意复杂的查询逻辑。

### 分页策略

| 模式 | 适用场景 | 说明 |
|------|----------|------|
| **Page-Based** | 传统 Web 分页 | 页码 + 页大小，适合有总页数展示的列表 |
| **Offset-Based** | API 跳页查询 | 偏移量 + 限制数，适合灵活跳页 |
| **Token-Based** | 无限滚动 / 流式加载 | 基于游标的分页，性能稳定不偏移 |

### 缓存层

| 能力 | 说明 |
|------|------|
| Cache-Aside 模式 | 读时回填，写时失效，保证数据一致性 |
| SingleFlight 防击穿 | 并发请求自动合并，保护后端数据库 |
| 单条 / 列表独立 TTL | 单条缓存与列表缓存支持不同的过期策略 |
| 指标监控 | 内置缓存命中率、延迟等指标采集 |

### 审计日志

| 能力 | 说明 |
|------|------|
| Auditor 接口 | 统一的审计日志记录接口，支持同步写入与异步缓冲 |
| Context 注入 | 通过 Context 透传审计信息，无侵入式集成 |
| Entry 数据模型 | 标准化的审计记录结构，包含操作者、操作类型、变更内容 |
| Noop 实现 | 内置空实现，无审计需求时零开销 |

### 数据权限控制

| 范围级别 | 说明 |
|----------|------|
| **SELF** | 仅限本人创建 / 拥有的数据 |
| **UNIT** | 组织维度隔离，支持本部门及下级部门 |
| **USER** | 指定的用户列表 |
| **ALL** | 全量放行，不注入过滤条件 |
| **NONE** | 禁止任何数据访问 |

---

## 技术栈

| 层级 | 技术 | 说明 |
|------|------|------|
| 语言 | Go 1.24+ | 高性能编译型语言，泛型支持 |
| ORM | GORM / Ent | 主流关系型 ORM，按需选用 |
| 文档数据库 | MongoDB | NoSQL 文档存储 |
| OLAP 引擎 | ClickHouse / Apache Doris | 列式存储，极致分析性能 |
| 搜索引擎 | Elasticsearch / OpenSearch | 全文检索与数据分析 |
| 时序数据库 | InfluxDB | 时序数据采集与分析 |
| 缓存 | Redis | 内存数据库，防击穿保护 |
| DTO 映射 | go-utils/mapper | 泛型 CopierMapper，双向自动映射 |
| API 定义 | Protobuf + buf.build | 接口契约优先，跨语言支持 |
| 日志 | go-kratos/log | Kratos 框架日志集成 |
| 可观测性 | OpenTelemetry | 分布式追踪与指标（GORM / Ent） |

---

## 快速开始

### 安装

```bash
# 按需安装所需模块

go get github.com/tx7do/go-crud/gorm         # GORM
go get github.com/tx7do/go-crud/entgo         # Ent
go get github.com/tx7do/go-crud/mongodb       # MongoDB
go get github.com/tx7do/go-crud/clickhouse    # ClickHouse
go get github.com/tx7do/go-crud/doris         # Apache Doris
go get github.com/tx7do/go-crud/elasticsearch # Elasticsearch
go get github.com/tx7do/go-crud/opensearch    # OpenSearch
go get github.com/tx7do/go-crud/influxdb      # InfluxDB
```

### 示例：GORM Repository

```go
package main

import (
    "context"
    "fmt"

    "github.com/tx7do/go-crud/gorm"
    "github.com/tx7do/go-utils/mapper"
    paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

// 1. 定义 Entity（数据库表映射）
type UserEntity struct {
    ID    uint64 `gorm:"primaryKey;autoIncrement"
    Name  string `gorm:"column:name;type:varchar(100)"
    Email string `gorm:"column:email;type:varchar(200)"
}

func (UserEntity) TableName() string { return "users" }

// 2. 创建 Repository
func main() {
    ctx := context.Background()

    m := mapper.NewCopierMapper[User, UserEntity]()
    repo := gorm.NewRepository[User, UserEntity](m)

    // 创建记录
    user, _ := repo.Create(ctx, db, &User{Name: "John", Email: "john@example.com"}, nil)

    // 分页查询
    page := uint32(1)
    pageSize := uint32(10)
    result, _ := repo.ListWithPaging(ctx, db, &paginationV1.PagingRequest{
        Page:     &page,
        PageSize: &pageSize,
    })
    fmt.Printf("Total: %d, Items: %d\n", result.Total, len(result.Items))
}
```

### 示例：ClickHouse Repository

```go
package main

import (
    "github.com/tx7do/go-crud/clickhouse"
    "github.com/tx7do/go-utils/mapper"
)

func main() {
    // 创建 Client
    client, _ := clickhouse.NewClient(
        clickhouse.WithDsn("clickhouse://default:123456@localhost:9000/my_database"),
    )

    // 创建 Repository
    m := mapper.NewCopierMapper[Event, EventEntity]()
    repo := clickhouse.NewRepository[Event, EventEntity](client, m, "events", logger)

    // 批量写入
    events := []*Event{{...}, {...}}
    repo.BatchCreate(ctx, events, nil)

    // 分页查询
    result, _ := repo.ListWithPaging(ctx, req)
}
```

### 示例：结构化过滤

```go
// 使用 Protobuf FilterExpr 构建复杂查询
page := uint32(1)
pageSize := uint32(10)
result, _ := repo.ListWithPaging(ctx, db, &paginationV1.PagingRequest{
    Page:     &page,
    PageSize: &pageSize,
    FilteringType: &paginationV1.PagingRequest_FilterExpr{
        FilterExpr: &paginationV1.FilterExpr{
            Type: paginationV1.ExprType_AND,
            Conditions: []*paginationV1.FilterCondition{
                {Field: "status", Op: paginationV1.Operator_EQ, Value: &paginationV1.FilterCondition_Value{Value: "active"}},
                {Field: "age", Op: paginationV1.Operator_GTE, Value: &paginationV1.FilterCondition_Value{Value: "18"}},
            },
        },
    },
})
```

---

## 与同类项目的区别

| 特性 | go-crud | 纯手写 Repository | 其他 CRUD 库 |
|------|---------|-------------------|-------------|
| 多引擎统一 API | ✅ 8 种引擎 | ❌ 每种手写 | ❌ 通常只支持一种 |
| 泛型类型安全 | ✅ DTO ↔ Entity 双向映射 | ⚠️ 视实现而定 | ⚠️ 部分支持 |
| Protocol Buffers 契约 | ✅ 标准化接口定义 | ❌ | ❌ |
| 结构化过滤引擎 | ✅ 29+ 操作符 + AND/OR 嵌套 | ❌ | ⚠️ 基础过滤 |
| 三种分页策略 | ✅ Page / Offset / Token | ❌ | ❌ |
| 内置缓存 | ✅ Cache-Aside + 防击穿 | ❌ | ❌ |
| 审计日志 | ✅ 全链路追踪 | ❌ | ❌ |
| 数据权限控制 | ✅ 五级数据范围 | ❌ | ❌ |
| Upsert 支持 | ✅ INSERT ON CONFLICT | ⚠️ 手写 | ❌ |
| OLAP 引擎支持 | ✅ ClickHouse + Doris | ❌ | ❌ |
| 搜索引擎支持 | ✅ Elasticsearch + OpenSearch | ❌ | ❌ |
| 时序数据库支持 | ✅ InfluxDB | ❌ | ❌ |

---

## 贡献

欢迎提交 Issue 或 Pull Request 参与项目改进。贡献前请确保：

- 代码通过 `go vet` 检查
- 新功能有对应的单元测试
- 遵循项目现有的代码规范

## 许可证

本项目基于 [MIT 许可证](./LICENSE) 开源，允许自由使用、修改和分发。
