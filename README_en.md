<p align="center">
  <h1 align="center">go-crud · Universal Data Access Layer Toolkit</h1>
  <p align="center">
    <strong>A single generic Repository interface to unify 8 data storage engines</strong>
  </p>
  <p align="center">
    <em>Stop writing boilerplate — let every line of code focus on business value</em>
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

## Highlights

- **Unified Data Access Layer**: A single generic Repository interface covering GORM, Ent, MongoDB, ClickHouse, Apache Doris, Elasticsearch, OpenSearch, and InfluxDB — say goodbye to repetitive boilerplate
- **Three Pagination Strategies**: Offset / Page / Token pagination modes for traditional web paging, RESTful APIs, and infinite-scroll scenarios
- **Structured Filter Engine**: 29+ operators with AND/OR multi-level nesting, supporting both JSON and Google AIP filter syntaxes with parameterized queries to prevent SQL injection
- **Protocol Buffers Contract**: Standardized pagination, filtering, and sorting definitions via Protobuf — a natural fit for gRPC microservices; interfaces as documentation
- **Redis Cache Layer**: Built-in Cache-Aside pattern with SingleFlight stampede protection; enable caching with a single line of code
- **Audit Logging**: Unified Auditor interface with Context injection for full-chain operation tracing and data change recording
- **Data Access Control**: Viewer context supporting multi-tenant isolation with five data scope levels (SELF / UNIT / USER / ALL / NONE) for fine-grained row-level permissions
- **Fully Type-Safe**: Built on Go 1.24+ generics with bidirectional DTO ↔ Entity mapping via `mapper.CopierMapper`, catching type errors at compile time
- **Upsert Support**: Native Upsert (INSERT ON CONFLICT) support for GORM / ClickHouse / Doris with automatic conflict resolution
- **Tree Queries**: Built-in tree structure assembly in the Ent module, automatically building hierarchical relationships from ParentID

---

## Supported Data Engines

| Engine | Type | Status | Use Cases |
|--------|------|:------:|-----------|
| [GORM](./gorm) | Relational ORM | ✅ | MySQL, PostgreSQL, SQLite, SQL Server and other mainstream relational databases |
| [Ent](./entgo) | Relational ORM (Code Gen) | ✅ | MySQL, PostgreSQL, SQLite — compile-time type safety, open-sourced by Facebook |
| [MongoDB](./mongodb) | Document DB | ✅ | Semi-structured data, flexible schema, content management |
| [ClickHouse](./clickhouse) | Columnar OLAP | ✅ | Massive log analytics, metrics aggregation, user behavior analysis, real-time data warehousing |
| [Apache Doris](./doris) | Columnar OLAP | ✅ | Real-time BI dashboards, interactive analytics, high-speed Stream Load ingestion |
| [Elasticsearch](./elasticsearch) | Search Engine | ✅ | Full-text search, log analysis, highlighted results, aggregation analytics |
| [OpenSearch](./opensearch) | Search Engine | ✅ | Open-source ES alternative, vector search, security analytics |
| [InfluxDB](./influxdb) | Time-Series DB | ✅ | IoT monitoring, DevOps metrics, time-series data analytics |
| [Cassandra](./cassandra) | Wide-Column DB | 🚧 | High-availability writes, cross-datacenter replication (in development) |

---

## System Architecture

```mermaid
graph TB
    subgraph API["API Contract Layer"]
        Proto["Protobuf Definitions<br/>PagingRequest · PaginationRequest<br/>FilterExpr · Sorting · FieldMask"]
    end

    subgraph Infra["Infrastructure Layer"]
        Pagination["Pagination<br/>Paging Strategies · Filter Engine · Sort Conversion"]
        Cache["Cache<br/>Redis Cache-Aside · SingleFlight Stampede Protection"]
        Audit["Audit<br/>Audit Log · Context Injection · Change Tracking"]
        Viewer["Viewer<br/>Identity Context · Permission Checks · Five-Level Data Scope"]
    end

    subgraph DAL["Data Access Layer"]
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

## Project Structure

```
go-crud/
├── api/                          # Protocol Buffers contract definitions & generated code
│   ├── protos/pagination/v1/     # .proto source files (PagingRequest / FilterExpr / Sorting)
│   └── gen/go/pagination/v1/     # buf-generated Go code
├── pagination/                   # Core: pagination, filtering, sorting utilities
│   ├── paginator/                # Paginator implementations (Page / Offset / Token)
│   ├── filter/                   # Filter converters (JSON syntax / Google AIP syntax)
│   └── sorting/                  # Sort format converters
├── cache/                        # Redis cache layer (Cache-Aside + SingleFlight stampede protection)
├── audit/                        # Unified audit logging interface (Auditor · Entry · Context)
├── viewer/                       # Viewer context (identity · permissions · five-level data scope)
├── gorm/                         # GORM data access layer (CRUD · Upsert · Cache · Soft Delete)
├── entgo/                        # Ent data access layer (CRUD · Tree Queries · Cache · Transactions)
├── mongodb/                      # MongoDB data access layer (CRUD · QueryBuilder)
├── clickhouse/                   # ClickHouse data access layer (CRUD · Batch Write · Upsert)
├── doris/                        # Apache Doris data access layer (CRUD · Stream Load · SQL Queries)
├── elasticsearch/                # Elasticsearch client and utilities
├── opensearch/                   # OpenSearch client and utilities
├── influxdb/                     # InfluxDB data access layer (Flux queries)
└── cassandra/                    # Cassandra data access layer (in development)
```

---

## Core Features

### Data Access Layer

Every DAL module provides a unified generic Repository wrapper with bidirectional DTO ↔ Entity auto-mapping via `mapper.CopierMapper[DTO, ENTITY]`:

| Feature | GORM | Ent | MongoDB | ClickHouse | Doris | ES / OS | InfluxDB |
|---------|:----:|:---:|:-------:|:----------:|:-----:|:-------:|:--------:|
| Create / Get / Update / Delete | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Paginated Query (Page / Offset / Token) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Structured Filtering (29+ operators) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Multi-field Sorting | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Field Selection (FieldMask) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Batch Write (BatchCreate) | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Upsert (INSERT ON CONFLICT) | ✅ | — | — | ✅ | ✅ | — | — |
| Soft Delete | ✅ | — | — | — | — | — | — |
| Count / CountWithOptions | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Existence Check (Exists) | ✅ | ✅ | ✅ | — | — | — | ✅ |
| Redis Cache | ✅ | ✅ | — | — | — | — | — |
| Tree Queries | — | ✅ | — | — | — | — | — |
| Transaction Support | ✅ | ✅ | — | — | ✅ | — | — |
| Stream Load | — | — | — | — | ✅ | — | — |
| Raw SQL Queries | — | — | — | — | ✅ | ✅ | — |

### Filter Operators

A structured filter engine defined via Protobuf, supporting 29+ operators:

| Category | Operators |
|----------|-----------|
| Basic Comparison | `EQ` `NEQ` `GT` `GTE` `LT` `LTE` |
| Pattern Matching | `LIKE` `ILIKE` `NOT_LIKE` |
| Set Operations | `IN` `NIN` |
| Null Checks | `IS_NULL` `IS_NOT_NULL` |
| Range & Regex | `BETWEEN` `REGEXP` `IREGEXP` |
| String Operations | `CONTAINS` `STARTS_WITH` `ENDS_WITH` `ICONTAINS` `ISTARTS_WITH` `IENDS_WITH` |
| JSON / Array | `JSON_CONTAINS` `ARRAY_CONTAINS` `EXISTS` |
| Full-Text Search | `SEARCH` `EXACT` `IEXACT` |

Supports `AND` / `OR` multi-level nested combinations via `FilterExpr` for arbitrarily complex query logic.

### Pagination Strategies

| Mode | Use Case | Description |
|------|----------|-------------|
| **Page-Based** | Traditional web paging | Page number + page size, suitable for lists with total page display |
| **Offset-Based** | API skip-page queries | Offset + limit, suitable for flexible page jumping |
| **Token-Based** | Infinite scroll / streaming | Cursor-based pagination with stable performance and no offset drift |

### Cache Layer

| Feature | Description |
|---------|-------------|
| Cache-Aside Pattern | Read-through with write invalidation, ensuring data consistency |
| SingleFlight Stampede Protection | Automatic concurrent request coalescing to protect backend databases |
| Independent TTL per Cache Type | Single-item and list caches support different expiration policies |
| Metrics Monitoring | Built-in cache hit rate, latency, and other metric collection |

### Audit Logging

| Feature | Description |
|---------|-------------|
| Auditor Interface | Unified audit log recording interface, supporting both sync and async buffering |
| Context Injection | Audit information transparently propagated via Context for non-intrusive integration |
| Entry Data Model | Standardized audit record structure including operator, operation type, and change content |
| Noop Implementation | Built-in no-op implementation with zero overhead when audit is not needed |

### Data Access Control

| Scope Level | Description |
|-------------|-------------|
| **SELF** | Only data created / owned by the current user |
| **UNIT** | Organization-level isolation, supporting current department and sub-departments |
| **USER** | Specified user list |
| **ALL** | Full access without filter injection |
| **NONE** | Deny all data access |

---

## Tech Stack

| Layer | Technology | Description |
|-------|------------|-------------|
| Language | Go 1.24+ | High-performance compiled language with generics |
| ORM | GORM / Ent | Mainstream relational ORMs — pick what fits |
| Document DB | MongoDB | NoSQL document storage |
| OLAP Engine | ClickHouse / Apache Doris | Columnar storage for extreme analytical performance |
| Search Engine | Elasticsearch / OpenSearch | Full-text search and data analytics |
| Time-Series DB | InfluxDB | Time-series data collection and analytics |
| Cache | Redis | In-memory data store with stampede protection |
| DTO Mapping | go-utils/mapper | Generic CopierMapper with bidirectional auto-mapping |
| API Definition | Protobuf + buf.build | Contract-first API design, cross-language support |
| Logging | go-wind/log | go-wind framework log integration |
| Observability | OpenTelemetry | Distributed tracing and metrics (GORM / Ent) |

---

## Quick Start

### Installation

```bash
# Install only what you need

go get github.com/tx7do/go-crud/gorm         # GORM
go get github.com/tx7do/go-crud/entgo         # Ent
go get github.com/tx7do/go-crud/mongodb       # MongoDB
go get github.com/tx7do/go-crud/clickhouse    # ClickHouse
go get github.com/tx7do/go-crud/doris         # Apache Doris
go get github.com/tx7do/go-crud/elasticsearch # Elasticsearch
go get github.com/tx7do/go-crud/opensearch    # OpenSearch
go get github.com/tx7do/go-crud/influxdb      # InfluxDB
```

### Example: GORM Repository

```go
package main

import (
    "context"
    "fmt"

    "github.com/tx7do/go-crud/gorm"
    "github.com/tx7do/go-utils/mapper"
    paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

// 1. Define Entity (database table mapping)
type UserEntity struct {
    ID    uint64 `gorm:"primaryKey;autoIncrement"`
    Name  string `gorm:"column:name;type:varchar(100)"`
    Email string `gorm:"column:email;type:varchar(200)"`
}

func (UserEntity) TableName() string { return "users" }

// 2. Create Repository
func main() {
    ctx := context.Background()

    m := mapper.NewCopierMapper[User, UserEntity]()
    repo := gorm.NewRepository[User, UserEntity](m)

    // Create record
    user, _ := repo.Create(ctx, db, &User{Name: "John", Email: "john@example.com"}, nil)

    // Paginated query
    page := uint32(1)
    pageSize := uint32(10)
    result, _ := repo.ListWithPaging(ctx, db, &paginationV1.PagingRequest{
        Page:     &page,
        PageSize: &pageSize,
    })
    fmt.Printf("Total: %d, Items: %d\n", result.Total, len(result.Items))
}
```

### Example: ClickHouse Repository

```go
package main

import (
    "github.com/tx7do/go-crud/clickhouse"
    "github.com/tx7do/go-utils/mapper"
)

func main() {
    // Create Client
    client, _ := clickhouse.NewClient(
        clickhouse.WithDsn("clickhouse://default:123456@localhost:9000/my_database"),
    )

    // Create Repository
    m := mapper.NewCopierMapper[Event, EventEntity]()
    repo := clickhouse.NewRepository[Event, EventEntity](client, m, "events", logger)

    // Batch insert
    events := []*Event{{...}, {...}}
    repo.BatchCreate(ctx, events, nil)

    // Paginated query
    result, _ := repo.ListWithPaging(ctx, req)
}
```

### Example: Structured Filtering

```go
// Build complex queries using Protobuf FilterExpr
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

## Comparison with Alternatives

| Feature | go-crud | Hand-written Repository | Other CRUD Libraries |
|---------|---------|------------------------|---------------------|
| Multi-engine unified API | ✅ 8 engines | ❌ Write each manually | ❌ Usually one engine |
| Generic type safety | ✅ Bidirectional DTO ↔ Entity mapping | ⚠️ Varies | ⚠️ Partial |
| Protocol Buffers contract | ✅ Standardized interface definitions | ❌ | ❌ |
| Structured filter engine | ✅ 29+ operators + AND/OR nesting | ❌ | ⚠️ Basic filtering |
| Three pagination strategies | ✅ Page / Offset / Token | ❌ | ❌ |
| Built-in caching | ✅ Cache-Aside + stampede protection | ❌ | ❌ |
| Audit logging | ✅ Full-chain tracing | ❌ | ❌ |
| Data access control | ✅ Five-level data scope | ❌ | ❌ |
| Upsert support | ✅ INSERT ON CONFLICT | ⚠️ Manual | ❌ |
| OLAP engine support | ✅ ClickHouse + Doris | ❌ | ❌ |
| Search engine support | ✅ Elasticsearch + OpenSearch | ❌ | ❌ |
| Time-series DB support | ✅ InfluxDB | ❌ | ❌ |

---

## Contributing

Issues and Pull Requests are welcome. Before contributing, please ensure:

- Code passes `go vet` checks
- New features have corresponding unit tests
- Code follows the project's existing conventions

## License

This project is licensed under the [MIT License](./LICENSE). Free to use, modify, and distribute.
