<p align="center">
  <h1 align="center">go-crud · Universal Data Access Layer Toolkit</h1>
  <p align="center">
    <strong>A single generic Repository interface to unify 10 data storage engines</strong>
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

- **Unified Data Access Layer**: A single generic Repository interface covering GORM, Ent, MongoDB, ClickHouse, Apache Doris, Elasticsearch, OpenSearch, Qdrant, Milvus, Weaviate, Neo4j, and InfluxDB — twelve data engines in total, say goodbye to repetitive boilerplate
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
| [Qdrant](./qdrant) | Vector Database | ✅ | RAG retrieval, semantic search, recommendation recall, multi-tenant vector isolation |
| [Milvus](./milvus) | Vector Database | ✅ | RAG retrieval, semantic search, recommendation recall, multi-tenant vector isolation |
| [Weaviate](./weaviate) | Vector Database | ✅ | RAG retrieval, semantic search, multi-tenant vector isolation (GraphQL search) |
| [Neo4j](./neo4j) | Graph Database | ✅ | Node CRUD, property-level multi-tenancy (label as table, element id as row identity) |
| [InfluxDB](./influxdb) | Time-Series DB | ✅ | IoT monitoring, DevOps metrics, time-series data analytics |
| [Cassandra](./cassandra) | Wide-Column DB | ✅ | High-availability writes, cross-datacenter replication, row-level multi-tenancy (direct ScyllaDB compatibility) |

### Compatible Ecosystem

The following engines reuse existing modules via wire-protocol compatibility — no new dependencies:

| Compatible Engine | Reused Module | Notes |
|----------|----------|------|
| TiDB / OceanBase | GORM | MySQL wire protocol compatible |
| CockroachDB / YugabyteDB / openGauss / KingbaseES | GORM | PostgreSQL wire protocol compatible |
| Dameng DM8 | GORM | Community gorm driver (dm-go) |
| TimescaleDB | GORM | PostgreSQL extension; vectors via pgvector |
| ScyllaDB | Cassandra | Same CQL binary protocol, direct connection (see cassandra/README compatibility note) |
| AWS DocumentDB / Azure Cosmos DB (Mongo API) | MongoDB | Mongo wire protocol compatible |
| Zilliz Cloud | Milvus | Official SDK compatible |

> Closed-source SaaS (Pinecone etc.) and multi-model newcomers are not on the roadmap;
> Redis is positioned as the [cache](./cache) layer in this library, not a primary storage engine.

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
        Vector["Vector<br/>Vector Search Contract · Unified Distance Metrics"]
    end

    subgraph DAL["Data Access Layer"]
        GORM["GORM"]
        ENT["Ent"]
        Mongo["MongoDB"]
        CH["ClickHouse"]
        Doris["Apache Doris"]
        ES["Elasticsearch"]
        OS["OpenSearch"]
        Qdrant["Qdrant"]
        Milvus["Milvus"]
        Weaviate["Weaviate"]
        Neo4j["Neo4j"]
        Influx["InfluxDB"]
        Cassandra["Cassandra"]
    end

    API --> Pagination
    Pagination --> DAL
    Cache --> DAL
    Audit --> DAL
    Viewer --> DAL
    Vector --> DAL
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
├── vector/                       # Vector search contract (Query/Result · distance metrics · pgvector text codec)
├── gorm/                         # GORM data access layer (CRUD · Upsert · Cache · Soft Delete)
├── entgo/                        # Ent data access layer (CRUD · Tree Queries · Cache · Transactions)
├── mongodb/                      # MongoDB data access layer (CRUD · QueryBuilder)
├── clickhouse/                   # ClickHouse data access layer (CRUD · Batch Write · Upsert)
├── doris/                        # Apache Doris data access layer (CRUD · Stream Load · SQL Queries)
├── elasticsearch/                # Elasticsearch client and utilities
├── opensearch/                   # OpenSearch client and utilities
├── qdrant/                       # Qdrant data access layer (vector search · tenant isolation)
├── milvus/                       # Milvus data access layer (vector search · tenant isolation)
├── weaviate/                     # Weaviate data access layer (vector search · tenant isolation)
├── neo4j/                        # Neo4j data access layer (node CRUD · property-level tenancy)
├── influxdb/                     # InfluxDB data access layer (Flux queries)
├── cassandra/                    # Cassandra data access layer (raw CQL executor · generic repository · tenant isolation)
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
| Vector Search (kNN / TopK) | ✅ pgvector | — | ✅ Atlas | ✅ | ✅ | ✅ kNN | — |

### Vector Search (RAG / Semantic Search)

A cross-engine unified vector search contract via the standalone [vector](./vector) module: `vector.Query` expresses the request (vector field, query vector, TopK, distance metric, metadata filter), and `vector.Result[T]` returns hits whose similarity score is always "higher is better".

| Engine | Underlying Syntax | Metric Binding | Metadata Filter | Notes |
|--------|-------------------|----------------|-----------------|-------|
| GORM (PostgreSQL) | pgvector `<->` / `<=>` / `<#>` | At query time | whereSelectors channel | Entity field uses `vector.Float32Vector` (`gorm:"type:vector(N)"`), HNSW index creation included |
| MongoDB | Atlas `$vectorSearch` aggregation | Mapping (Search index) | Pre-filter + Builder | Requires Atlas 7.0+ / self-managed 8.0+, Search index create/drop included |
| Elasticsearch 8+/9.x | Top-level `knn` clause + dense_vector | Mapping (similarity) | knn.filter (query DSL) | Hybrid search with query in the same body |
| OpenSearch 2.11+ | `query.knn` + knn_vector | Mapping (space_type) | knn.filter (DSL) | `index.knn=true` enabled on index creation |
| ClickHouse | cosineDistance / L2Distance / dotProduct | At query time | baseWhere + whereArgs | Brute-force distance scan, score recomputed in Go |
| Doris 3.0+ | cosine_distance / l2_distance / inner_product | At query time | baseWhere + whereArgs | Accelerated by vector index when enabled |
| Qdrant | Query API (nearest neighbors) | Fixed at collection creation | qdrant.Filter (payload filtering, backed by payload indexes) | tenant_id integer payload index; tenant condition injected into Filter; client-side tenant check on direct-ID paths |
| Milvus 2.4+ | Search (AUTOINDEX ANN) | Fixed at index creation | Milvus expression pre-filter | tenant_id column marked as partition key; tenant predicate injected into expressions; client-side tenant check on direct-PK paths |
| Weaviate 1.27+ | GraphQL nearVector | Fixed at collection creation (vectorIndexConfig.distance) | Where-condition pre-filter | lowercase-first property names enforced (GraphQL naming convention); tenant condition injected into Where; client-side tenant check on direct-UUID paths |

```go
// One request shape across engines
q := &vector.Query{
    Field:  "embedding",
    Vector: embedding,        // []float32
    TopK:   10,
    Metric: vector.MetricCosine,
}

// ES / OpenSearch (client methods)
res, err := client.KnnSearch(ctx, "docs", q)
// GORM / MongoDB / ClickHouse / Doris (repository methods; filters via the where/builder channel)
res, err := repo.SearchByVector(ctx, baseWhereOrBuilder, q)
// Qdrant / Milvus / Weaviate (repository methods; filters via q.Filter, the engine-native condition channel)
res, err := repo.SearchByVector(ctx, q)
```

Unified conventions:
- **Score semantics**: `Score` is always "higher is more similar"; each engine converts its native distance/score accordingly (see per-module docs);
- **TopK semantics**: vector search returns TopK nearest neighbors rather than pages; `Total` is the number of hits;
- **Tenant isolation**: repository-level search reuses each module's tenant row-level enforcement;
- InfluxDB / Ent / Cassandra / Neo4j do not provide vector search yet.

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
