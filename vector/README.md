# vector

跨引擎统一的**向量检索契约**（RAG / 语义检索场景）。

本模块只承载类型与纯函数，**不依赖任何数据库驱动**，供 go-crud 各引擎模块按需引入，把同一份 `Query` 映射到各自的底层语法：

| 引擎 | 底层语法 | 模块方法 |
|------|----------|----------|
| Elasticsearch 8+/9.x | 顶层 `knn` 子句 + `dense_vector` | `KnnSearch` / `SearchWithKnn` / `CreateVectorIndex` |
| OpenSearch 2.11+ | `query.knn` + `knn_vector` | `KnnSearch` / `SearchWithKnn` / `CreateVectorIndex` |
| GORM (PostgreSQL) | pgvector `<->` / `<=>` / `<#>` | `Repository.SearchByVector` / `Client.CreateVectorIndex` |
| MongoDB Atlas 7.0+ | `$vectorSearch` 聚合阶段 | `Repository.SearchByVector` / `Client.CreateVectorSearchIndex` |
| ClickHouse | cosineDistance / L2Distance / dotProduct | `Repository.SearchByVector` |
| Apache Doris 3.0+ | cosine_distance / l2_distance / inner_product | `Repository.SearchByVector` |

## 核心类型

- **`Query`** —— 统一的向量检索请求：向量字段、查询向量、TopK、距离度量、引擎原生过滤条件等（字段语义与各引擎的生效范围见代码注释）；
- **`Result[T]` / `Hit[T]`** —— 统一的检索结果，`Score` 恒为「越大越相似」；
- **`DistanceMetric`** —— 距离度量枚举（cosine / euclidean / dot）；
- **`Float32Vector`** —— 以 pgvector 文本格式（`[1,2,3]`）编解码的向量列类型，实现 `driver.Valuer` / `sql.Scanner`，可直接用作 GORM 实体字段（`gorm:"type:vector(N)"`）；
- **`Distance` / `DistanceToScore`** —— 纯函数距离计算与分数换算，供各引擎在无法从 SQL 直接取回距离列时重算分数。

## 统一约定

1. **分数语义**：`Hit.Score` 恒为「越大越相似」。各引擎把原生距离/分数换算到该语义：
   - cosine 距离 `d = 1 - cos` → `score = 1 - d`；
   - L2 距离 → `score = 1 / (1 + d)`；
   - 内积 → 分数即内积本身（内积是相似度，排序时按降序取近邻）。
2. **TopK 语义**：向量检索是 TopK 近邻而非分页，`Result.Total` 即命中条数。
3. **度量时机**：ES / OpenSearch 的度量由 mapping 固定（查询期不可换）；pgvector / ClickHouse / Doris 在查询期选择算子；MongoDB Atlas 由 Search 索引定义。
4. **embedding 生成不属于本库**：数据访问层只收发已算好的向量，模型调用留给业务侧。

## 快速开始

```go
import "github.com/tx7do/go-crud/vector"

q := &vector.Query{
    Field:  "embedding",
    Vector: embedding,            // []float32，维度须与索引/列定义一致
    TopK:   10,
    Metric: vector.MetricCosine,
}
if err := q.Validate(); err != nil { /* ... */ }

// 各引擎检索方法见上表，均返回 *vector.Result[T]
```
