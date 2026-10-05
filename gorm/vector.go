package gorm

import (
	"context"
	"errors"
	"fmt"

	"gorm.io/gorm"

	"github.com/tx7do/go-wind/log"

	"github.com/tx7do/go-crud/vector"
)

// ─────────────────────────────────────────────────────────────────────────────
// 向量检索（RAG / 语义检索）— 基于 pgvector，仅 PostgreSQL。
//
// 使用方式：
//  1. 数据库执行 CREATE EXTENSION IF NOT EXISTS vector;
//  2. 实体向量字段使用 vector.Float32Vector 并声明列类型：
//	     Embedding vector.Float32Vector `gorm:"type:vector(768)"`
//  3. AutoMigrate 建表后调用 CreateVectorIndex 建立 HNSW 索引（可选但推荐）；
//  4. Repository.SearchByVector 检索近邻。
//
// 度量与操作符映射（查询期选择，pgvector 支持按查询换算子）：
//   - cosine（默认）    <=>  余弦距离 d = 1 - cos，score = 1 - d
//   - euclidean         <->  欧氏距离，score = 1 / (1 + d)
//   - dot               <#>  负内积，score = -d（即内积本身）
//
// Query.Filter / Query.Index / Query.NumCandidates / Query.MinScore /
// Query.MaxDistance 在本实现中不生效：过滤走模块统一的 whereSelectors 通道，
// 其余参数是 ES/OS/Mongo 等引擎的专属概念。
// ─────────────────────────────────────────────────────────────────────────────

// pgvectorOperators 把统一度量枚举映射到 pgvector 距离操作符。
var pgvectorOperators = map[vector.DistanceMetric]string{
	vector.MetricCosine:     "<=>",
	vector.MetricEuclidean:  "<->",
	vector.MetricDotProduct: "<#>",
}

// pgvectorOpclasses 把统一度量枚举映射到 HNSW 索引的操作符类。
var pgvectorOpclasses = map[vector.DistanceMetric]string{
	vector.MetricCosine:     "vector_cosine_ops",
	vector.MetricEuclidean:  "vector_l2_ops",
	vector.MetricDotProduct: "vector_ip_ops",
}

// pgvectorOperator 返回度量对应的距离操作符，未指定度量时默认余弦。
func pgvectorOperator(metric vector.DistanceMetric) (string, error) {
	if metric == "" {
		metric = vector.MetricCosine
	}
	op, ok := pgvectorOperators[metric]
	if !ok {
		return "", fmt.Errorf("unsupported vector metric %q", metric)
	}
	return op, nil
}

// vectorScanRow 用于把「实体全列 + __vs_distance」扫描进内存：
// Entity 以 embedded 方式展开，gorm 按 Entity 自身字段名匹配列。
type vectorScanRow[ENTITY any] struct {
	Distance float64 `gorm:"column:__vs_distance"`
	Entity   ENTITY  `gorm:"embedded"`
}

// SearchByVector 向量近邻检索（pgvector，仅 PostgreSQL）。
//
// whereSelectors 复用模块统一的过滤通道（含租户谓词注入的既有约定：
// 经 client.RegisterTenantCallbacks 注册的行级回调对本查询同样生效）。
// 返回结果按相似度分从高到低排序；Total 即命中的条数（TopK 语义，非分页）。
//
//	示例调用：
//	  res, err := repo.SearchByVector(ctx, db, &vector.Query{
//	      Field: "embedding", Vector: embedding, TopK: 10, Metric: vector.MetricCosine,
//	  }, []func(*gorm.DB) *gorm.DB{func(db *gorm.DB) *gorm.DB {
//	      return db.Where("tenant_id = ?", "t1")
//	  }})
func (r *Repository[DTO, ENTITY]) SearchByVector(
	ctx context.Context,
	db *gorm.DB,
	q *vector.Query,
	whereSelectors []func(*gorm.DB) *gorm.DB,
) (*vector.Result[*DTO], error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}
	if r.mapper == nil {
		return nil, errors.New("mapper is nil")
	}
	if err := q.Validate(); err != nil {
		return nil, fmt.Errorf("invalid vector query: %w", err)
	}

	op, err := pgvectorOperator(q.Metric)
	if err != nil {
		return nil, err
	}
	// 分数换算与操作符使用同一份度量语义（空度量按余弦）
	metric := q.Metric
	if metric == "" {
		metric = vector.MetricCosine
	}
	column := db.NamingStrategy.ColumnName("", q.Field)

	// 非 DryRun 会话下强校验方言，避免 pgvector 语法在其它数据库上产生难懂的报错；
	// DryRun（生成 SQL 预览/测试）不受限。
	if !db.DryRun && db.Dialector.Name() != "postgres" {
		return nil, fmt.Errorf("vector search requires postgres with pgvector, got dialect %q", db.Dialector.Name())
	}

	tx := db.WithContext(ctx).Model(new(ENTITY))
	for _, s := range whereSelectors {
		if s != nil {
			tx = s(tx)
		}
	}

	// 附加距离列：*, "embedding" <=> ? AS __vs_distance，向量经 Float32Vector
	// 的 driver.Valuer 以 "[1,2,3]" 文本绑定，PostgreSQL 按操作符签名推断为 vector。
	tx = tx.Select(
		fmt.Sprintf("*, %q %s ? AS __vs_distance", column, op),
		vector.Float32Vector(q.Vector),
	)
	tx = tx.Order("__vs_distance ASC")
	tx = tx.Limit(q.TopK)

	rows := make([]vectorScanRow[ENTITY], 0, q.TopK)
	if err := tx.Find(&rows).Error; err != nil {
		log.Error(context.Background(), fmt.Sprintf("vector search failed: %s", err.Error()))
		return nil, errors.New("vector search failed")
	}

	hits := make([]vector.Hit[*DTO], 0, len(rows))
	for i := range rows {
		hits = append(hits, vector.Hit[*DTO]{
			// __vs_distance 为 pgvector 原生距离，换算成「越大越相似」的统一分数
			Score: vector.DistanceToScore(metric, rows[i].Distance),
			Value: r.mapper.ToDTO(&rows[i].Entity),
		})
	}

	return &vector.Result[*DTO]{
		Hits:  hits,
		Total: int64(len(hits)),
	}, nil
}

// CreateVectorIndex 为实体的向量列建立 pgvector HNSW 索引（仅 PostgreSQL）。
// 需已开启 CREATE EXTENSION vector，且列已存在（如 AutoMigrate 生成的 vector(N) 列）。
func (c *Client) CreateVectorIndex(tableName, column string, metric vector.DistanceMetric) error {
	opclass, ok := pgvectorOpclasses[metric]
	if !ok {
		return fmt.Errorf("unsupported vector metric %q", metric)
	}
	if c.DB == nil {
		return errors.New("db is nil")
	}
	if c.Dialector.Name() != "postgres" {
		return fmt.Errorf("vector index requires postgres with pgvector, got dialect %q", c.Dialector.Name())
	}

	indexName := fmt.Sprintf("idx_%s_%s_hnsw", tableName, column)
	stmt := fmt.Sprintf(
		`CREATE INDEX IF NOT EXISTS %s ON %s USING hnsw (%s %s)`,
		quotePgIdentifier(indexName),
		quotePgIdentifier(tableName),
		quotePgIdentifier(column),
		opclass,
	)
	if err := c.Exec(stmt).Error; err != nil {
		return fmt.Errorf("create vector index failed: %w", err)
	}
	return nil
}

// quotePgIdentifier 以双引号包裹 PostgreSQL 标识符并转义内嵌双引号。
func quotePgIdentifier(name string) string {
	escaped := ""
	for _, r := range name {
		if r == '"' {
			escaped += `""`
		} else {
			escaped += string(r)
		}
	}
	return `"` + escaped + `"`
}
