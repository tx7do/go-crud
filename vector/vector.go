// Package vector 定义跨引擎统一的向量检索契约（RAG / 语义检索场景）。
//
// 本包只承载类型与纯函数，不依赖任何数据库驱动，供各引擎模块
// （elasticsearch / opensearch / gorm / mongodb / clickhouse / doris）
// 按需引入并映射到各自的底层语法：
//
//	Elasticsearch 8+/9.x    顶层 knn 子句（dense_vector，度量在 mapping 固定）
//	OpenSearch 2.x          query.knn（knn_vector，度量在 mapping 固定）
//	PostgreSQL (GORM)       pgvector 操作符 <-> / <=> / <#>
//	MongoDB Atlas 7.0+      $vectorSearch 聚合阶段
//	ClickHouse              cosineDistance / L2Distance / dotProduct（暴力检索）
//	Apache Doris 3.0+       cosine_distance / l2_distance / inner_product
//
// 统一约定：
//   - Score 恒为「相似度分」，越大越相似；各引擎把原生距离/分数换算到该语义，
//     换算规则见各引擎实现处的注释。
//   - Metric 仅在「建表 / 建 mapping / 建索引」以及支持查询期选择度量的引擎
//     （pgvector、ClickHouse、Doris）中生效；ES/OpenSearch 的度量由 mapping 决定，
//     查询期传入会被忽略。
package vector

import (
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// TopKDefaultNumCandidatesRatio 在 Query.NumCandidates 未指定（0）时，
// 候选数取 TopK 的倍数，保证召回率的同时避免全量扫描。
const TopKDefaultNumCandidatesRatio = 10

// DistanceMetric 向量距离度量。
type DistanceMetric string

const (
	// MetricCosine 余弦相似度（最常用，推荐默认值）。
	MetricCosine DistanceMetric = "cosine"

	// MetricEuclidean 欧氏距离（L2）。
	MetricEuclidean DistanceMetric = "euclidean"

	// MetricDotProduct 内积（点积）。向量未归一化时注意分数量纲不统一。
	MetricDotProduct DistanceMetric = "dot"
)

// Query 跨引擎统一的向量检索请求。
type Query struct {
	// Field 向量字段名（必填）。
	Field string

	// Vector 查询向量（必填，维度须与索引/列定义一致）。
	Vector []float32

	// TopK 返回的近邻条数（必填，>0）。
	TopK int

	// NumCandidates ANN 候选数（HNSW 类索引）。
	// 0 表示由各引擎取默认值（TopK × TopKDefaultNumCandidatesRatio）。
	// 仅 ES / OpenSearch / MongoDB Atlas 等暴露该参数的引擎生效。
	NumCandidates int

	// Index 命名向量索引（如 MongoDB Atlas $vectorSearch 的 search index 名）。
	// ES/OpenSearch 的索引即 collection 本身，无需此字段，忽略。
	Index string

	// Metric 距离度量。建表/建索引时必填；查询期仅 pgvector / ClickHouse / Doris 生效。
	Metric DistanceMetric

	// Filter 引擎原生的元数据过滤条件（可选），用于 pre-filter：
	//   - Elasticsearch：query DSL（map[string]any）或 DSL 数组；
	//   - OpenSearch：query DSL（map[string]any）；
	//   - MongoDB：过滤文档（bson.M）；
	//   - GORM / ClickHouse / Doris：按各模块既有约定另行传参，忽略此字段。
	Filter any

	// MinScore 相似度分下限（可选，0 表示不过滤）。
	// 仅支持分数阈值的引擎生效（如 ES knn.similarity、OpenSearch 顶层 min_score）。
	MinScore float64

	// MaxDistance 距离上限（可选，0 表示不过滤）。
	// 仅支持距离阈值的引擎生效。
	MaxDistance float64
}

// Validate 校验请求的必填项与取值范围。
func (q *Query) Validate() error {
	if q == nil {
		return errors.New("vector query is nil")
	}
	if q.Field == "" {
		return errors.New("vector query field is empty")
	}
	if len(q.Vector) == 0 {
		return errors.New("vector query vector is empty")
	}
	for i, v := range q.Vector {
		if math.IsNaN(float64(v)) || math.IsInf(float64(v), 0) {
			return fmt.Errorf("vector query vector[%d] is not a finite number", i)
		}
	}
	if q.TopK <= 0 {
		return fmt.Errorf("vector query topK must be positive, got %d", q.TopK)
	}
	if q.NumCandidates < 0 {
		return fmt.Errorf("vector query numCandidates must not be negative, got %d", q.NumCandidates)
	}
	return nil
}

// EffectiveNumCandidates 返回生效的候选数：
// 未指定时按 TopK × TopKDefaultNumCandidatesRatio 放大。
func (q *Query) EffectiveNumCandidates() int {
	if q.NumCandidates > 0 {
		return q.NumCandidates
	}
	return q.TopK * TopKDefaultNumCandidatesRatio
}

// Hit 单条向量检索命中。
type Hit[T any] struct {
	// Score 相似度分，越大越相似（各引擎换算规则见实现处注释）。
	Score float64

	// Value 命中的文档/实体。
	Value T
}

// Result 向量检索结果。
//
// 向量检索是 TopK 语义而非分页语义：Total 即本次命中的条数
// （引擎不支持返回「命中总数」，详见各引擎实现处的注释）。
type Result[T any] struct {
	Hits  []Hit[T]
	Total int64
}

// Float32Vector 以 pgvector 文本格式编解码的 float32 向量列类型，
// 可直接用作 GORM 实体字段（配合 `gorm:"type:vector(N)"` 标签），
// 也兼容任何以 "[1,2,3]" 文本表示向量的驱动。
type Float32Vector []float32

// String 以 pgvector 文本格式输出：[1,2,3]。
func (v Float32Vector) String() string {
	return FormatVectorLiteral(v)
}

// Value 实现 driver.Valuer，以 pgvector 文本格式落库。
func (v Float32Vector) Value() (driver.Value, error) {
	return v.String(), nil
}

// Scan 实现 sql.Scanner，支持 string / []byte 两种来源，
// 兼容 pgvector 的 "[1,2,3]" 文本格式。
func (v *Float32Vector) Scan(src any) error {
	switch val := src.(type) {
	case nil:
		*v = nil
		return nil
	case []byte:
		return v.parse(string(val))
	case string:
		return v.parse(val)
	default:
		return fmt.Errorf("vector: unsupported scan source %T", src)
	}
}

func (v *Float32Vector) parse(s string) error {
	parsed, err := ParseVector(s)
	if err != nil {
		return err
	}
	*v = parsed
	return nil
}

// ParseVector 解析 pgvector 文本格式（"[1,2,3]"，容忍空格与方括号缺失）。
func ParseVector(s string) (Float32Vector, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	s = strings.TrimPrefix(s, "[")
	s = strings.TrimSuffix(s, "]")
	s = strings.TrimSpace(s)
	if s == "" {
		return Float32Vector{}, nil
	}
	parts := strings.Split(s, ",")
	out := make(Float32Vector, 0, len(parts))
	for i, p := range parts {
		p = strings.TrimSpace(p)
		f, err := strconv.ParseFloat(p, 32)
		if err != nil {
			return nil, fmt.Errorf("vector: parse element %d (%q): %w", i, p, err)
		}
		out = append(out, float32(f))
	}
	return out, nil
}

// FormatVectorLiteral 以 pgvector 文本格式输出向量字面量：[1,2,3]。
func FormatVectorLiteral(vec []float32) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, f := range vec {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(f), 'g', -1, 32))
	}
	b.WriteByte(']')
	return b.String()
}

// DistanceToScore 把引擎原生距离换算为统一语义的相似度分（越大越相似）。
//
//	metric=MetricCosine    ：距离 d ∈ [0,2]（1 - cos），score = 1 - d
//	metric=MetricEuclidean ：距离 d ≥ 0，score = 1 / (1 + d)
//	metric=MetricDotProduct：距离为「负内积」，score = -d（即内积本身）
//
// 未知度量按欧氏距离处理。
func DistanceToScore(metric DistanceMetric, distance float64) float64 {
	switch metric {
	case MetricCosine:
		return 1 - distance
	case MetricDotProduct:
		return -distance
	default:
		return 1 / (1 + distance)
	}
}

// Distance 在 Go 侧计算两个向量的距离/内积（与 ClickHouse、Doris 的同名
// SQL 函数语义一致，供无法从 SQL 直接取回距离列的引擎在内存中重算分数）。
//
//	metric=MetricCosine    ：返回 1 - cos（零向量视为与任何向量正交，返回 1）
//	metric=MetricEuclidean ：返回 L2 距离
//	metric=MetricDotProduct：返回内积本身（内积是相似度而非距离，
//	                         ORDER BY 时应按降序取近邻，分数不做换算）
//
// 未指定度量按余弦处理；维度不一致时报错。
func Distance(metric DistanceMetric, a, b []float32) (float64, error) {
	if len(a) != len(b) {
		return 0, fmt.Errorf("vector dimension mismatch: %d vs %d", len(a), len(b))
	}
	if metric == "" {
		metric = MetricCosine
	}
	switch metric {
	case MetricCosine, MetricDotProduct:
		var dot, normA, normB float64
		for i := range a {
			dot += float64(a[i]) * float64(b[i])
			normA += float64(a[i]) * float64(a[i])
			normB += float64(b[i]) * float64(b[i])
		}
		if metric == MetricDotProduct {
			return dot, nil
		}
		if normA == 0 || normB == 0 {
			return 1, nil
		}
		return 1 - dot/(math.Sqrt(normA)*math.Sqrt(normB)), nil
	case MetricEuclidean:
		var sum float64
		for i := range a {
			d := float64(a[i]) - float64(b[i])
			sum += d * d
		}
		return math.Sqrt(sum), nil
	default:
		return 0, fmt.Errorf("unsupported vector metric %q", metric)
	}
}
