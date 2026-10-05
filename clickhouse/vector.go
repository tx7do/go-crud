package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/tx7do/go-wind/log"

	"github.com/tx7do/go-crud/vector"
)

// ─────────────────────────────────────────────────────────────────────────────
// 向量检索（RAG / 语义检索）— 基于 ClickHouse 数组距离函数的暴力检索。
//
// ClickHouse 原生无通用 ANN 索引（实验性的 vector_similarity 索引除外），
// 本实现用 cosineDistance / L2Distance / dotProduct 排序 + LIMIT 完成近邻检索，
// 适合中小规模向量集；大规模场景建议配合物理化的专用向量引擎。
//
//   - Query.Filter 在本实现中不生效：过滤条件按模块既有约定经 baseWhere +
//     whereArgs 传入（含租户谓词注入 InjectTenantFilterIntoBaseWhere）；
//   - Query.Metric 查询期选择距离函数（未指定按 cosine）；
//   - Query.NumCandidates / Query.Index / Query.MinScore / Query.MaxDistance
//     无对应参数，忽略；
//   - 相似度分在 Go 侧按命中实体携带的向量重算（避免为取分数而多输出一列，
//     兼容驱动的 ScanStruct 全列映射要求），语义与 vector.DistanceToScore 一致：
//     cosine：score = 1 - cosineDistance；euclidean：score = 1/(1+L2Distance)；
//     dot：score = dotProduct 本身（内积即相似度，降序取近邻）。
// ─────────────────────────────────────────────────────────────────────────────

// clickhouseDistanceFuncs 把统一度量枚举映射到 ClickHouse 距离函数。
var clickhouseDistanceFuncs = map[vector.DistanceMetric]string{
	vector.MetricCosine:     "cosineDistance",
	vector.MetricEuclidean:  "L2Distance",
	vector.MetricDotProduct: "dotProduct",
}

// clickhouseDistanceFunc 返回度量对应的距离函数，未指定度量时默认余弦。
func clickhouseDistanceFunc(metric vector.DistanceMetric) (string, error) {
	if metric == "" {
		metric = vector.MetricCosine
	}
	fn, ok := clickhouseDistanceFuncs[metric]
	if !ok {
		return "", fmt.Errorf("unsupported vector metric %q", metric)
	}
	return fn, nil
}

// buildVectorSearchSQL 构造向量近邻检索 SQL（纯函数，便于离线测试）。
// 向量字面量为数值数组（[0.1,0.2,0.3]），不含注入面。
func buildVectorSearchSQL(table, baseWhere string, q *vector.Query) (string, error) {
	if err := q.Validate(); err != nil {
		return "", fmt.Errorf("invalid vector query: %w", err)
	}
	fn, err := clickhouseDistanceFunc(q.Metric)
	if err != nil {
		return "", err
	}
	if table == "" {
		return "", errors.New("table is empty")
	}

	sql := "SELECT * FROM " + table
	bw := strings.TrimSpace(baseWhere)
	if bw != "" {
		// 与 Count 约定一致：不包含 WHERE 前缀时自动添加
		if !strings.HasPrefix(strings.ToUpper(bw), "WHERE") {
			sql += " WHERE " + bw
		} else {
			sql += " " + bw
		}
	}

	// 内积是相似度而非距离：降序取近邻；距离类升序。
	orderDir := "ASC"
	if q.Metric == vector.MetricDotProduct {
		orderDir = "DESC"
	}
	sql += fmt.Sprintf(" ORDER BY %s(`%s`, %s) %s LIMIT %d",
		fn, q.Field, vector.FormatVectorLiteral(q.Vector), orderDir, q.TopK)
	return sql, nil
}

// goDistance 在 Go 侧重算与 ClickHouse 同语义的距离/内积值。
// 实现已下沉到共享 vector.Distance（各引擎一致），此处保留薄包装以便测试与调用点语义清晰。
func goDistance(metric vector.DistanceMetric, a, b []float32) (float64, error) {
	return vector.Distance(metric, a, b)
}

// entityVectorField 从实体中按列名取出向量字段（兼容 `ch` 标签、精确与
// 忽略大小写的字段名匹配，规则与驱动的 ScanStruct 列映射一致）。
func entityVectorField[ENTITY any](entity *ENTITY, field string) ([]float32, error) {
	if entity == nil {
		return nil, errors.New("entity is nil")
	}
	rv := reflect.ValueOf(*entity)
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		name := f.Tag.Get("ch")
		if name == "" {
			name = f.Name
		}
		if name != field && !strings.EqualFold(name, field) {
			continue
		}
		fv := rv.Field(i)
		if fv.Kind() != reflect.Slice {
			return nil, fmt.Errorf("entity vector field %q is %s, expected slice", field, fv.Kind())
		}
		out := make([]float32, fv.Len())
		for j := 0; j < fv.Len(); j++ {
			out[j] = float32(fv.Index(j).Float())
		}
		return out, nil
	}
	return nil, fmt.Errorf("entity vector field %q not found in %T", field, entity)
}

// SearchByVector 向量近邻检索（暴力距离函数 + LIMIT）。
//
// whereArgs 复用模块统一的过滤通道（含既有约定：租户行级强制
// InjectTenantFilterIntoBaseWhere 对本查询同样生效）。
// 返回结果按相似度分排序（恒为「越大越相似」）；Total 即命中的条数
// （TopK 语义，非分页）。
//
//	示例调用：
//	  res, err := repo.SearchByVector(ctx, "tenant_id = ?", &vector.Query{
//	      Field: "embedding", Vector: embedding, TopK: 10, Metric: vector.MetricCosine,
//	  }, "t1")
func (r *Repository[DTO, ENTITY]) SearchByVector(
	ctx context.Context,
	baseWhere string,
	q *vector.Query,
	whereArgs ...any,
) (*vector.Result[*DTO], error) {
	if r.client == nil {
		return nil, errors.New("clickhouse client is nil")
	}
	if r.table == "" {
		return nil, errors.New("table is empty")
	}
	if r.mapper == nil {
		return nil, errors.New("mapper is nil")
	}
	if err := q.Validate(); err != nil {
		return nil, fmt.Errorf("invalid vector query: %w", err)
	}

	// 展开单个切片参数为独立参数（与 Count 约定一致）
	if len(whereArgs) == 1 {
		v := reflect.ValueOf(whereArgs[0])
		if v.IsValid() && v.Kind() == reflect.Slice {
			expanded := make([]any, v.Len())
			for i := 0; i < v.Len(); i++ {
				expanded[i] = v.Index(i).Interface()
			}
			whereArgs = expanded
		}
	}

	// 租户行级强制：tenant-scoped 实体追加 tenant_id 谓词。
	var err error
	baseWhere, whereArgs, err = InjectTenantFilterIntoBaseWhere[ENTITY](ctx, baseWhere, whereArgs)
	if err != nil {
		return nil, err
	}

	sql, err := buildVectorSearchSQL(r.table, baseWhere, q)
	if err != nil {
		return nil, err
	}

	var rawResults []any
	creator := func() any {
		var e ENTITY
		return &e
	}
	if err = r.client.Query(ctx, creator, &rawResults, sql, whereArgs...); err != nil {
		log.Error(context.Background(), fmt.Sprintf("vector search query failed: %v", err))
		return nil, errors.New("vector search failed")
	}

	hits := make([]vector.Hit[*DTO], 0, len(rawResults))
	// 分数换算与 ORDER BY 使用同一份度量语义（空度量按余弦）
	metric := q.Metric
	if metric == "" {
		metric = vector.MetricCosine
	}
	for _, res := range rawResults {
		entity, ok := res.(*ENTITY)
		if !ok {
			continue
		}
		score := 0.0
		if rowVec, vecErr := entityVectorField(entity, q.Field); vecErr == nil {
			if metric == vector.MetricDotProduct {
				// 内积本身即相似度
				score, _ = goDistance(vector.MetricDotProduct, rowVec, q.Vector)
			} else {
				distance, distErr := goDistance(metric, rowVec, q.Vector)
				if distErr == nil {
					score = vector.DistanceToScore(metric, distance)
				}
			}
		}
		hits = append(hits, vector.Hit[*DTO]{
			Score: score,
			Value: r.mapper.ToDTO(entity),
		})
	}

	return &vector.Result[*DTO]{
		Hits:  hits,
		Total: int64(len(hits)),
	}, nil
}
