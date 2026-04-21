package query

import (
	"fmt"
	"strings"
)

// Builder 用于构建 OpenSearch 查询 DSL
// 支持 must/should/filter、分页、排序、字段过滤等
// 用法与原MongoDB风格兼容
type Builder struct {
	must    []map[string]any
	should  []map[string]any
	filter  []map[string]any
	mustNot []map[string]any

	sort    []map[string]any
	from    int
	size    int
	_source []string
}

func NewQueryBuilder() *Builder {
	return &Builder{
		must:    make([]map[string]any, 0),
		should:  make([]map[string]any, 0),
		filter:  make([]map[string]any, 0),
		mustNot: make([]map[string]any, 0),
		sort:    make([]map[string]any, 0),
		from:    0,
		size:    10,
		_source: nil,
	}
}

// Where 添加must条件
func (b *Builder) Where(cond map[string]any) *Builder {
	b.must = append(b.must, cond)
	return b
}

// Should 添加should条件
func (b *Builder) Should(cond map[string]any) *Builder {
	b.should = append(b.should, cond)
	return b
}

// Filter 添加filter条件
func (b *Builder) Filter(cond map[string]any) *Builder {
	b.filter = append(b.filter, cond)
	return b
}

// MustNot 添加must_not条件
func (b *Builder) MustNot(cond map[string]any) *Builder {
	b.mustNot = append(b.mustNot, cond)
	return b
}

// SetSort 设置排序条件
func (b *Builder) SetSort(field string, asc bool) *Builder {
	order := "desc"
	if asc {
		order = "asc"
	}
	b.sort = append(b.sort, map[string]any{field: map[string]any{"order": order}})
	return b
}

// SetPage 设置分页参数（page从1开始，size为每页数量）
func (b *Builder) SetPage(page, size int) *Builder {
	if page < 1 {
		page = 1
	}
	if size <= 0 {
		size = 10
	}
	b.from = (page - 1) * size
	b.size = size
	return b
}

// SetFromSize 直接设置from/size
func (b *Builder) SetFromSize(from, size int) *Builder {
	b.from = from
	b.size = size
	return b
}

// SetSource 设置返回字段
func (b *Builder) SetSource(fields ...string) *Builder {
	b._source = fields
	return b
}

// SetRange 添加范围查询
func (b *Builder) SetRange(field string, gte, lte any) *Builder {
	rangeCond := map[string]any{
		"range": map[string]any{
			field: map[string]any{},
		},
	}
	if gte != nil {
		rangeCond["range"].(map[string]any)[field].(map[string]any)["gte"] = gte
	}
	if lte != nil {
		rangeCond["range"].(map[string]any)[field].(map[string]any)["lte"] = lte
	}
	b.filter = append(b.filter, rangeCond)
	return b
}

// SetIn 添加in查询
func (b *Builder) SetIn(field string, values []any) *Builder {
	inCond := map[string]any{
		"terms": map[string]any{field: values},
	}
	b.filter = append(b.filter, inCond)
	return b
}

// SetExists 添加exists查询
func (b *Builder) SetExists(field string) *Builder {
	existsCond := map[string]any{
		"exists": map[string]any{"field": field},
	}
	b.filter = append(b.filter, existsCond)
	return b
}

// Build 生成最终的OpenSearch查询DSL
func (b *Builder) Build() map[string]any {
	query := map[string]any{
		"bool": map[string]any{},
	}
	boolQuery := query["bool"].(map[string]any)
	if len(b.must) > 0 {
		boolQuery["must"] = b.must
	}
	if len(b.should) > 0 {
		boolQuery["should"] = b.should
	}
	if len(b.filter) > 0 {
		boolQuery["filter"] = b.filter
	}
	if len(b.mustNot) > 0 {
		boolQuery["must_not"] = b.mustNot
	}

	dsl := map[string]any{
		"query": query,
		"from":  b.from,
		"size":  b.size,
	}
	if len(b.sort) > 0 {
		dsl["sort"] = b.sort
	}
	if b._source != nil && len(b._source) > 0 {
		dsl["_source"] = b._source
	}
	return dsl
}

// BuildSQL 生成SQL语句（无参数、无fields、值直接写入SQL）
// 自动使用 _source 作为查询字段，无则用 *
func (b *Builder) BuildSQL(table string) string {
	if table == "" {
		return ""
	}

	// 1. 处理 SELECT 字段（自动从 _source 获取）
	selectFields := "*"
	if len(b._source) > 0 {
		selectFields = strings.Join(b._source, ", ")
	}

	// 2. 拼接 WHERE 条件
	var whereParts []string

	// 处理 must + filter（与条件）
	for _, conds := range [][]map[string]any{b.must, b.filter} {
		for _, cond := range conds {
			for condType, v := range cond {
				switch condType {
				case "term": // 等值查询
					for field, val := range v.(map[string]any) {
						whereParts = append(whereParts, field+" = "+escapeValue(val))
					}
				case "terms": // IN 查询
					for field, vals := range v.(map[string]any) {
						vs, ok := vals.([]any)
						if !ok {
							continue
						}
						var phs []string
						for _, val := range vs {
							phs = append(phs, escapeValue(val))
						}
						whereParts = append(whereParts, field+" IN ("+strings.Join(phs, ", ")+")")
					}
				case "range": // 范围查询
					for field, rng := range v.(map[string]any) {
						r := rng.(map[string]any)
						if gte, ok := r["gte"]; ok {
							whereParts = append(whereParts, field+" >= "+escapeValue(gte))
						}
						if lte, ok := r["lte"]; ok {
							whereParts = append(whereParts, field+" <= "+escapeValue(lte))
						}
						if gt, ok := r["gt"]; ok {
							whereParts = append(whereParts, field+" > "+escapeValue(gt))
						}
						if lt, ok := r["lt"]; ok {
							whereParts = append(whereParts, field+" < "+escapeValue(lt))
						}
					}
				case "exists": // 字段存在
					exists := v.(map[string]any)
					if f, ok := exists["field"].(string); ok && f != "" {
						whereParts = append(whereParts, f+" IS NOT NULL")
					}
				}
			}
		}
	}

	// 拼接 WHERE
	whereSQL := ""
	if len(whereParts) > 0 {
		whereSQL = " WHERE " + strings.Join(whereParts, " AND ")
	}

	// 3. 拼接 ORDER BY
	orderSQL := ""
	if len(b.sort) > 0 {
		var orderParts []string
		for _, s := range b.sort {
			for field, orderObj := range s {
				order := orderObj.(map[string]any)["order"].(string)
				orderParts = append(orderParts, field+" "+strings.ToUpper(order))
			}
		}
		orderSQL = " ORDER BY " + strings.Join(orderParts, ", ")
	}

	// 4. 拼接 LIMIT / OFFSET
	limitSQL := ""
	if b.size > 0 {
		limitSQL = " LIMIT " + escapeValue(b.size)
	}
	offsetSQL := ""
	if b.from > 0 {
		offsetSQL = " OFFSET " + escapeValue(b.from)
	}

	// 最终 SQL
	return "SELECT " + selectFields +
		" FROM " + table +
		whereSQL +
		orderSQL +
		limitSQL +
		offsetSQL
}

// escapeValue 自动给字符串加引号，数字不加，防基础格式错误
func escapeValue(v any) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case string:
		return "'" + strings.ReplaceAll(val, "'", "''") + "'"
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64:
		return fmt.Sprintf("%v", val)
	case bool:
		if val {
			return "TRUE"
		}
		return "FALSE"
	default:
		return "'" + strings.ReplaceAll(fmt.Sprintf("%v", val), "'", "''") + "'"
	}
}
