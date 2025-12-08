package query

import (
	"fmt"
	"sort"
	"strings"
)

// Builder 用于构造 InfluxQL 查询
type Builder struct {
	table     string
	fields    []string
	where     []string
	groupBy   []string
	orderBy   []string
	limit     int
	offset    int
	precision string
}

// NewQueryBuilder 创建新的 QueryBuilder
func NewQueryBuilder(table string) *Builder {
	return &Builder{
		table:  table,
		limit:  -1,
		offset: -1,
	}
}

// Select 指定要查询的字段，传 nil 或 空数组 表示 "*"
func (qb *Builder) Select(fields []string) *Builder {
	if len(fields) == 0 {
		qb.fields = []string{"*"}
	} else {
		qb.fields = fields
	}
	return qb
}

// WhereFromMaps 根据 filters 和 operators 构造 WHERE 子句
// filters: map[field]value
// operators: map[field]operator (operator 支持: =, !=, >, >=, <, <=, in, regex)
func (qb *Builder) WhereFromMaps(filters map[string]interface{}, operators map[string]string) *Builder {
	if len(filters) == 0 {
		return qb
	}

	// deterministic order
	keys := make([]string, 0, len(filters))
	for k := range filters {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		v := filters[k]
		op := strings.ToLower(strings.TrimSpace(operators[k]))
		if op == "" {
			op = "="
		}

		var expr string
		switch op {
		case "=", "eq":
			expr = fmt.Sprintf("%s = %s", k, formatValue(v))
		case "!=", "ne":
			expr = fmt.Sprintf("%s != %s", k, formatValue(v))
		case ">", "gt":
			expr = fmt.Sprintf("%s > %s", k, formatValue(v))
		case ">=", "gte":
			expr = fmt.Sprintf("%s >= %s", k, formatValue(v))
		case "<", "lt":
			expr = fmt.Sprintf("%s < %s", k, formatValue(v))
		case "<=", "lte":
			expr = fmt.Sprintf("%s <= %s", k, formatValue(v))
		case "in":
			// formatValue 对 slice 会返回 "(a,b,c)"
			expr = fmt.Sprintf("%s IN %s", k, formatValue(v))
		case "regex", "re", "=~":
			// 使用正则匹配，确保传入的是字符串或能被格式化为字符串
			// formatRegex wraps value into /.../
			expr = fmt.Sprintf("%s =~ %s", k, formatRegex(v))
		default:
			// fallback to equals
			expr = fmt.Sprintf("%s = %s", k, formatValue(v))
		}
		qb.where = append(qb.where, expr)
	}

	return qb
}

// GroupBy 设置 group by 字段
func (qb *Builder) GroupBy(fields ...string) *Builder {
	qb.groupBy = append(qb.groupBy, fields...)
	return qb
}

// OrderBy 设置排序，desc 为 true 时使用 DESC
func (qb *Builder) OrderBy(field string, desc bool) *Builder {
	if field == "" {
		return qb
	}
	if desc {
		qb.orderBy = append(qb.orderBy, fmt.Sprintf("%s DESC", field))
	} else {
		qb.orderBy = append(qb.orderBy, fmt.Sprintf("%s ASC", field))
	}
	return qb
}

// Limit 设置 limit
func (qb *Builder) Limit(n int) *Builder {
	qb.limit = n
	return qb
}

// Offset 设置 offset
func (qb *Builder) Offset(n int) *Builder {
	qb.offset = n
	return qb
}

// Build 生成最终的 InfluxQL 查询字符串
func (qb *Builder) Build() string {
	// fields
	fields := "*"
	if len(qb.fields) > 0 {
		fields = strings.Join(qb.fields, ", ")
	}

	// from
	sb := strings.Builder{}
	sb.WriteString("SELECT ")
	sb.WriteString(fields)
	sb.WriteString(" FROM ")
	sb.WriteString(qb.table)

	// where
	if len(qb.where) > 0 {
		sb.WriteString(" WHERE ")
		sb.WriteString(strings.Join(qb.where, " AND "))
	}

	// group by
	if len(qb.groupBy) > 0 {
		sb.WriteString(" GROUP BY ")
		sb.WriteString(strings.Join(qb.groupBy, ", "))
	}

	// order by
	if len(qb.orderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		sb.WriteString(strings.Join(qb.orderBy, ", "))
	}

	// limit/offset
	if qb.limit >= 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", qb.limit))
	}
	if qb.offset >= 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", qb.offset))
	}

	return sb.String()
}

// BuildQueryWithParams 兼容现有 client.go 的调用签名
func BuildQueryWithParams(
	table string,
	filters map[string]interface{},
	operators map[string]string,
	fields []string,
) string {
	qb := NewQueryBuilder(table).
		Select(fields).
		WhereFromMaps(filters, operators)
	return qb.Build()
}
