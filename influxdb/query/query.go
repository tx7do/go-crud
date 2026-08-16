package query

import (
	"fmt"
	"reflect"
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

// Where 接受任意类型的条件并分发处理：
// - string / []string -> WhereFromRaw
// - map[string]any / map[string]any -> WhereFromMaps (operators=nil)
// 其它类型忽略返回原 qb
func (qb *Builder) Where(cond any) *Builder {
	switch v := cond.(type) {
	case string:
		return qb.WhereFromRaw(v)
	case []string:
		return qb.WhereFromRaw(v...)
	case map[string]any:
		// 将 map[string]any 转为 map[string]any
		m := make(map[string]any, len(v))
		for k, val := range v {
			m[k] = val
		}
		return qb.WhereFromMaps(m, nil)
	default:
		return qb
	}
}

// WhereFromMaps 根据 filters 和 operators 构造 WHERE 子句
// filters: map[field]value
// operators: map[field]operator (operator 支持: =, !=, >, >=, <, <=, in, regex)
func (qb *Builder) WhereFromMaps(filters map[string]any, operators map[string]string) *Builder {
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

// WhereFromAny 接受任意类型的 filters 并分发到相应处理：
// - map[string]any / map[string]any -> WhereFromMaps
// - string -> WhereFromRaw
// - struct / *struct -> 提取导出字段 (优先使用 json tag) 并调用 WhereFromMaps
// - 其它 -> 尝试 fmt.Sprintf 转为字符串并当作 raw 条件
func (qb *Builder) WhereFromAny(filters any, operators map[string]string) *Builder {
	if filters == nil {
		return qb
	}

	switch v := filters.(type) {
	case map[string]any:
		m := make(map[string]any, len(v))
		for k, val := range v {
			m[k] = val
		}
		return qb.WhereFromMaps(m, operators)
	case string:
		return qb.WhereFromRaw(v)
	}

	// 支持 struct 或 *struct，通过反射提取导出字段，优先使用 json tag
	rv := reflect.ValueOf(filters)
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return qb
		}
		rv = rv.Elem()
	}
	if rv.IsValid() && rv.Kind() == reflect.Struct {
		rt := rv.Type()
		m := make(map[string]any)
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			// 只处理导出字段
			if f.PkgPath != "" {
				continue
			}
			name := f.Tag.Get("json")
			if name == "" {
				name = f.Name
			} else {
				// tag 可能包含 omitempty 等，取逗号前部分
				name = strings.Split(name, ",")[0]
				if name == "" {
					name = f.Name
				}
			}
			val := rv.Field(i).Interface()
			m[name] = val
		}
		return qb.WhereFromMaps(m, operators)
	}

	// 兜底：格式化为字符串作为 raw 条件
	str := strings.TrimSpace(fmt.Sprintf("%v", filters))
	if str != "" {
		return qb.WhereFromRaw(str)
	}
	return qb
}

// WhereFromAnys 接收混合类型的条件并分发处理：
// - string / []string -> WhereFromRaw
// - []any -> 递归处理每个元素
// - map[string]any / map[string]any -> WhereFromMaps (operators=nil)
// - fmt.Stringer -> 使用 String()
// 其它类型尝试用 fmt.Sprintf("%v") 转为字符串并当作 raw 片段（若非空）
func (qb *Builder) WhereFromAnys(raws ...any) *Builder {
	if len(raws) == 0 {
		return qb
	}
	for _, r := range raws {
		if r == nil {
			continue
		}
		switch v := r.(type) {
		case string:
			qb.WhereFromRaw(v)
		case []string:
			qb.WhereFromRaw(v...)
		case []any:
			// 递归处理任意切片
			for _, item := range v {
				qb.WhereFromAnys(item)
			}
		case map[string]any:
			m := make(map[string]any, len(v))
			for k, val := range v {
				m[k] = val
			}
			qb.WhereFromMaps(m, nil)
		default:
			if s, ok := v.(fmt.Stringer); ok {
				qb.WhereFromRaw(s.String())
				continue
			}
			// fallback: 尝试格式化为字符串
			str := strings.TrimSpace(fmt.Sprintf("%v", v))
			if str != "" {
				qb.WhereFromRaw(str)
			}
		}
	}
	return qb
}

// WhereFromRaw 接受一个或多个原始 WHERE 条件片段，
// 会去掉空白并移除可选的前缀 "WHERE"，然后追加到 qb.where 中。
func (qb *Builder) WhereFromRaw(raws ...string) *Builder {
	if len(raws) == 0 {
		return qb
	}
	for _, r := range raws {
		if r == "" {
			continue
		}
		s := strings.TrimSpace(r)
		if s == "" {
			continue
		}
		// 如果传入以 "WHERE " 开头，去掉该前缀以避免重复
		l := strings.ToLower(s)
		if strings.HasPrefix(l, "where ") {
			s = strings.TrimSpace(s[len("where "):])
		}
		qb.where = append(qb.where, s)
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

// Build 生成最终的 InfluxQL 查询字符串（含分页 LIMIT/OFFSET）
func (qb *Builder) Build() string {
	return qb.build(true)
}

// BuildWithoutPaging 生成不含 LIMIT/OFFSET 的 InfluxQL 查询字符串，
// 用于 COUNT 等聚合场景——带 LIMIT 的查询会把 total 截断到页长。
func (qb *Builder) BuildWithoutPaging() string {
	return qb.build(false)
}

func (qb *Builder) build(withPaging bool) string {
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

	// limit/offset（计数场景不施加分页）
	if withPaging {
		if qb.limit >= 0 {
			sb.WriteString(fmt.Sprintf(" LIMIT %d", qb.limit))
		}
		if qb.offset >= 0 {
			sb.WriteString(fmt.Sprintf(" OFFSET %d", qb.offset))
		}
	}

	return sb.String()
}

// BuildQueryWithParams 兼容现有 client.go 的调用签名
func BuildQueryWithParams(
	table string,
	filters map[string]any,
	operators map[string]string,
	fields []string,
) string {
	qb := NewQueryBuilder(table).
		Select(fields).
		WhereFromMaps(filters, operators)
	return qb.Build()
}
