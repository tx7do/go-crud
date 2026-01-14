package filter

import (
	"encoding/json"
	"regexp"
	"strings"

	"github.com/go-kratos/kratos/v2/encoding"
	_ "github.com/go-kratos/kratos/v2/encoding/json"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/influxdb/query"
	"github.com/tx7do/go-utils/stringcase"
)

var jsonKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_.]+$`)

// Processor 用于基于 *query.Builder 构建 InfluxDB 风格的 WHERE 子句
type Processor struct {
	codec encoding.Codec
}

// NewProcessor 返回 InfluxDB 用的 Processor
func NewProcessor() *Processor {
	return &Processor{
		codec: encoding.GetCodec("json"),
	}
}

// Process 根据 operator 在 builder 上追加对应的 filter 并返回 builder。
// field 为字段路径（可能包含点），value 为单值，values 为额外的分割值列表（如 IN）。
func (poc Processor) Process(builder *query.Builder, op paginationV1.Operator, field, value string, values []string) *query.Builder {
	if builder == nil {
		return nil
	}
	switch op {
	case paginationV1.Operator_EQ:
		return poc.Equal(builder, field, value)
	case paginationV1.Operator_NEQ:
		return poc.NotEqual(builder, field, value)
	case paginationV1.Operator_IN:
		return poc.In(builder, field, value, values)
	case paginationV1.Operator_NIN:
		return poc.NotIn(builder, field, value, values)
	case paginationV1.Operator_GTE:
		return poc.GTE(builder, field, value)
	case paginationV1.Operator_GT:
		return poc.GT(builder, field, value)
	case paginationV1.Operator_LTE:
		return poc.LTE(builder, field, value)
	case paginationV1.Operator_LT:
		return poc.LT(builder, field, value)
	case paginationV1.Operator_BETWEEN:
		return poc.Range(builder, field, value, values)
	case paginationV1.Operator_IS_NULL:
		// InfluxQL 的 NULL 支持与实现差异较大，暂不自动生成 IS NULL 表达式
		return builder
	case paginationV1.Operator_IS_NOT_NULL:
		return builder
	case paginationV1.Operator_CONTAINS:
		return poc.Contains(builder, field, value)
	case paginationV1.Operator_ICONTAINS:
		return poc.InsensitiveContains(builder, field, value)
	case paginationV1.Operator_STARTS_WITH:
		return poc.StartsWith(builder, field, value)
	case paginationV1.Operator_ISTARTS_WITH:
		return poc.InsensitiveStartsWith(builder, field, value)
	case paginationV1.Operator_ENDS_WITH:
		return poc.EndsWith(builder, field, value)
	case paginationV1.Operator_IENDS_WITH:
		return poc.InsensitiveEndsWith(builder, field, value)
	case paginationV1.Operator_EXACT:
		return poc.Exact(builder, field, value)
	case paginationV1.Operator_IEXACT:
		return poc.InsensitiveExact(builder, field, value)
	case paginationV1.Operator_REGEXP:
		return poc.Regex(builder, field, value)
	case paginationV1.Operator_IREGEXP:
		return poc.InsensitiveRegex(builder, field, value)
	case paginationV1.Operator_SEARCH:
		return poc.Search(builder, field, value)
	default:
		return builder
	}
}

// makeKey 构造 InfluxDB 字段键（支持点路径），并校验 jsonKey 合法性。
// 返回空字符串表示不可用（避免注入）。
func (poc Processor) makeKey(field string) string {
	field = strings.TrimSpace(field)
	if field == "" {
		return ""
	}
	if strings.Contains(field, ".") {
		parts := strings.Split(field, ".")
		col := stringcase.ToSnakeCase(parts[0])
		jsonKey := strings.Join(parts[1:], ".")
		if !jsonKeyPattern.MatchString(jsonKey) {
			return ""
		}
		return col + "." + jsonKey
	}
	return stringcase.ToSnakeCase(field)
}

// helper: 尝试从 value 或 values 中解析为 slice of interface{}
func (poc Processor) parseArray(value string, values []string) ([]interface{}, bool) {
	// 优先尝试 JSON 数组字符串
	if strings.TrimSpace(value) != "" {
		var arr []interface{}
		if err := poc.codec.Unmarshal([]byte(value), &arr); err == nil {
			return arr, true
		}
		// 其次尝试逗号分割
		if strings.Contains(value, ",") {
			parts := strings.Split(value, ",")
			out := make([]interface{}, 0, len(parts))
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p != "" {
					out = append(out, p)
				}
			}
			return out, true
		}
	}
	// 最后使用传入的 values 切片
	if len(values) > 0 {
		out := make([]interface{}, 0, len(values))
		for _, v := range values {
			out = append(out, v)
		}
		return out, true
	}
	return nil, false
}

// Equal 等于
func (poc Processor) Equal(builder *query.Builder, field, value string) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}
	return builder.WhereFromMaps(map[string]interface{}{key: value}, map[string]string{key: "="})
}

// NotEqual 不等于
func (poc Processor) NotEqual(builder *query.Builder, field, value string) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}
	return builder.WhereFromMaps(map[string]interface{}{key: value}, map[string]string{key: "!="})
}

// In 包含
func (poc Processor) In(builder *query.Builder, field, value string, values []string) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}
	arr, ok := poc.parseArray(value, values)
	if !ok || len(arr) == 0 {
		return builder
	}
	return builder.WhereFromMaps(map[string]interface{}{key: arr}, map[string]string{key: "in"})
}

// NotIn 不包含 —— 通过多个 != 条件构造等价的 AND 表达式
func (poc Processor) NotIn(builder *query.Builder, field, value string, values []string) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}
	arr, ok := poc.parseArray(value, values)
	if !ok || len(arr) == 0 {
		return builder
	}
	for _, v := range arr {
		// 每次追加一个 != 条件，最终会由 Builder 用 AND 连接
		builder = builder.WhereFromMaps(map[string]interface{}{key: v}, map[string]string{key: "!="})
	}
	return builder
}

// GTE 大于等于
func (poc Processor) GTE(builder *query.Builder, field, value string) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}
	return builder.WhereFromMaps(map[string]interface{}{key: value}, map[string]string{key: ">="})
}

// GT 大于
func (poc Processor) GT(builder *query.Builder, field, value string) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}
	return builder.WhereFromMaps(map[string]interface{}{key: value}, map[string]string{key: ">"})
}

// LTE 小于等于
func (poc Processor) LTE(builder *query.Builder, field, value string) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}
	return builder.WhereFromMaps(map[string]interface{}{key: value}, map[string]string{key: "<="})
}

// LT 小于
func (poc Processor) LT(builder *query.Builder, field, value string) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}
	return builder.WhereFromMaps(map[string]interface{}{key: value}, map[string]string{key: "<"})
}

// Range BETWEEN 范围查询 — 尝试解析为两个值
func (poc Processor) Range(builder *query.Builder, field, value string, values []string) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}
	// 先尝试 JSON 数组或逗号分割
	if strings.TrimSpace(value) != "" {
		var arr []interface{}
		if err := json.Unmarshal([]byte(value), &arr); err == nil {
			if len(arr) == 2 {
				builder = builder.WhereFromMaps(map[string]interface{}{key: arr[0]}, map[string]string{key: ">="})
				builder = builder.WhereFromMaps(map[string]interface{}{key: arr[1]}, map[string]string{key: "<="})
				return builder
			}
		}
		if strings.Contains(value, ",") {
			parts := strings.SplitN(value, ",", 2)
			if len(parts) == 2 {
				a := strings.TrimSpace(parts[0])
				b := strings.TrimSpace(parts[1])
				builder = builder.WhereFromMaps(map[string]interface{}{key: a}, map[string]string{key: ">="})
				builder = builder.WhereFromMaps(map[string]interface{}{key: b}, map[string]string{key: "<="})
				return builder
			}
		}
	}
	if len(values) == 2 {
		builder = builder.WhereFromMaps(map[string]interface{}{key: values[0]}, map[string]string{key: ">="})
		builder = builder.WhereFromMaps(map[string]interface{}{key: values[1]}, map[string]string{key: "<="})
		return builder
	}
	// fallback to equality when single
	if strings.TrimSpace(value) != "" {
		return poc.Equal(builder, field, value)
	}
	return builder
}

// Contains (LIKE %val%) 使用正则匹配
func (poc Processor) Contains(builder *query.Builder, field, value string) *query.Builder {
	key := poc.makeKey(field)
	if key == "" || strings.TrimSpace(value) == "" {
		return builder
	}
	pat := ".*" + regexpEscape(value) + ".*"
	return builder.WhereFromMaps(map[string]interface{}{key: pat}, map[string]string{key: "=~"})
}

// InsensitiveContains 不区分大小写
func (poc Processor) InsensitiveContains(builder *query.Builder, field, value string) *query.Builder {
	key := poc.makeKey(field)
	if key == "" || strings.TrimSpace(value) == "" {
		return builder
	}
	pat := "(?i).*" + regexpEscape(value) + ".*"
	return builder.WhereFromMaps(map[string]interface{}{key: pat}, map[string]string{key: "=~"})
}

// StartsWith 开始于
func (poc Processor) StartsWith(builder *query.Builder, field, value string) *query.Builder {
	key := poc.makeKey(field)
	if key == "" || strings.TrimSpace(value) == "" {
		return builder
	}
	pat := "^" + regexpEscape(value) + ".*"
	return builder.WhereFromMaps(map[string]interface{}{key: pat}, map[string]string{key: "=~"})
}

// InsensitiveStartsWith 不区分大小写
func (poc Processor) InsensitiveStartsWith(builder *query.Builder, field, value string) *query.Builder {
	key := poc.makeKey(field)
	if key == "" || strings.TrimSpace(value) == "" {
		return builder
	}
	pat := "(?i)^" + regexpEscape(value) + ".*"
	return builder.WhereFromMaps(map[string]interface{}{key: pat}, map[string]string{key: "=~"})
}

// EndsWith 结束于
func (poc Processor) EndsWith(builder *query.Builder, field, value string) *query.Builder {
	key := poc.makeKey(field)
	if key == "" || strings.TrimSpace(value) == "" {
		return builder
	}
	pat := ".*" + regexpEscape(value) + "$"
	return builder.WhereFromMaps(map[string]interface{}{key: pat}, map[string]string{key: "=~"})
}

// InsensitiveEndsWith 不区分大小写
func (poc Processor) InsensitiveEndsWith(builder *query.Builder, field, value string) *query.Builder {
	key := poc.makeKey(field)
	if key == "" || strings.TrimSpace(value) == "" {
		return builder
	}
	pat := "(?i).*" + regexpEscape(value) + "$"
	return builder.WhereFromMaps(map[string]interface{}{key: pat}, map[string]string{key: "=~"})
}

// Exact 等值比较
func (poc Processor) Exact(builder *query.Builder, field, value string) *query.Builder {
	return poc.Equal(builder, field, value)
}

// InsensitiveExact 不区分大小写的等值比较（使用 regex ^val$ + i）
func (poc Processor) InsensitiveExact(builder *query.Builder, field, value string) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}
	pat := "(?i)^" + regexpEscape(value) + "$"
	return builder.WhereFromMaps(map[string]interface{}{key: pat}, map[string]string{key: "=~"})
}

// Regex 直接使用用户提供的正则
func (poc Processor) Regex(builder *query.Builder, field, value string) *query.Builder {
	key := poc.makeKey(field)
	if key == "" || strings.TrimSpace(value) == "" {
		return builder
	}
	return builder.WhereFromMaps(map[string]interface{}{key: value}, map[string]string{key: "=~"})
}

// InsensitiveRegex 不区分大小写的正则
func (poc Processor) InsensitiveRegex(builder *query.Builder, field, value string) *query.Builder {
	key := poc.makeKey(field)
	if key == "" || strings.TrimSpace(value) == "" {
		return builder
	}
	// 如果用户已包含 (?i) 则不重复添加
	prefix := "(?i)"
	if strings.HasPrefix(value, "(?i)") {
		return builder.WhereFromMaps(map[string]interface{}{key: value}, map[string]string{key: "=~"})
	}
	return builder.WhereFromMaps(map[string]interface{}{key: prefix + value}, map[string]string{key: "=~"})
}

// Search 简单全文搜索，fallback 为 contains（Regex %val%）
func (poc Processor) Search(builder *query.Builder, field, value string) *query.Builder {
	if strings.TrimSpace(value) == "" {
		return builder
	}
	return poc.Contains(builder, field, value)
}

// 简单转义用户输入在正则中的特殊字符（避免构造非法正则或注入）
func regexpEscape(s string) string {
	// 使用 Golang 的 regexp.QuoteMeta 等价实现
	return strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(
		strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(s,
			"\\", `\\`), ".", `\.`), "+", `\+`), "*", `\*`), "?", `\?`), "|", `\|`), "{", `\{`), "}", `\}`), "(", `\(`), ")", `\)`), "^", `\^`)
}
