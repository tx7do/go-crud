package filter

import (
	"regexp"
	"strings"

	"github.com/go-kratos/kratos/v2/encoding"
	_ "github.com/go-kratos/kratos/v2/encoding/json"

	"github.com/tx7do/go-crud/mongodb/query"
	"github.com/tx7do/go-utils/stringcase"

	pagination "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	bsonV2 "go.mongodb.org/mongo-driver/v2/bson"
)

var jsonKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_.]+$`)

// Processor 用于基于 *query.Builder 构建 MongoDB 风格的 filter
type Processor struct {
	codec encoding.Codec
}

func NewProcessor() *Processor {
	return &Processor{
		codec: encoding.GetCodec("json"),
	}
}

// Process 根据 operator 在 builder 上追加对应的 filter 并返回 builder。
// field 为字段路径（可能包含点），value 为单值，values 为额外的分割值列表（如 IN）。
func (poc Processor) Process(builder *query.Builder, op pagination.Operator, field string, value any, values []any) *query.Builder {
	if builder == nil {
		return nil
	}
	switch op {
	case pagination.Operator_EQ:
		return poc.Equal(builder, field, value)
	case pagination.Operator_NEQ:
		return poc.NotEqual(builder, field, value)
	case pagination.Operator_IN:
		return poc.In(builder, field, value, values)
	case pagination.Operator_NIN:
		return poc.NotIn(builder, field, value, values)
	case pagination.Operator_GTE:
		return poc.GTE(builder, field, value)
	case pagination.Operator_GT:
		return poc.GT(builder, field, value)
	case pagination.Operator_LTE:
		return poc.LTE(builder, field, value)
	case pagination.Operator_LT:
		return poc.LT(builder, field, value)
	case pagination.Operator_BETWEEN:
		return poc.Range(builder, field, value, values)
	case pagination.Operator_IS_NULL:
		return poc.IsNull(builder, field)
	case pagination.Operator_IS_NOT_NULL:
		return poc.IsNotNull(builder, field)
	case pagination.Operator_CONTAINS:
		return poc.Contains(builder, field, value)
	case pagination.Operator_ICONTAINS:
		return poc.InsensitiveContains(builder, field, value)
	case pagination.Operator_STARTS_WITH:
		return poc.StartsWith(builder, field, value)
	case pagination.Operator_ISTARTS_WITH:
		return poc.InsensitiveStartsWith(builder, field, value)
	case pagination.Operator_ENDS_WITH:
		return poc.EndsWith(builder, field, value)
	case pagination.Operator_IENDS_WITH:
		return poc.InsensitiveEndsWith(builder, field, value)
	case pagination.Operator_EXACT:
		return poc.Exact(builder, field, value)
	case pagination.Operator_IEXACT:
		return poc.InsensitiveExact(builder, field, value)
	case pagination.Operator_REGEXP:
		return poc.Regex(builder, field, value)
	case pagination.Operator_IREGEXP:
		return poc.InsensitiveRegex(builder, field, value)
	case pagination.Operator_SEARCH:
		return poc.Search(builder, field, value)
	default:
		return builder
	}
}

// helper: 构造 MongoDB 字段键（支持点路径），并校验 jsonKey 合法性。
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

// appendFilter 将构建好的条件设置到 builder。
// 这里使用 builder.SetFilter，假设 query.Builder 会处理多次调用时的合并逻辑（如内部使用 $and）。
func (poc Processor) appendFilter(builder *query.Builder, cond bsonV2.M) *query.Builder {
	if cond == nil || len(cond) == 0 {
		return builder
	}
	builder.SetFilter(cond)
	return builder
}

// Equal 等于
func (poc Processor) Equal(builder *query.Builder, field string, value any) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}
	return poc.appendFilter(builder, bsonV2.M{key: value})
}

// NotEqual 不等于
func (poc Processor) NotEqual(builder *query.Builder, field string, value any) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}
	return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$ne": value}})
}

// In 包含
func (poc Processor) In(builder *query.Builder, field string, value any, values []any) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}

	// helper: 永假条件
	falseExpr := bsonV2.M{"$expr": bsonV2.A{bsonV2.M{"$eq": bsonV2.A{1, 0}}}}

	// try string
	if s, ok := value.(string); ok {
		s = strings.TrimSpace(s)
		if s == "" {
			// empty string -> fallthrough to values
			goto tryValues
		}
		// try json array
		var arr []interface{}
		if err := poc.codec.Unmarshal([]byte(s), &arr); err == nil {
			if len(arr) == 0 {
				return poc.appendFilter(builder, falseExpr)
			}
			return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$in": arr}})
		}
		// comma separated
		if strings.Contains(s, ",") {
			parts := strings.Split(s, ",")
			args := make([]interface{}, 0, len(parts))
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p != "" {
					args = append(args, p)
				}
			}
			if len(args) == 0 {
				return poc.appendFilter(builder, falseExpr)
			}
			return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$in": args}})
		}
		// single string value
		return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$in": []interface{}{s}}})
	}

	// try []byte
	if b, ok := value.([]byte); ok {
		if len(b) == 0 {
			goto tryValues
		}
		var arr []interface{}
		if err := poc.codec.Unmarshal(b, &arr); err == nil {
			if len(arr) == 0 {
				return poc.appendFilter(builder, falseExpr)
			}
			return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$in": arr}})
		}
		// try comma-separated from bytes
		s := strings.TrimSpace(string(b))
		if s == "" {
			goto tryValues
		}
		if strings.Contains(s, ",") {
			parts := strings.Split(s, ",")
			args := make([]interface{}, 0, len(parts))
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p != "" {
					args = append(args, p)
				}
			}
			if len(args) == 0 {
				return poc.appendFilter(builder, falseExpr)
			}
			return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$in": args}})
		}
		return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$in": []interface{}{s}}})
	}

	// try []interface{}
	if arrI, ok := value.([]interface{}); ok {
		if len(arrI) == 0 {
			return poc.appendFilter(builder, falseExpr)
		}
		return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$in": arrI}})
	}

tryValues:
	// fallback to values param when provided
	if len(values) > 0 {
		args := make([]interface{}, 0, len(values))
		for _, v := range values {
			args = append(args, v)
		}
		if len(args) == 0 {
			return poc.appendFilter(builder, falseExpr)
		}
		return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$in": args}})
	}

	// last fallback: single non-nil value
	if value != nil {
		return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$in": []interface{}{value}}})
	}

	return builder
}

// NotIn 不包含
func (poc Processor) NotIn(builder *query.Builder, field string, value any, values []any) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}

	// try string
	if s, ok := value.(string); ok {
		s = strings.TrimSpace(s)
		if s == "" {
			goto tryValues
		}
		var arr []interface{}
		if err := poc.codec.Unmarshal([]byte(s), &arr); err == nil {
			if len(arr) == 0 {
				return builder
			}
			return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$nin": arr}})
		}
		if strings.Contains(s, ",") {
			parts := strings.Split(s, ",")
			args := make([]interface{}, 0, len(parts))
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p != "" {
					args = append(args, p)
				}
			}
			if len(args) == 0 {
				return builder
			}
			return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$nin": args}})
		}
		return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$nin": []interface{}{s}}})
	}

	// try []byte
	if b, ok := value.([]byte); ok {
		if len(b) == 0 {
			goto tryValues
		}
		var arr []interface{}
		if err := poc.codec.Unmarshal(b, &arr); err == nil {
			if len(arr) == 0 {
				return builder
			}
			return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$nin": arr}})
		}
		s := strings.TrimSpace(string(b))
		if s == "" {
			goto tryValues
		}
		if strings.Contains(s, ",") {
			parts := strings.Split(s, ",")
			args := make([]interface{}, 0, len(parts))
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p != "" {
					args = append(args, p)
				}
			}
			if len(args) == 0 {
				return builder
			}
			return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$nin": args}})
		}
		return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$nin": []interface{}{s}}})
	}

	// try []interface{}
	if arrI, ok := value.([]interface{}); ok {
		if len(arrI) == 0 {
			return builder
		}
		return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$nin": arrI}})
	}

tryValues:
	// fallback to values param when provided
	if len(values) > 0 {
		args := make([]interface{}, 0, len(values))
		for _, v := range values {
			args = append(args, v)
		}
		if len(args) == 0 {
			return builder
		}
		return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$nin": args}})
	}

	// last fallback: single non-nil value
	if value != nil {
		return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$nin": []interface{}{value}}})
	}

	return builder
}

// GTE 大于等于
func (poc Processor) GTE(builder *query.Builder, field string, value any) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}
	return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$gte": value}})
}

// GT 大于
func (poc Processor) GT(builder *query.Builder, field string, value any) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}
	return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$gt": value}})
}

// LTE 小于等于
func (poc Processor) LTE(builder *query.Builder, field string, value any) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}
	return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$lte": value}})
}

// LT 小于
func (poc Processor) LT(builder *query.Builder, field string, value any) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}
	return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$lt": value}})
}

// Range BETWEEN 范围查询
func (poc Processor) Range(builder *query.Builder, field string, value any, values []any) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}

	// try string
	if s, ok := value.(string); ok {
		s = strings.TrimSpace(s)
		if s == "" {
			goto tryValues
		}
		// try JSON array
		var arr []interface{}
		if err := poc.codec.Unmarshal([]byte(s), &arr); err == nil {
			if len(arr) == 2 {
				return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$gte": arr[0], "$lte": arr[1]}})
			}
		}
		// comma separated
		if strings.Contains(s, ",") {
			parts := strings.SplitN(s, ",", 2)
			if len(parts) == 2 {
				a := strings.TrimSpace(parts[0])
				b := strings.TrimSpace(parts[1])
				return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$gte": a, "$lte": b}})
			}
		}
		// fallback to equality when single
		return poc.appendFilter(builder, bsonV2.M{key: s})
	}

	// try []byte
	if b, ok := value.([]byte); ok {
		if len(b) == 0 {
			goto tryValues
		}
		var arr []interface{}
		if err := poc.codec.Unmarshal(b, &arr); err == nil {
			if len(arr) == 2 {
				return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$gte": arr[0], "$lte": arr[1]}})
			}
		}
		s := strings.TrimSpace(string(b))
		if s == "" {
			goto tryValues
		}
		if strings.Contains(s, ",") {
			parts := strings.SplitN(s, ",", 2)
			if len(parts) == 2 {
				a := strings.TrimSpace(parts[0])
				b := strings.TrimSpace(parts[1])
				return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$gte": a, "$lte": b}})
			}
		}
		return poc.appendFilter(builder, bsonV2.M{key: s})
	}

	// try []interface{}
	if arrI, ok := value.([]interface{}); ok {
		if len(arrI) == 2 {
			return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$gte": arrI[0], "$lte": arrI[1]}})
		}
		if len(arrI) == 1 {
			return poc.appendFilter(builder, bsonV2.M{key: arrI[0]})
		}
		// empty -> fallthrough to try values
	}

tryValues:
	// fallback to values param when provided (expecting two items for range)
	if len(values) == 2 {
		return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$gte": values[0], "$lte": values[1]}})
	}
	if len(values) == 1 {
		return poc.appendFilter(builder, bsonV2.M{key: values[0]})
	}

	// last fallback: single non-nil value -> equality
	if value != nil {
		return poc.appendFilter(builder, bsonV2.M{key: value})
	}
	return builder
}

// IsNull 检查是否为 NULL（匹配 null）
func (poc Processor) IsNull(builder *query.Builder, field string) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}
	return poc.appendFilter(builder, bsonV2.M{key: nil})
}

// IsNotNull 检查是否不为 NULL
func (poc Processor) IsNotNull(builder *query.Builder, field string) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}
	return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$ne": nil}})
}

// Contains (LIKE %val%) 使用 regex 匹配
func (poc Processor) Contains(builder *query.Builder, field string, value any) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}

	var s string
	switch v := value.(type) {
	case string:
		s = strings.TrimSpace(v)
	case []byte:
		s = strings.TrimSpace(string(v))
	case interface{ String() string }:
		s = strings.TrimSpace(v.String())
	default:
		// 不支持的类型则不追加条件
		return builder
	}

	if s == "" {
		return builder
	}

	// 转义用户输入并做模糊匹配
	pat := regexp.QuoteMeta(s)
	pat = ".*" + pat + ".*"
	return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$regex": pat}})
}

// InsensitiveContains 不区分大小写
func (poc Processor) InsensitiveContains(builder *query.Builder, field string, value any) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}

	var s string
	switch v := value.(type) {
	case string:
		s = strings.TrimSpace(v)
	case []byte:
		s = strings.TrimSpace(string(v))
	case interface{ String() string }:
		s = strings.TrimSpace(v.String())
	default:
		return builder
	}

	if s == "" {
		return builder
	}

	pat := regexp.QuoteMeta(s)
	pat = ".*" + pat + ".*"
	return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$regex": pat, "$options": "i"}})
}

// StartsWith 开始于
func (poc Processor) StartsWith(builder *query.Builder, field string, value any) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}

	var s string
	switch v := value.(type) {
	case string:
		s = strings.TrimSpace(v)
	case []byte:
		s = strings.TrimSpace(string(v))
	case interface{ String() string }:
		s = strings.TrimSpace(v.String())
	default:
		return builder
	}

	if s == "" {
		return builder
	}

	pat := "^" + regexp.QuoteMeta(s)
	return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$regex": pat}})
}

// InsensitiveStartsWith 不区分大小写
func (poc Processor) InsensitiveStartsWith(builder *query.Builder, field string, value any) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}

	var s string
	switch v := value.(type) {
	case string:
		s = strings.TrimSpace(v)
	case []byte:
		s = strings.TrimSpace(string(v))
	case interface{ String() string }:
		s = strings.TrimSpace(v.String())
	default:
		return builder
	}

	if s == "" {
		return builder
	}

	pat := "^" + regexp.QuoteMeta(s)
	return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$regex": pat, "$options": "i"}})
}

// EndsWith 结束于
func (poc Processor) EndsWith(builder *query.Builder, field string, value any) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}

	var s string
	switch v := value.(type) {
	case string:
		s = strings.TrimSpace(v)
	case []byte:
		s = strings.TrimSpace(string(v))
	case interface{ String() string }:
		s = strings.TrimSpace(v.String())
	default:
		return builder
	}

	if s == "" {
		return builder
	}

	pat := regexp.QuoteMeta(s) + "$"
	return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$regex": pat}})
}

// InsensitiveEndsWith 不区分大小写
func (poc Processor) InsensitiveEndsWith(builder *query.Builder, field string, value any) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}

	var s string
	switch v := value.(type) {
	case string:
		s = strings.TrimSpace(v)
	case []byte:
		s = strings.TrimSpace(string(v))
	case interface{ String() string }:
		s = strings.TrimSpace(v.String())
	default:
		return builder
	}

	if s == "" {
		return builder
	}

	pat := regexp.QuoteMeta(s) + "$"
	return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$regex": pat, "$options": "i"}})
}

// Exact 等值比较
func (poc Processor) Exact(builder *query.Builder, field string, value any) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}

	// 对常见字符串类型做 trim 并忽略空串
	switch v := value.(type) {
	case string:
		s := strings.TrimSpace(v)
		if s == "" {
			return builder
		}
		return poc.appendFilter(builder, bsonV2.M{key: s})
	case []byte:
		s := strings.TrimSpace(string(v))
		if s == "" {
			return builder
		}
		return poc.appendFilter(builder, bsonV2.M{key: s})
	case interface{ String() string }:
		s := strings.TrimSpace(v.String())
		if s == "" {
			return builder
		}
		return poc.appendFilter(builder, bsonV2.M{key: s})
	default:
		if value == nil {
			return builder
		}
		return poc.appendFilter(builder, bsonV2.M{key: value})
	}
}

// InsensitiveExact 不区分大小写的等值比较（使用 regex ^val$ + i）
func (poc Processor) InsensitiveExact(builder *query.Builder, field string, value any) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}

	var s string
	switch v := value.(type) {
	case string:
		s = strings.TrimSpace(v)
	case []byte:
		s = strings.TrimSpace(string(v))
	case interface{ String() string }:
		s = strings.TrimSpace(v.String())
	default:
		return builder
	}

	if s == "" {
		return builder
	}

	pat := "^" + regexp.QuoteMeta(s) + "$"
	return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$regex": pat, "$options": "i"}})
}

// Regex 直接使用用户提供的正则
func (poc Processor) Regex(builder *query.Builder, field string, value any) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}

	var s string
	switch v := value.(type) {
	case string:
		s = strings.TrimSpace(v)
	case []byte:
		s = strings.TrimSpace(string(v))
	case interface{ String() string }:
		s = strings.TrimSpace(v.String())
	default:
		return builder
	}

	if s == "" {
		return builder
	}
	return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$regex": s}})
}

// InsensitiveRegex 不区分大小写的正则
func (poc Processor) InsensitiveRegex(builder *query.Builder, field string, value any) *query.Builder {
	key := poc.makeKey(field)
	if key == "" {
		return builder
	}

	var s string
	switch v := value.(type) {
	case string:
		s = strings.TrimSpace(v)
	case []byte:
		s = strings.TrimSpace(string(v))
	case interface{ String() string }:
		s = strings.TrimSpace(v.String())
	default:
		return builder
	}

	if s == "" {
		return builder
	}
	return poc.appendFilter(builder, bsonV2.M{key: bsonV2.M{"$regex": s, "$options": "i"}})
}

// Search 简单全文搜索，fallback 为 contains（Regex %val%）
func (poc Processor) Search(builder *query.Builder, field string, value any) *query.Builder {
	var s string
	switch v := value.(type) {
	case string:
		s = strings.TrimSpace(v)
	case []byte:
		s = strings.TrimSpace(string(v))
	case interface{ String() string }:
		s = strings.TrimSpace(v.String())
	default:
		return builder
	}

	if s == "" {
		return builder
	}
	return poc.Contains(builder, field, value)
}

// DatePartField 和 JsonbField 不适用于 MongoDB，此处保留空实现以兼容调用（可按需实现）。
func (poc Processor) DatePartField(datePart, field string) string { return "" }
func (poc Processor) JsonbField(jsonbField, field string) string  { return "" }
