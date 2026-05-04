package filter

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"gorm.io/gorm"

	"github.com/go-kratos/kratos/v2/encoding"
	_ "github.com/go-kratos/kratos/v2/encoding/json"

	"github.com/tx7do/go-utils/stringcase"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

var jsonKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_\.]+$`)

// Processor 过滤处理器（GORM 版）
type Processor struct {
	codec encoding.Codec
}

// NewProcessor 返回带 json codec 的 Processor
func NewProcessor() *Processor {
	return &Processor{
		codec: encoding.GetCodec("json"),
	}
}

// Process 将给定操作映射为对 *gorm.DB 的修改并返回修改后的 *gorm.DB
func (poc Processor) Process(db *gorm.DB, op paginationV1.Operator, field, value string, values []string) *gorm.DB {
	if db == nil {
		return db
	}
	// 将 field 转为 snake_case（与 DB 列风格一致）
	field = stringcase.ToSnakeCase(field)

	switch op {
	case paginationV1.Operator_EQ:
		return poc.Equal(db, field, value)
	case paginationV1.Operator_NEQ:
		return poc.NotEqual(db, field, value)
	case paginationV1.Operator_IN:
		return poc.In(db, field, value, values)
	case paginationV1.Operator_NIN:
		return poc.NotIn(db, field, value, values)
	case paginationV1.Operator_GTE:
		return poc.GTE(db, field, value)
	case paginationV1.Operator_GT:
		return poc.GT(db, field, value)
	case paginationV1.Operator_LTE:
		return poc.LTE(db, field, value)
	case paginationV1.Operator_LT:
		return poc.LT(db, field, value)
	case paginationV1.Operator_BETWEEN:
		return poc.Range(db, field, value, values)
	case paginationV1.Operator_IS_NULL:
		return poc.IsNull(db, field)
	case paginationV1.Operator_IS_NOT_NULL:
		return poc.IsNotNull(db, field)
	case paginationV1.Operator_CONTAINS:
		return poc.Contains(db, field, value)
	case paginationV1.Operator_ICONTAINS:
		return poc.InsensitiveContains(db, field, value)
	case paginationV1.Operator_STARTS_WITH:
		return poc.StartsWith(db, field, value)
	case paginationV1.Operator_ISTARTS_WITH:
		return poc.InsensitiveStartsWith(db, field, value)
	case paginationV1.Operator_ENDS_WITH:
		return poc.EndsWith(db, field, value)
	case paginationV1.Operator_IENDS_WITH:
		return poc.InsensitiveEndsWith(db, field, value)
	case paginationV1.Operator_EXACT:
		return poc.Exact(db, field, value)
	case paginationV1.Operator_IEXACT:
		return poc.InsensitiveExact(db, field, value)
	case paginationV1.Operator_REGEXP:
		return poc.Regex(db, field, value)
	case paginationV1.Operator_IREGEXP:
		return poc.InsensitiveRegex(db, field, value)
	case paginationV1.Operator_SEARCH:
		return poc.Search(db, field, value)
	default:
		return db
	}
}

// --- 基本比较 ---

// Equal 相等比较，空值不添加条件（与 ent 的 EmptyBehavior 类似）
func (poc Processor) Equal(db *gorm.DB, field, value string) *gorm.DB {
	if strings.TrimSpace(value) == "" {
		return db
	}
	return db.Where(fmt.Sprintf("%s = ?", field), value)
}

// NotEqual 不相等比较，空值不添加条件（与 ent 的 EmptyBehavior 类似）
func (poc Processor) NotEqual(db *gorm.DB, field, value string) *gorm.DB {
	if strings.TrimSpace(value) == "" {
		return db
	}
	// 使用 NOT (field = ?) 以保留 NULL 行为一致性
	return db.Not(fmt.Sprintf("%s = ?", field), value)
}

// GTE 大于等于比较，空值不添加条件（与 ent 的 EmptyBehavior 类似）
func (poc Processor) GTE(db *gorm.DB, field, value string) *gorm.DB {
	if strings.TrimSpace(value) == "" {
		return db
	}
	return db.Where(fmt.Sprintf("%s >= ?", field), value)
}

// GT 大于比较，空值不添加条件（与 ent 的 EmptyBehavior 类似）
func (poc Processor) GT(db *gorm.DB, field, value string) *gorm.DB {
	if strings.TrimSpace(value) == "" {
		return db
	}
	return db.Where(fmt.Sprintf("%s > ?", field), value)
}

// LTE 小于等于比较，空值不添加条件（与 ent 的 EmptyBehavior 类似）
func (poc Processor) LTE(db *gorm.DB, field, value string) *gorm.DB {
	if strings.TrimSpace(value) == "" {
		return db
	}
	return db.Where(fmt.Sprintf("%s <= ?", field), value)
}

// LT 小于比较，空值不添加条件（与 ent 的 EmptyBehavior 类似）
func (poc Processor) LT(db *gorm.DB, field, value string) *gorm.DB {
	if strings.TrimSpace(value) == "" {
		return db
	}
	return db.Where(fmt.Sprintf("%s < ?", field), value)
}

// --- IN / NOT IN ---

// In 支持两种输入方式：1) value 作为 JSON 数组字符串；2) values 作为多个单值输入。空值不添加条件。
func (poc Processor) In(db *gorm.DB, field, value string, values []string) *gorm.DB {
	if len(value) > 0 {
		var jsonValues []any
		if err := poc.codec.Unmarshal([]byte(value), &jsonValues); err == nil {
			return db.Where(fmt.Sprintf("%s IN ?", field), jsonValues)
		}
	} else if len(values) > 0 {
		var anyVals []any
		for _, v := range values {
			anyVals = append(anyVals, v)
		}
		return db.Where(fmt.Sprintf("%s IN ?", field), anyVals)
	}
	return db
}

// NotIn 支持两种输入方式：1) value 作为 JSON 数组字符串；2) values 作为多个单值输入。空值不添加条件。
func (poc Processor) NotIn(db *gorm.DB, field, value string, values []string) *gorm.DB {
	if len(value) > 0 {
		var jsonValues []any
		if err := poc.codec.Unmarshal([]byte(value), &jsonValues); err == nil {
			return db.Not(fmt.Sprintf("%s IN ?", field), jsonValues)
		}
	} else if len(values) > 0 {
		var anyVals []any
		for _, v := range values {
			anyVals = append(anyVals, v)
		}
		return db.Not(fmt.Sprintf("%s IN ?", field), anyVals)
	}
	return db
}

// --- Range / Between ---

// Range 支持两种输入方式：1) value 作为 JSON 数组字符串，且必须包含两个元素；2) values 作为两个单值输入。空值不添加条件。
func (poc Processor) Range(db *gorm.DB, field, value string, values []string) *gorm.DB {
	if len(value) > 0 {
		var jsonValues []any
		if err := poc.codec.Unmarshal([]byte(value), &jsonValues); err == nil {
			if len(jsonValues) != 2 {
				return db
			}
			return db.Where(fmt.Sprintf("%s >= ? AND %s <= ?", field, field), jsonValues[0], jsonValues[1])
		}
	} else if len(values) == 2 {
		return db.Where(fmt.Sprintf("%s >= ? AND %s <= ?", field, field), values[0], values[1])
	}
	return db
}

// --- NULL ---

// IsNull IS NULL 判断
func (poc Processor) IsNull(db *gorm.DB, field string) *gorm.DB {
	return db.Where(fmt.Sprintf("%s IS NULL", field))
}

// IsNotNull IS NOT NULL 判断
func (poc Processor) IsNotNull(db *gorm.DB, field string) *gorm.DB {
	return db.Where(fmt.Sprintf("%s IS NOT NULL", field))
}

// --- 字符串 / 模糊匹配 ---

// Contains 使用 LIKE '%value%' 进行包含匹配，空值不添加条件（与 ent 的 EmptyBehavior 类似）
func (poc Processor) Contains(db *gorm.DB, field, value string) *gorm.DB {
	if strings.TrimSpace(value) == "" {
		return db
	}
	return db.Where(fmt.Sprintf("%s LIKE ?", field), "%"+value+"%")
}

// InsensitiveContains 使用 ILIKE（PostgreSQL）或 LOWER + LIKE（其他）进行大小写不敏感的包含匹配，空值不添加条件（与 ent 的 EmptyBehavior 类似）
func (poc Processor) InsensitiveContains(db *gorm.DB, field, value string) *gorm.DB {
	if strings.TrimSpace(value) == "" {
		return db
	}
	switch strings.ToLower(db.Dialector.Name()) {
	case "postgres":
		return db.Where(fmt.Sprintf("%s ILIKE ?", field), "%"+value+"%")
	default:
		return db.Where(fmt.Sprintf("LOWER(%s) LIKE ?", field), "%"+strings.ToLower(value)+"%")
	}
}

// StartsWith 使用 LIKE 'value%' 进行前缀匹配，空值不添加条件（与 ent 的 EmptyBehavior 类似）
func (poc Processor) StartsWith(db *gorm.DB, field, value string) *gorm.DB {
	if strings.TrimSpace(value) == "" {
		return db
	}
	return db.Where(fmt.Sprintf("%s LIKE ?", field), value+"%")
}

// InsensitiveStartsWith 使用 ILIKE（PostgreSQL）或 LOWER + LIKE（其他）进行大小写不敏感的前缀匹配，空值不添加条件（与 ent 的 EmptyBehavior 类似）
func (poc Processor) InsensitiveStartsWith(db *gorm.DB, field, value string) *gorm.DB {
	if strings.TrimSpace(value) == "" {
		return db
	}
	switch strings.ToLower(db.Dialector.Name()) {
	case "postgres":
		return db.Where(fmt.Sprintf("%s ILIKE ?", field), value+"%")
	default:
		return db.Where(fmt.Sprintf("LOWER(%s) LIKE ?", field), strings.ToLower(value)+"%")
	}
}

// EndsWith 使用 LIKE '%value' 进行后缀匹配，空值不添加条件（与 ent 的 EmptyBehavior 类似）
func (poc Processor) EndsWith(db *gorm.DB, field, value string) *gorm.DB {
	if strings.TrimSpace(value) == "" {
		return db
	}
	return db.Where(fmt.Sprintf("%s LIKE ?", field), "%"+value)
}

// InsensitiveEndsWith 使用 ILIKE（PostgreSQL）或 LOWER + LIKE（其他）进行大小写不敏感的后缀匹配，空值不添加条件（与 ent 的 EmptyBehavior 类似）
func (poc Processor) InsensitiveEndsWith(db *gorm.DB, field, value string) *gorm.DB {
	if strings.TrimSpace(value) == "" {
		return db
	}
	switch strings.ToLower(db.Dialector.Name()) {
	case "postgres":
		return db.Where(fmt.Sprintf("%s ILIKE ?", field), "%"+value)
	default:
		return db.Where(fmt.Sprintf("LOWER(%s) LIKE ?", field), "%"+strings.ToLower(value))
	}
}

// Exact 使用等于比较，空值不添加条件（与 ent 的 EmptyBehavior 类似）
func (poc Processor) Exact(db *gorm.DB, field, value string) *gorm.DB {
	if strings.TrimSpace(value) == "" {
		return db
	}
	// Exact 使用等于比较（与 ent 的 LIKE 精确不同）
	return db.Where(fmt.Sprintf("%s = ?", field), value)
}

// InsensitiveExact 使用 ILIKE（PostgreSQL）或 LOWER + LIKE（其他）进行大小写不敏感的精确匹配，空值不添加条件（与 ent 的 EmptyBehavior 类似）
func (poc Processor) InsensitiveExact(db *gorm.DB, field, value string) *gorm.DB {
	if strings.TrimSpace(value) == "" {
		return db
	}
	switch strings.ToLower(db.Dialector.Name()) {
	case "postgres":
		return db.Where(fmt.Sprintf("%s ILIKE ?", field), value)
	default:
		return db.Where(fmt.Sprintf("LOWER(%s) = ?", field), strings.ToLower(value))
	}
}

// --- 正则 ---

// Regex 根据不同数据库使用正则表达式进行匹配，空值不添加条件（与 ent 的 EmptyBehavior 类似）
func (poc Processor) Regex(db *gorm.DB, field, value string) *gorm.DB {
	if strings.TrimSpace(value) == "" {
		return db
	}
	switch strings.ToLower(db.Dialector.Name()) {
	case "postgres":
		return db.Where(fmt.Sprintf("%s ~ ?", field), value)
	case "mysql":
		// BINARY 强制大小写敏感
		return db.Where(fmt.Sprintf("%s REGEXP BINARY ?", field), value)
	case "sqlite":
		return db.Where(fmt.Sprintf("%s REGEXP ?", field), value)
	default:
		return db
	}
}

// InsensitiveRegex 根据不同数据库使用正则表达式进行大小写不敏感的匹配，空值不添加条件（与 ent 的 EmptyBehavior 类似）
func (poc Processor) InsensitiveRegex(db *gorm.DB, field, value string) *gorm.DB {
	if strings.TrimSpace(value) == "" {
		return db
	}
	switch strings.ToLower(db.Dialector.Name()) {
	case "postgres":
		return db.Where(fmt.Sprintf("%s ~* ?", field), value)
	case "mysql":
		return db.Where(fmt.Sprintf("%s REGEXP ?", field), value)
	case "sqlite":
		// SQLite 可以在 pattern 前加 (?i) 来忽略大小写
		if !strings.HasPrefix(value, "(?i)") {
			value = "(?i)" + value
		}
		return db.Where(fmt.Sprintf("%s REGEXP ?", field), value)
	default:
		return db
	}
}

// --- 全文搜索 ---

// Search 根据不同数据库实现全文搜索
func (poc Processor) Search(db *gorm.DB, field, value string) *gorm.DB {
	if strings.TrimSpace(value) == "" {
		return db
	}
	switch strings.ToLower(db.Dialector.Name()) {
	case "postgres":
		return db.Where(fmt.Sprintf("to_tsvector(%s) @@ plainto_tsquery(?)", field), value)
	case "mysql":
		return db.Where(fmt.Sprintf("MATCH(%s) AGAINST(? IN NATURAL LANGUAGE MODE)", field), value)
	case "sqlite":
		return db.Where(fmt.Sprintf("%s LIKE ?", field), "%"+value+"%")
	default:
		return db.Where(fmt.Sprintf("%s LIKE ?", field), "%"+value+"%")
	}
}

// --- DatePart ---

// DatePart 根据指定的 date part 对字段进行过滤（仅检查非 NULL）
func (poc Processor) DatePart(db *gorm.DB, datePart, field string) *gorm.DB {
	if !IsValidDatePartString(datePart) {
		return db
	}
	part := strings.ToUpper(datePart)
	switch strings.ToLower(db.Dialector.Name()) {
	case "postgres":
		return db.Where(fmt.Sprintf("EXTRACT('%s' FROM %s) IS NOT NULL", part, field))
	case "mysql":
		return db.Where(fmt.Sprintf("%s(%s) IS NOT NULL", part, field))
	default:
		return db.Where(fmt.Sprintf("EXTRACT('%s' FROM %s) IS NOT NULL", part, field))
	}
}

// IsValidDatePartString 简单验证 date part 来源（与 paginator 共用逻辑可替换）
func IsValidDatePartString(s string) bool {
	if s == "" {
		return false
	}
	// 允许字母与下划线
	return regexp.MustCompile(`^[A-Za-z_]+$`).MatchString(s)
}

// --- JSONB 相关 ---

// Jsonb 在 WHERE 子句中直接引用 JSON 字段的子键
func (poc Processor) Jsonb(db *gorm.DB, jsonbField, field string) *gorm.DB {
	jsonbField = strings.TrimSpace(jsonbField)
	if jsonbField == "" {
		return db
	}
	if !jsonKeyPattern.MatchString(jsonbField) {
		return db
	}
	switch strings.ToLower(db.Dialector.Name()) {
	case "postgres":
		// column ->> 'key'
		return db.Where(fmt.Sprintf("%s ->> '%s' IS NOT NULL", field, jsonbField))
	case "mysql":
		// JSON_EXTRACT(column, '$.key')
		return db.Where(fmt.Sprintf("JSON_EXTRACT(%s, '$.%s') IS NOT NULL", field, jsonbField))
	default:
		return db.Where(fmt.Sprintf("%s ->> '%s' IS NOT NULL", field, jsonbField))
	}
}

// JsonbFieldExpr 返回一个表达式字符串与对应参数，可用于 Select/Order 等
func (poc Processor) JsonbFieldExpr(db *gorm.DB, jsonbField, field string) (string, []interface{}) {
	jsonbField = strings.TrimSpace(jsonbField)
	if jsonbField == "" || !jsonKeyPattern.MatchString(jsonbField) {
		return "", nil
	}
	switch strings.ToLower(db.Dialector.Name()) {
	case "postgres":
		return fmt.Sprintf("%s ->> '%s'", field, jsonbField), nil
	case "mysql":
		return fmt.Sprintf("JSON_EXTRACT(%s, '$.%s')", field, jsonbField), nil
	default:
		return fmt.Sprintf("%s ->> '%s'", field, jsonbField), nil
	}
}

// JsonbField 返回 JSONB 子字段在 SQL 中的字符串表示（可直接用于 Select）
func (poc Processor) JsonbField(db *gorm.DB, jsonbField, field string) string {
	expr, _ := poc.JsonbFieldExpr(db, jsonbField, field)
	return expr
}

// parseJSONValues 解析 JSON 字符串为 slice(any)
func (poc Processor) parseJSONValues(raw string) ([]any, error) {
	var arr []any
	if err := poc.codec.Unmarshal([]byte(raw), &arr); err != nil {
		// fallback to standard json.Unmarshal
		if err2 := json.Unmarshal([]byte(raw), &arr); err2 != nil {
			return nil, err
		}
	}
	return arr, nil
}
