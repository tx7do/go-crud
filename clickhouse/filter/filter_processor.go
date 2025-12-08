package filter

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/go-kratos/kratos/v2/encoding"
	"github.com/tx7do/go-crud/clickhouse/query"
	"github.com/tx7do/go-crud/paginator"
	"github.com/tx7do/go-utils/stringcase"

	pagination "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

var jsonKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_.]+$`)

// Processor 用于基于 *query.Builder 构建 ClickHouse 风格的 WHERE/ARGS
type Processor struct {
	codec encoding.Codec
}

func NewProcessor() *Processor {
	return &Processor{
		codec: encoding.GetCodec("json"),
	}
}

// Process 根据 operator 在 builder 上追加对应的条件表达式并返回 builder。
// field 为列名或 json 字段（含点分隔），value 为单值，values 为额外的分割值列表（如 IN）。
func (poc Processor) Process(builder *query.Builder, op pagination.Operator, field, value string, values []string) *query.Builder {
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

// helper: 构造列表达式（支持 json 字段）
func (poc Processor) colExpr(field string) string {
	field = strings.TrimSpace(field)
	if field == "" {
		return ""
	}
	if strings.Contains(field, ".") {
		parts := strings.Split(field, ".")
		col := stringcase.ToSnakeCase(parts[0])
		jsonKey := strings.Join(parts[1:], ".")
		// 校验 jsonKey 合法性
		if !jsonKeyPattern.MatchString(jsonKey) {
			return col // 返回列名，避免注入
		}
		return fmt.Sprintf("JSONExtractString(%s, '%s')", col, jsonKey)
	}
	return stringcase.ToSnakeCase(field)
}

func (poc Processor) appendWhere(builder *query.Builder, expr string, args ...interface{}) *query.Builder {
	if expr == "" {
		return builder
	}

	builder.Where(expr, args...)

	return builder
}

// Equal 等于
func (poc Processor) Equal(builder *query.Builder, field, value string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" {
		return builder
	}
	return poc.appendWhere(builder, fmt.Sprintf("%s = ?", col), value)
}

// NotEqual 不等于
func (poc Processor) NotEqual(builder *query.Builder, field, value string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" {
		return builder
	}
	return poc.appendWhere(builder, fmt.Sprintf("%s != ?", col), value)
}

// In 包含
func (poc Processor) In(builder *query.Builder, field, value string, values []string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" {
		return builder
	}

	// 支持 JSON 数组字符串或 values 列表
	if value != "" {
		var arr []interface{}
		if err := poc.codec.Unmarshal([]byte(value), &arr); err == nil {
			if len(arr) == 0 {
				return poc.appendWhere(builder, "1 = 0")
			}
			ps := strings.Repeat("?,", len(arr))
			ps = strings.TrimRight(ps, ",")
			return poc.appendWhere(builder, fmt.Sprintf("%s IN (%s)", col, ps), arr...)
		} else {
			if strings.Contains(value, ",") {
				parts := strings.Split(value, ",")
				args := make([]interface{}, 0, len(parts))
				for _, p := range parts {
					p = strings.TrimSpace(p)
					if p != "" {
						args = append(args, p)
					}
				}
				if len(args) == 0 {
					return poc.appendWhere(builder, "1 = 0")
				}
				ps := strings.Repeat("?,", len(args))
				ps = strings.TrimRight(ps, ",")
				return poc.appendWhere(builder, fmt.Sprintf("%s IN (%s)", col, ps), args...)
			}
		}
	}

	if len(values) > 0 {
		ps := strings.Repeat("?,", len(values))
		ps = strings.TrimRight(ps, ",")
		args := make([]interface{}, 0, len(values))
		for _, v := range values {
			args = append(args, v)
		}
		return poc.appendWhere(builder, fmt.Sprintf("%s IN (%s)", col, ps), args...)
	}

	return builder
}

// NotIn 不包含
func (poc Processor) NotIn(builder *query.Builder, field, value string, values []string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" {
		return builder
	}
	// 支持 JSON 数组字符串
	if value != "" {
		var arr []interface{}
		if err := poc.codec.Unmarshal([]byte(value), &arr); err == nil {
			if len(arr) == 0 {
				// 空集合 -> 不追加条件（或可视为始终为真）
				return builder
			}
			ps := strings.Repeat("?,", len(arr))
			ps = strings.TrimRight(ps, ",")
			return poc.appendWhere(builder, fmt.Sprintf("%s NOT IN (%s)", col, ps), arr...)
		} else {
			if strings.Contains(value, ",") {
				parts := strings.Split(value, ",")
				args := make([]interface{}, 0, len(parts))
				for _, p := range parts {
					p = strings.TrimSpace(p)
					if p != "" {
						args = append(args, p)
					}
				}
				if len(args) == 0 {
					return poc.appendWhere(builder, "1 = 0")
				}
				ps := strings.Repeat("?,", len(args))
				ps = strings.TrimRight(ps, ",")
				return poc.appendWhere(builder, fmt.Sprintf("%s IN (%s)", col, ps), args...)
			}
		}
	}

	if len(values) > 0 {
		ps := strings.Repeat("?,", len(values))
		ps = strings.TrimRight(ps, ",")
		args := make([]interface{}, 0, len(values))
		for _, v := range values {
			args = append(args, v)
		}
		return poc.appendWhere(builder, fmt.Sprintf("%s NOT IN (%s)", col, ps), args...)
	}

	return builder
}

// GTE 大于等于
func (poc Processor) GTE(builder *query.Builder, field, value string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" {
		return builder
	}
	return poc.appendWhere(builder, fmt.Sprintf("%s >= ?", col), value)
}

// GT 大于
func (poc Processor) GT(builder *query.Builder, field, value string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" {
		return builder
	}
	return poc.appendWhere(builder, fmt.Sprintf("%s > ?", col), value)
}

// LTE 小于等于
func (poc Processor) LTE(builder *query.Builder, field, value string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" {
		return builder
	}
	return poc.appendWhere(builder, fmt.Sprintf("%s <= ?", col), value)
}

// LT 小于
func (poc Processor) LT(builder *query.Builder, field, value string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" {
		return builder
	}
	return poc.appendWhere(builder, fmt.Sprintf("%s < ?", col), value)
}

// Range BETWEEN 范围查询
func (poc Processor) Range(builder *query.Builder, field, value string, values []string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" {
		return builder
	}
	// 支持 JSON 数组字符串
	if value != "" {
		var arr []interface{}
		if err := poc.codec.Unmarshal([]byte(value), &arr); err == nil {
			if len(arr) == 2 {
				return poc.appendWhere(builder, fmt.Sprintf("%s BETWEEN ? AND ?", col), arr[0], arr[1])
			}
		} else {
			if strings.Contains(value, ",") {
				parts := strings.SplitN(value, ",", 2)
				if len(parts) == 2 {
					return poc.appendWhere(builder, fmt.Sprintf("%s BETWEEN ? AND ?", col), strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))
				}
			}
		}
	}
	if len(values) == 2 {
		return poc.appendWhere(builder, fmt.Sprintf("%s BETWEEN ? AND ?", col), values[0], values[1])
	}
	// fallback to equality when single
	if value != "" {
		return poc.appendWhere(builder, fmt.Sprintf("%s = ?", col), value)
	}
	return builder
}

// IsNull 检查是否为 NULL
func (poc Processor) IsNull(builder *query.Builder, field string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" {
		return builder
	}
	return poc.appendWhere(builder, fmt.Sprintf("%s IS NULL", col))
}

// IsNotNull 检查是否不为 NULL
func (poc Processor) IsNotNull(builder *query.Builder, field string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" {
		return builder
	}
	return poc.appendWhere(builder, fmt.Sprintf("%s IS NOT NULL", col))
}

// Contains (LIKE %val%)
func (poc Processor) Contains(builder *query.Builder, field, value string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" {
		return builder
	}
	p := "%" + value + "%"
	return poc.appendWhere(builder, fmt.Sprintf("%s LIKE ?", col), p)
}

// InsensitiveContains (lower(col) LIKE lower(?))
func (poc Processor) InsensitiveContains(builder *query.Builder, field, value string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" {
		return builder
	}
	p := "%" + value + "%"
	return poc.appendWhere(builder, fmt.Sprintf("lower(%s) LIKE lower(?)", col), p)
}

// StartsWith 开始于
func (poc Processor) StartsWith(builder *query.Builder, field, value string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" {
		return builder
	}
	p := value + "%"
	return poc.appendWhere(builder, fmt.Sprintf("%s LIKE ?", col), p)
}

// InsensitiveStartsWith 开始于（不区分大小写）
func (poc Processor) InsensitiveStartsWith(builder *query.Builder, field, value string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" {
		return builder
	}
	p := value + "%"
	return poc.appendWhere(builder, fmt.Sprintf("lower(%s) LIKE lower(?)", col), p)
}

// EndsWith 结束于
func (poc Processor) EndsWith(builder *query.Builder, field, value string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" {
		return builder
	}
	p := "%" + value
	return poc.appendWhere(builder, fmt.Sprintf("%s LIKE ?", col), p)
}

// InsensitiveEndsWith 结束于（不区分大小写）
func (poc Processor) InsensitiveEndsWith(builder *query.Builder, field, value string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" {
		return builder
	}
	p := "%" + value
	return poc.appendWhere(builder, fmt.Sprintf("lower(%s) LIKE lower(?)", col), p)
}

// Exact (exact match) 等值比较
func (poc Processor) Exact(builder *query.Builder, field, value string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" {
		return builder
	}
	// Exact 使用等值比较
	return poc.appendWhere(builder, fmt.Sprintf("%s = ?", col), value)
}

// InsensitiveExact 等值比较（不区分大小写）
func (poc Processor) InsensitiveExact(builder *query.Builder, field, value string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" {
		return builder
	}
	return poc.appendWhere(builder, fmt.Sprintf("lower(%s) = lower(?)", col), value)
}

// Regex (ClickHouse 使用 match(column, 'pattern'))
func (poc Processor) Regex(builder *query.Builder, field, value string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" || strings.TrimSpace(value) == "" {
		return builder
	}
	return poc.appendWhere(builder, fmt.Sprintf("match(%s, ?)", col), value)
}

// InsensitiveRegex 使用 lower(...) 进行不区分大小写的匹配
func (poc Processor) InsensitiveRegex(builder *query.Builder, field, value string) *query.Builder {
	col := poc.colExpr(field)
	if col == "" || strings.TrimSpace(value) == "" {
		return builder
	}
	return poc.appendWhere(builder, fmt.Sprintf("match(lower(%s), lower(?))", col), value)
}

// Search 简单全文搜索，fallback 为 LIKE %val%
func (poc Processor) Search(builder *query.Builder, field, value string) *query.Builder {
	if strings.TrimSpace(value) == "" {
		return builder
	}
	col := poc.colExpr(field)
	if col == "" {
		return builder
	}
	p := "%" + value + "%"
	return poc.appendWhere(builder, fmt.Sprintf("%s LIKE ?", col), p)
}

// DatePartField 为 ClickHouse 提供简单的 date part 表达式，如 YEAR(col)
func (poc Processor) DatePartField(datePart, field string) string {
	if !paginator.IsValidDatePartString(datePart) || strings.TrimSpace(field) == "" {
		return ""
	}
	part := strings.ToUpper(datePart)
	col := stringcase.ToSnakeCase(field)
	// ClickHouse 常用函数名与 SQL 相近，采用 PART(col) 形式（如 YEAR(col), MONTH(col)）
	return fmt.Sprintf("%s(%s)", part, col)
}

// JsonbField 返回 JSON 字段表达式字符串（如 JSONExtractString(col, 'key')）
func (poc Processor) JsonbField(jsonbField, field string) string {
	field = stringcase.ToSnakeCase(strings.TrimSpace(field))
	if field == "" || strings.TrimSpace(jsonbField) == "" {
		return ""
	}
	if !jsonKeyPattern.MatchString(jsonbField) {
		return ""
	}
	return fmt.Sprintf("JSONExtractString(%s, '%s')", field, jsonbField)
}
