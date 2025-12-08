package filter

import (
	"fmt"
	"strings"

	"github.com/go-kratos/kratos/v2/encoding"
	"github.com/go-kratos/kratos/v2/log"

	"github.com/tx7do/go-utils/stringcase"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/clickhouse/query"
)

// StructuredFilter 基于 FilterExpr 的 ClickHouse 过滤器（不依赖 GORM）
type StructuredFilter struct {
	codec     encoding.Codec
	processor *Processor
}

func NewStructuredFilter() *StructuredFilter {
	return &StructuredFilter{
		codec:     encoding.GetCodec("json"),
		processor: NewProcessor(),
	}
}

// BuildSelectors 将 FilterExpr 转为并直接应用于 *query.Builder 的 WHERE/ARGS
func (sf StructuredFilter) BuildSelectors(builder *query.Builder, expr *paginationV1.FilterExpr) (*query.Builder, error) {
	if builder == nil {
		return nil, fmt.Errorf("builder is nil")
	}
	if expr == nil {
		return builder, nil
	}
	if expr.GetType() == paginationV1.ExprType_EXPR_TYPE_UNSPECIFIED {
		log.Warn("Skipping unspecified FilterExpr")
		return builder, nil
	}

	parts, partsArgs, err := sf.buildParts(expr)
	if err != nil {
		return builder, err
	}
	// 将每个生成的子表达式追加到 builder
	for i, p := range parts {
		if strings.TrimSpace(p) == "" {
			continue
		}
		if len(partsArgs) > i && len(partsArgs[i]) > 0 {
			builder.Where(p, partsArgs[i]...)
		} else {
			builder.Where(p)
		}
	}
	return builder, nil
}

// buildParts 将 expr 展开为若干子表达式及其对应参数列表（不直接修改 builder）
func (sf StructuredFilter) buildParts(expr *paginationV1.FilterExpr) ([]string, [][]interface{}, error) {
	if expr == nil {
		return nil, nil, nil
	}
	if expr.GetType() == paginationV1.ExprType_EXPR_TYPE_UNSPECIFIED {
		return nil, nil, nil
	}

	// helper: 根据 condition 生成单个 SQL 片段和参数
	buildCond := func(cond *paginationV1.Condition) (string, []interface{}) {
		if cond == nil {
			return "", nil
		}
		field := cond.GetField()
		val := ""
		if cond.Value != nil {
			val = *cond.Value
		}
		opName := cond.GetOp().String()
		values := cond.GetValues()

		// 支持 JSON 字段 (e.g. preferences.daily_email) -> JSONExtractString(col, 'key')
		isJSON := strings.Contains(field, ".")
		var colExpr string
		if isJSON {
			parts := strings.SplitN(field, ".", 2)
			col := stringcase.ToSnakeCase(parts[0])
			jsonKey := parts[1]
			colExpr = fmt.Sprintf("JSONExtractString(%s, '%s')", col, jsonKey)
		} else {
			colExpr = stringcase.ToSnakeCase(field)
		}

		switch opName {
		case "OP_EQ", "EQ", "EQUAL", "OP_EQUAL":
			return fmt.Sprintf("%s = ?", colExpr), []interface{}{val}
		case "OP_NEQ", "NE", "NEQ", "OP_NOT_EQUAL":
			return fmt.Sprintf("%s != ?", colExpr), []interface{}{val}
		case "OP_GT", "GT":
			return fmt.Sprintf("%s > ?", colExpr), []interface{}{val}
		case "OP_GTE", "GTE":
			return fmt.Sprintf("%s >= ?", colExpr), []interface{}{val}
		case "OP_LT", "LT":
			return fmt.Sprintf("%s < ?", colExpr), []interface{}{val}
		case "OP_LTE", "LTE":
			return fmt.Sprintf("%s <= ?", colExpr), []interface{}{val}
		case "OP_IS_NULL", "IS_NULL":
			return fmt.Sprintf("%s IS NULL", colExpr), nil
		case "OP_IS_NOT_NULL", "IS_NOT_NULL":
			return fmt.Sprintf("%s IS NOT NULL", colExpr), nil
		case "OP_IN", "IN":
			// 支持 values 列表，否则如果只有 Value 则解析逗号分隔
			var args []interface{}
			if len(values) > 0 {
				for _, v := range values {
					args = append(args, v)
				}
			} else if val != "" {
				parts := strings.Split(val, ",")
				for _, p := range parts {
					args = append(args, strings.TrimSpace(p))
				}
			}
			if len(args) == 0 {
				return "1 = 0", nil
			}
			ps := strings.Repeat("?,", len(args))
			ps = strings.TrimRight(ps, ",")
			return fmt.Sprintf("%s IN (%s)", colExpr, ps), args
		case "OP_BETWEEN", "BETWEEN":
			if len(values) >= 2 {
				return fmt.Sprintf("%s BETWEEN ? AND ?", colExpr), []interface{}{values[0], values[1]}
			}
			parts := strings.Split(val, ",")
			if len(parts) >= 2 {
				return fmt.Sprintf("%s BETWEEN ? AND ?", colExpr), []interface{}{strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])}
			}
			return fmt.Sprintf("%s = ?", colExpr), []interface{}{val}
		case "OP_CONTAINS", "CONTAINS", "OP_LIKE", "LIKE":
			p := "%" + val + "%"
			return fmt.Sprintf("%s LIKE ?", colExpr), []interface{}{p}
		case "OP_STARTS_WITH", "STARTS_WITH":
			p := val + "%"
			return fmt.Sprintf("%s LIKE ?", colExpr), []interface{}{p}
		case "OP_ENDS_WITH", "ENDS_WITH":
			p := "%" + val
			return fmt.Sprintf("%s LIKE ?", colExpr), []interface{}{p}
		default:
			if val != "" {
				return fmt.Sprintf("%s = ?", colExpr), []interface{}{val}
			}
			return "", nil
		}
	}

	flatten := func(arr [][]interface{}) []interface{} {
		var out []interface{}
		for _, a := range arr {
			if len(a) > 0 {
				out = append(out, a...)
			}
		}
		return out
	}

	var parts []string
	var partsArgs [][]interface{}

	switch expr.GetType() {
	case paginationV1.ExprType_AND:
		// 条件集合
		for _, cond := range expr.GetConditions() {
			clause, args := buildCond(cond)
			if clause == "" {
				continue
			}
			parts = append(parts, clause)
			partsArgs = append(partsArgs, args)
		}
		// 子组按 AND 语义合并为单个带括号的表达式并作为一个部分追加
		for _, g := range expr.GetGroups() {
			subParts, subArgs, err := sf.buildParts(g)
			if err != nil {
				log.Errorf("buildParts sub-group error: %v", err)
				continue
			}
			if len(subParts) == 0 {
				continue
			}
			joined := strings.Join(subParts, " AND ")
			parts = append(parts, "("+joined+")")
			partsArgs = append(partsArgs, flatten(subArgs))
		}
		return parts, partsArgs, nil

	case paginationV1.ExprType_OR:
		var orParts []string
		var orArgs [][]interface{}
		// 条件集合作为 OR 的子项
		for _, cond := range expr.GetConditions() {
			clause, args := buildCond(cond)
			if clause == "" {
				continue
			}
			orParts = append(orParts, clause)
			orArgs = append(orArgs, args)
		}
		// 子组作为 OR 的子项，子组内部以 AND 连接
		for _, g := range expr.GetGroups() {
			subParts, subArgs, err := sf.buildParts(g)
			if err != nil {
				log.Errorf("buildParts sub-group error: %v", err)
				continue
			}
			if len(subParts) == 0 {
				continue
			}
			joined := strings.Join(subParts, " AND ")
			orParts = append(orParts, "("+joined+")")
			orArgs = append(orArgs, flatten(subArgs))
		}
		if len(orParts) > 0 {
			parts = append(parts, "("+strings.Join(orParts, " OR ")+")")
			partsArgs = append(partsArgs, flatten(orArgs))
		}
		return parts, partsArgs, nil

	default:
		// 未知类型：跳过
		return nil, nil, nil
	}
}
