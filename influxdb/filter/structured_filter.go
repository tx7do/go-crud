package filter

import (
	"strings"

	"github.com/go-kratos/kratos/v2/encoding"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/influxdb/query"
)

// StructuredFilter 将 FilterExpr 转为基于 InfluxDB 的 查询条件，使用 Processor 在 *query.Builder 上追加 WHERE 子句
type StructuredFilter struct {
	codec     encoding.Codec
	processor *Processor
}

// NewStructuredFilter 创建 InfluxDB 用的 StructuredFilter
func NewStructuredFilter() *StructuredFilter {
	return &StructuredFilter{
		codec:     encoding.GetCodec("json"),
		processor: NewProcessor(),
	}
}

// BuildSelectors 将 expr 的条件应用到 builder 上；若 builder 为 nil 则新建一个。
// AND 类型会把所有子条件逐一通过 Processor.Process 添加（AND 语义）。
// OR 类型仅在组内只有单个条件或单个子组时处理该单项，复杂 OR 跳过（query.Builder 不支持复杂 OR）。
func (sf StructuredFilter) BuildSelectors(builder *query.Builder, expr *paginationV1.FilterExpr) (*query.Builder, error) {
	if builder == nil {
		builder = query.NewQueryBuilder("m")
	}
	if expr == nil {
		return builder, nil
	}

	// helper: 处理单个 Condition，返回是否成功处理（用于判断 OR 单项）
	processCond := func(b *query.Builder, cond *paginationV1.FilterCondition) bool {
		if cond == nil {
			return false
		}
		field := cond.GetField()
		if strings.TrimSpace(field) == "" {
			return false
		}
		val := ""
		switch cond.ValueOneof.(type) {
		case *paginationV1.FilterCondition_Value:
			val = cond.GetValue()
		default:
		}
		values := cond.GetValues()
		// 委托 Processor 追加到 builder
		sf.processor.Process(b, cond.GetOp(), field, val, values)
		return true
	}

	// 递归处理表达式
	var walk func(b *query.Builder, e *paginationV1.FilterExpr) bool
	walk = func(b *query.Builder, e *paginationV1.FilterExpr) bool {
		if e == nil {
			return false
		}
		switch e.GetType() {
		case paginationV1.ExprType_EXPR_TYPE_UNSPECIFIED:
			return false
		case paginationV1.ExprType_AND:
			applied := false
			for _, cond := range e.GetConditions() {
				if processCond(b, cond) {
					applied = true
				}
			}
			for _, g := range e.GetGroups() {
				if walk(b, g) {
					applied = true
				}
			}
			return applied
		case paginationV1.ExprType_OR:
			// 仅在组内总共只有一个项时处理该项（视为单一条件的 OR），否则跳过复杂 OR
			totalParts := len(e.GetConditions()) + len(e.GetGroups())
			if totalParts == 0 {
				return false
			}
			if totalParts == 1 {
				if len(e.GetConditions()) == 1 {
					return processCond(b, e.GetConditions()[0])
				}
				// single group
				if len(e.GetGroups()) == 1 {
					return walk(b, e.GetGroups()[0])
				}
			}
			// 复杂 OR 不支持，跳过
			return false
		default:
			return false
		}
	}

	walk(builder, expr)
	return builder, nil
}
