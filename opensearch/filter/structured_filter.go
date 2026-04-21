package filter

import (
	"strings"

	"github.com/go-kratos/kratos/v2/encoding"
	"github.com/tx7do/go-crud/opensearch/query"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

// StructuredFilter 将 FilterExpr 转为 OpenSearch 查询并应用到 *query.Builder
// 适配 OpenSearch 查询语法

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

// BuildSelectors 递归将 expr 转为 OpenSearch 查询并通过 builder 应用。
func (sf StructuredFilter) BuildSelectors(builder *query.Builder, expr *paginationV1.FilterExpr) (*query.Builder, error) {
	if builder == nil {
		builder = query.NewQueryBuilder()
	}
	if expr == nil {
		return builder, nil
	}

	var buildParts func(e *paginationV1.FilterExpr, b *query.Builder)
	buildParts = func(e *paginationV1.FilterExpr, b *query.Builder) {
		if e == nil {
			return
		}
		switch e.GetType() {
		case paginationV1.ExprType_EXPR_TYPE_UNSPECIFIED:
			return
		case paginationV1.ExprType_AND:
			for _, cond := range e.GetConditions() {
				if q := sf.buildCond(cond); q != nil {
					b.Where(q)
				}
			}
			for _, g := range e.GetGroups() {
				buildParts(g, b)
			}
		case paginationV1.ExprType_OR:
			ors := make([]map[string]any, 0)
			for _, cond := range e.GetConditions() {
				if q := sf.buildCond(cond); q != nil {
					ors = append(ors, q)
				}
			}
			for _, g := range e.GetGroups() {
				ob := query.NewQueryBuilder()
				buildParts(g, ob)
				q := ob.Build()["query"].(map[string]any)
				ors = append(ors, q)
			}
			if len(ors) > 0 {
				b.Should(map[string]any{"bool": map[string]any{"must": ors}})
			}
		}
	}

	buildParts(expr, builder)
	return builder, nil
}

// buildCond 将单个 Condition 转为 OpenSearch 查询片段
func (sf StructuredFilter) buildCond(cond *paginationV1.FilterCondition) map[string]any {
	if cond == nil {
		return nil
	}
	field := cond.GetField()
	if strings.TrimSpace(field) == "" {
		return nil
	}
	key := sf.processor.makeKey(field)
	if key == "" {
		return nil
	}

	val := cond.GetValue()
	values := cond.GetValues()

	switch cond.GetOp() {
	case paginationV1.Operator_EQ:
		return map[string]any{"term": map[string]any{key: val}}
	case paginationV1.Operator_NEQ:
		return map[string]any{"bool": map[string]any{"must_not": []map[string]any{{"term": map[string]any{key: val}}}}}
	case paginationV1.Operator_IN:
		arr := values
		if len(arr) == 0 && val != "" {
			arr = strings.Split(val, ",")
		}
		return map[string]any{"terms": map[string]any{key: arr}}
	case paginationV1.Operator_NIN:
		arr := values
		if len(arr) == 0 && val != "" {
			arr = strings.Split(val, ",")
		}
		return map[string]any{"bool": map[string]any{"must_not": []map[string]any{{"terms": map[string]any{key: arr}}}}}
	case paginationV1.Operator_GTE:
		return map[string]any{"range": map[string]any{key: map[string]any{"gte": val}}}
	case paginationV1.Operator_GT:
		return map[string]any{"range": map[string]any{key: map[string]any{"gt": val}}}
	case paginationV1.Operator_LTE:
		return map[string]any{"range": map[string]any{key: map[string]any{"lte": val}}}
	case paginationV1.Operator_LT:
		return map[string]any{"range": map[string]any{key: map[string]any{"lt": val}}}
	case paginationV1.Operator_BETWEEN:
		if len(values) == 2 {
			return map[string]any{"range": map[string]any{key: map[string]any{"gte": values[0], "lte": values[1]}}}
		}
		parts := strings.Split(val, ",")
		if len(parts) == 2 {
			return map[string]any{"range": map[string]any{key: map[string]any{"gte": strings.TrimSpace(parts[0]), "lte": strings.TrimSpace(parts[1])}}}
		}
		return nil
	case paginationV1.Operator_IS_NULL:
		return map[string]any{"bool": map[string]any{"must_not": []map[string]any{{"exists": map[string]any{"field": key}}}}}
	case paginationV1.Operator_IS_NOT_NULL:
		return map[string]any{"exists": map[string]any{"field": key}}
	case paginationV1.Operator_CONTAINS, paginationV1.Operator_ICONTAINS:
		if strings.TrimSpace(val) == "" {
			return nil
		}
		return map[string]any{"match_phrase": map[string]any{key: val}}
	case paginationV1.Operator_STARTS_WITH, paginationV1.Operator_ISTARTS_WITH:
		if strings.TrimSpace(val) == "" {
			return nil
		}
		return map[string]any{"prefix": map[string]any{key: val}}
	case paginationV1.Operator_ENDS_WITH, paginationV1.Operator_IENDS_WITH:
		if strings.TrimSpace(val) == "" {
			return nil
		}
		return map[string]any{"wildcard": map[string]any{key: "*" + val}}
	case paginationV1.Operator_EXACT, paginationV1.Operator_IEXACT:
		return map[string]any{"term": map[string]any{key: val}}
	case paginationV1.Operator_REGEXP, paginationV1.Operator_IREGEXP:
		if strings.TrimSpace(val) == "" {
			return nil
		}
		return map[string]any{"regexp": map[string]any{key: val}}
	case paginationV1.Operator_SEARCH:
		if strings.TrimSpace(val) == "" {
			return nil
		}
		return map[string]any{"query_string": map[string]any{"query": val}}
	default:
		if val != "" {
			return map[string]any{"term": map[string]any{key: val}}
		}
		return nil
	}
}
