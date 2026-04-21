// OpenSearch专用FilterProcessor，所有处理逻辑输出OpenSearch查询结构
// 递归、分组、条件、操作符等全部适配OpenSearch语法
// 仅保留接口风格，核心逻辑全部适配OpenSearch

package filter

import (
	"strings"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

type Processor struct{}

func NewProcessor() *Processor {
	return &Processor{}
}

// makeKey 字段名处理，可按需自定义
func (p *Processor) makeKey(field string) string {
	return strings.TrimSpace(field)
}

// BuildOpenSearchQuery 递归将 FilterExpr 转为 OpenSearch 查询结构
func (p *Processor) BuildOpenSearchQuery(expr *paginationV1.FilterExpr) map[string]any {
	if expr == nil {
		return nil
	}
	var buildParts func(e *paginationV1.FilterExpr) map[string]any
	buildParts = func(e *paginationV1.FilterExpr) map[string]any {
		if e == nil {
			return nil
		}
		switch e.GetType() {
		case paginationV1.ExprType_EXPR_TYPE_UNSPECIFIED:
			return nil
		case paginationV1.ExprType_AND:
			must := make([]map[string]any, 0)
			for _, cond := range e.GetConditions() {
				if q := p.buildCond(cond); q != nil {
					must = append(must, q)
				}
			}
			for _, g := range e.GetGroups() {
				if sub := buildParts(g); sub != nil {
					must = append(must, sub)
				}
			}
			if len(must) == 0 {
				return nil
			}
			return map[string]any{"bool": map[string]any{"must": must}}
		case paginationV1.ExprType_OR:
			should := make([]map[string]any, 0)
			for _, cond := range e.GetConditions() {
				if q := p.buildCond(cond); q != nil {
					should = append(should, q)
				}
			}
			for _, g := range e.GetGroups() {
				if sub := buildParts(g); sub != nil {
					should = append(should, sub)
				}
			}
			if len(should) == 0 {
				return nil
			}
			return map[string]any{"bool": map[string]any{"should": should}}
		default:
			return nil
		}
	}
	return buildParts(expr)
}

// buildCond 单个条件转OpenSearch结构
func (p *Processor) buildCond(cond *paginationV1.FilterCondition) map[string]any {
	if cond == nil {
		return nil
	}
	field := cond.GetField()
	if strings.TrimSpace(field) == "" {
		return nil
	}
	key := p.makeKey(field)
	if key == "" {
		return nil
	}
	val := cond.GetValue()
	values := cond.GetValues()
	switch cond.GetOp() {
	case paginationV1.Operator_EQ:
		return map[string]any{"term": map[string]any{key: val}}
	case paginationV1.Operator_NEQ:
		return map[string]any{
			"bool": map[string]any{
				"must_not": []map[string]any{
					{"term": map[string]any{key: val}},
				},
			},
		}
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
		return map[string]any{
			"bool": map[string]any{
				"must_not": []map[string]any{
					{"terms": map[string]any{key: arr}},
				},
			},
		}
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
		return map[string]any{
			"bool": map[string]any{
				"must_not": []map[string]any{
					{"exists": map[string]any{"field": key}},
				},
			},
		}
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
