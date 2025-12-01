package filter

import (
	"entgo.io/ent/dialect/sql"

	"github.com/go-kratos/kratos/v2/encoding"
	_ "github.com/go-kratos/kratos/v2/encoding/json"
	"github.com/go-kratos/kratos/v2/log"

	pagination "github.com/tx7do/go-curd/api/gen/go/pagination/v1"
)

// StructuredFilter 基于 FilterExpr 的过滤器
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

// BuildSelectors 构建过滤选择器
func (sf StructuredFilter) BuildSelectors(expr *pagination.FilterExpr) ([]func(s *sql.Selector), error) {
	var queryConditions []func(s *sql.Selector)

	if expr == nil {
		return queryConditions, nil
	}

	// Skip unspecified expressions
	if expr.GetType() == pagination.ExprType_EXPR_TYPE_UNSPECIFIED {
		log.Warn("Skipping unspecified FilterExpr")
		return nil, nil
	}

	selector, err := sf.buildFilterSelector(expr)
	if err != nil {
		return nil, err
	}
	if selector != nil {
		queryConditions = append(queryConditions, selector)
	}

	return queryConditions, nil
}

func (sf StructuredFilter) buildFilterSelector(expr *pagination.FilterExpr) (func(s *sql.Selector), error) {
	var selector func(s *sql.Selector)

	// Skip nil expressions
	if expr == nil {
		log.Warn("Skipping nil FilterExpr")
		return nil, nil
	}

	// Skip unspecified expressions
	if expr.GetType() == pagination.ExprType_EXPR_TYPE_UNSPECIFIED {
		log.Warn("Skipping unspecified FilterExpr")
		return nil, nil
	}

	// Process conditions
	selector = func(s *sql.Selector) {
		// Process groups recursively
		for _, cond := range expr.GetGroups() {
			subSelector, err := sf.buildFilterSelector(cond)
			if err != nil {
				log.Errorf("Error processing sub-group: %v", err)
				continue
			}
			if subSelector != nil {
				subSelector(s)
			}
		}

		// Process current level conditions
		ps, err := sf.processCondition(s, expr.GetConditions())
		if err != nil {
			return
		}

		// Combine predicates based on expression type
		switch expr.GetType() {
		case pagination.ExprType_AND:
			s.Where(sql.Or(ps...))
		case pagination.ExprType_OR:
			s.Where(sql.Or(ps...))
		}
	}

	return selector, nil
}

// processCondition 处理条件
func (sf StructuredFilter) processCondition(s *sql.Selector, conditions []*pagination.Condition) ([]*sql.Predicate, error) {
	if len(conditions) == 0 {
		return nil, nil
	}

	var ps []*sql.Predicate
	for _, cond := range conditions {
		p := sql.P()
		if cp := sf.processor.Process(s, p, cond.GetOp(), cond.GetField(), cond.GetValue(), cond.GetValues()); cp != nil {
			ps = append(ps, cp)
		}
	}

	return ps, nil
}
