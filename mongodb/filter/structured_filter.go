package filter

import (
	"strings"

	"github.com/go-kratos/kratos/v2/encoding"
	bsonV2 "go.mongodb.org/mongo-driver/v2/bson"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/mongodb/query"
)

// StructuredFilter 将 FilterExpr 转为 MongoDB BSON filter 并应用到 *query.Builder
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

// BuildSelectors 将 expr 转为 BSON 过滤器并通过 builder.SetFilter 应用。
// 若 builder 为 nil 会新建一个。
func (sf StructuredFilter) BuildSelectors(builder *query.Builder, expr *paginationV1.FilterExpr) (*query.Builder, error) {
	if builder == nil {
		builder = &query.Builder{}
	}
	if expr == nil {
		return builder, nil
	}

	// 递归将 expr 转为单个 bsonV2.M 过滤器（可能包含 $and/$or）
	var buildParts func(e *paginationV1.FilterExpr) bsonV2.M
	buildParts = func(e *paginationV1.FilterExpr) bsonV2.M {
		if e == nil {
			return nil
		}
		switch e.GetType() {
		case paginationV1.ExprType_EXPR_TYPE_UNSPECIFIED:
			return nil
		case paginationV1.ExprType_AND:
			var andParts bsonV2.A
			// conditions
			for _, cond := range e.GetConditions() {
				if c := sf.buildCond(cond); c != nil {
					andParts = append(andParts, c)
				}
			}
			// groups
			for _, g := range e.GetGroups() {
				if sub := buildParts(g); sub != nil {
					andParts = append(andParts, sub)
				}
			}
			if len(andParts) == 0 {
				return nil
			}
			if len(andParts) == 1 {
				// single part: return it directly
				if m, ok := andParts[0].(bsonV2.M); ok {
					return m
				}
				return bsonV2.M{"$and": andParts}
			}
			return bsonV2.M{"$and": andParts}

		case paginationV1.ExprType_OR:
			var orParts bsonV2.A
			for _, cond := range e.GetConditions() {
				if c := sf.buildCond(cond); c != nil {
					orParts = append(orParts, c)
				}
			}
			for _, g := range e.GetGroups() {
				if sub := buildParts(g); sub != nil {
					orParts = append(orParts, sub)
				}
			}
			if len(orParts) == 0 {
				return nil
			}
			if len(orParts) == 1 {
				if m, ok := orParts[0].(bsonV2.M); ok {
					return m
				}
				return bsonV2.M{"$or": orParts}
			}
			return bsonV2.M{"$or": orParts}
		default:
			return nil
		}
	}

	filter := buildParts(expr)
	if filter != nil {
		builder.SetFilter(filter)
	}
	return builder, nil
}

// buildCond 将单个 Condition 转为 bsonV2.M，失败或不可用返回 nil
func (sf StructuredFilter) buildCond(cond *paginationV1.Condition) bsonV2.M {
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

	val := ""
	if cond.Value != nil {
		val = *cond.Value
	}
	values := cond.GetValues()

	// helper: parse JSON array string into []interface{}
	parseArray := func(s string) ([]interface{}, bool) {
		if strings.TrimSpace(s) == "" {
			return nil, false
		}
		var arr []interface{}
		if err := sf.codec.Unmarshal([]byte(s), &arr); err == nil {
			return arr, true
		}
		// comma separated fallback
		if strings.Contains(s, ",") {
			parts := strings.Split(s, ",")
			out := make([]interface{}, 0, len(parts))
			for _, p := range parts {
				p = strings.TrimSpace(p)
				if p != "" {
					out = append(out, p)
				}
			}
			if len(out) > 0 {
				return out, true
			}
		}
		return nil, false
	}

	switch cond.GetOp() {
	case paginationV1.Operator_EQ:
		return bsonV2.M{key: val}
	case paginationV1.Operator_NEQ:
		return bsonV2.M{key: bsonV2.M{"$ne": val}}
	case paginationV1.Operator_IN:
		// prefer JSON array in Value
		if arr, ok := parseArray(val); ok {
			if len(arr) == 0 {
				// 永假
				return bsonV2.M{"$expr": bsonV2.A{bsonV2.M{"$eq": bsonV2.A{1, 0}}}}
			}
			return bsonV2.M{key: bsonV2.M{"$in": arr}}
		}
		if len(values) > 0 {
			args := make([]interface{}, 0, len(values))
			for _, v := range values {
				args = append(args, v)
			}
			if len(args) == 0 {
				return bsonV2.M{"$expr": bsonV2.A{bsonV2.M{"$eq": bsonV2.A{1, 0}}}}
			}
			return bsonV2.M{key: bsonV2.M{"$in": args}}
		}
		return nil
	case paginationV1.Operator_NIN:
		if arr, ok := parseArray(val); ok {
			if len(arr) == 0 {
				// 空集合 -> 不追加条件
				return nil
			}
			return bsonV2.M{key: bsonV2.M{"$nin": arr}}
		}
		if len(values) > 0 {
			args := make([]interface{}, 0, len(values))
			for _, v := range values {
				args = append(args, v)
			}
			if len(args) == 0 {
				return nil
			}
			return bsonV2.M{key: bsonV2.M{"$nin": args}}
		}
		return nil
	case paginationV1.Operator_GTE:
		return bsonV2.M{key: bsonV2.M{"$gte": val}}
	case paginationV1.Operator_GT:
		return bsonV2.M{key: bsonV2.M{"$gt": val}}
	case paginationV1.Operator_LTE:
		return bsonV2.M{key: bsonV2.M{"$lte": val}}
	case paginationV1.Operator_LT:
		return bsonV2.M{key: bsonV2.M{"$lt": val}}
	case paginationV1.Operator_BETWEEN:
		// value may be JSON array or comma separated
		if arr, ok := parseArray(val); ok && len(arr) == 2 {
			return bsonV2.M{key: bsonV2.M{"$gte": arr[0], "$lte": arr[1]}}
		}
		if len(values) == 2 {
			return bsonV2.M{key: bsonV2.M{"$gte": values[0], "$lte": values[1]}}
		}
		if strings.Contains(val, ",") {
			parts := strings.SplitN(val, ",", 2)
			if len(parts) == 2 {
				a := strings.TrimSpace(parts[0])
				b := strings.TrimSpace(parts[1])
				return bsonV2.M{key: bsonV2.M{"$gte": a, "$lte": b}}
			}
		}
		if val != "" {
			return bsonV2.M{key: val}
		}
		return nil
	case paginationV1.Operator_IS_NULL:
		return bsonV2.M{key: nil}
	case paginationV1.Operator_IS_NOT_NULL:
		return bsonV2.M{key: bsonV2.M{"$ne": nil}}
	case paginationV1.Operator_CONTAINS:
		if strings.TrimSpace(val) == "" {
			return nil
		}
		return bsonV2.M{key: bsonV2.M{"$regex": val}}
	case paginationV1.Operator_ICONTAINS:
		if strings.TrimSpace(val) == "" {
			return nil
		}
		return bsonV2.M{key: bsonV2.M{"$regex": val, "$options": "i"}}
	case paginationV1.Operator_STARTS_WITH:
		if strings.TrimSpace(val) == "" {
			return nil
		}
		return bsonV2.M{key: bsonV2.M{"$regex": "^" + val}}
	case paginationV1.Operator_ISTARTS_WITH:
		if strings.TrimSpace(val) == "" {
			return nil
		}
		return bsonV2.M{key: bsonV2.M{"$regex": "^" + val, "$options": "i"}}
	case paginationV1.Operator_ENDS_WITH:
		if strings.TrimSpace(val) == "" {
			return nil
		}
		return bsonV2.M{key: bsonV2.M{"$regex": val + "$"}}
	case paginationV1.Operator_IENDS_WITH:
		if strings.TrimSpace(val) == "" {
			return nil
		}
		return bsonV2.M{key: bsonV2.M{"$regex": val + "$", "$options": "i"}}
	case paginationV1.Operator_EXACT:
		return bsonV2.M{key: val}
	case paginationV1.Operator_IEXACT:
		return bsonV2.M{key: bsonV2.M{"$regex": "^" + val + "$", "$options": "i"}}
	case paginationV1.Operator_REGEXP:
		if strings.TrimSpace(val) == "" {
			return nil
		}
		return bsonV2.M{key: bsonV2.M{"$regex": val}}
	case paginationV1.Operator_IREGEXP:
		if strings.TrimSpace(val) == "" {
			return nil
		}
		return bsonV2.M{key: bsonV2.M{"$regex": val, "$options": "i"}}
	case paginationV1.Operator_SEARCH:
		if strings.TrimSpace(val) == "" {
			return nil
		}
		// fallback to regex contains
		return bsonV2.M{key: bsonV2.M{"$regex": val}}
	default:
		// unknown operator -> fallback to equality if value present
		if val != "" {
			return bsonV2.M{key: val}
		}
		return nil
	}
}
