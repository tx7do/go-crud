package filter

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-kratos/kratos/v2/encoding"
	bsonV2 "go.mongodb.org/mongo-driver/v2/bson"

	pagination "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/mongodb/query"
)

const (
	QueryDelimiter     = "__" // 分隔符
	JsonFieldDelimiter = "."  // JSON 字段分隔符
)

// QueryStringFilter 字符串过滤器 (MongoDB 版)，使用 Processor 构建字段名与解析规则
type QueryStringFilter struct {
	codec     encoding.Codec
	processor *Processor
}

func NewQueryStringFilter() *QueryStringFilter {
	return &QueryStringFilter{
		codec:     encoding.GetCodec("json"),
		processor: NewProcessor(),
	}
}

// BuildSelectors 将 and/or JSON 字符串解析并把过滤条件追加到 builder 中。
// 对于 andFilterJsonString：各 key => 条件以 AND 追加（直接多次调用 builder.SetFilter，假定合并为 $and）。
// 对于 orFilterJsonString：每个 map 内部的条件以 OR 合并成一个 $or 条件并追加到 builder。
func (sf *QueryStringFilter) BuildSelectors(builder *query.Builder, andFilterJsonString, orFilterJsonString string) (*query.Builder, error) {
	if builder == nil {
		builder = &query.Builder{}
	}

	// helper to convert common types to trimmed string
	toStr := func(v any) (string, bool) {
		switch t := v.(type) {
		case string:
			return strings.TrimSpace(t), true
		case []byte:
			return strings.TrimSpace(string(t)), true
		case interface{ String() string }:
			return strings.TrimSpace(t.String()), true
		default:
			return "", false
		}
	}

	// parse helper
	unmarshalToMaps := func(strJson string) ([]map[string]any, error) {
		var arr []map[string]any
		if strings.TrimSpace(strJson) == "" {
			return nil, nil
		}
		// try codec into array
		if err := sf.codec.Unmarshal([]byte(strJson), &arr); err == nil {
			return arr, nil
		}
		// try codec into single map
		var single map[string]any
		if err := sf.codec.Unmarshal([]byte(strJson), &single); err == nil {
			return []map[string]any{single}, nil
		}
		// fallback to standard json
		if err := json.Unmarshal([]byte(strJson), &arr); err == nil {
			return arr, nil
		}
		if err := json.Unmarshal([]byte(strJson), &single); err == nil {
			return []map[string]any{single}, nil
		}
		return nil, fmt.Errorf("invalid filter json")
	}

	// helper to build a single condition (bsonV2.M) from operator/field/value
	buildCondition := func(op pagination.Operator, field string, value any, values []any) bsonV2.M {
		key := sf.processor.makeKey(field)
		if key == "" {
			return nil
		}
		switch op {
		case pagination.Operator_EQ:
			return bsonV2.M{key: value}
		case pagination.Operator_NEQ:
			return bsonV2.M{key: bsonV2.M{"$ne": value}}
		case pagination.Operator_IN:
			// support JSON array string
			if s, ok := toStr(value); ok && s != "" {
				var arr []interface{}
				if err := sf.codec.Unmarshal([]byte(s), &arr); err == nil {
					if len(arr) == 0 {
						return bsonV2.M{"$expr": bsonV2.A{bsonV2.M{"$eq": bsonV2.A{1, 0}}}}
					}
					return bsonV2.M{key: bsonV2.M{"$in": arr}}
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
						return bsonV2.M{"$expr": bsonV2.A{bsonV2.M{"$eq": bsonV2.A{1, 0}}}}
					}
					return bsonV2.M{key: bsonV2.M{"$in": args}}
				}
			}
			if len(values) > 0 {
				args := make([]interface{}, 0, len(values))
				for _, v := range values {
					args = append(args, v)
				}
				return bsonV2.M{key: bsonV2.M{"$in": args}}
			}
			return nil
		case pagination.Operator_NIN:
			if s, ok := toStr(value); ok && s != "" {
				var arr []interface{}
				if err := sf.codec.Unmarshal([]byte(s), &arr); err == nil {
					if len(arr) == 0 {
						return nil
					}
					return bsonV2.M{key: bsonV2.M{"$nin": arr}}
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
						return nil
					}
					return bsonV2.M{key: bsonV2.M{"$nin": args}}
				}
			}
			if len(values) > 0 {
				args := make([]interface{}, 0, len(values))
				for _, v := range values {
					args = append(args, v)
				}
				return bsonV2.M{key: bsonV2.M{"$nin": args}}
			}
			return nil
		case pagination.Operator_GTE:
			return bsonV2.M{key: bsonV2.M{"$gte": value}}
		case pagination.Operator_GT:
			return bsonV2.M{key: bsonV2.M{"$gt": value}}
		case pagination.Operator_LTE:
			return bsonV2.M{key: bsonV2.M{"$lte": value}}
		case pagination.Operator_LT:
			return bsonV2.M{key: bsonV2.M{"$lt": value}}
		case pagination.Operator_BETWEEN:
			// value may be JSON array or comma separated
			if s, ok := toStr(value); ok && s != "" {
				var arr []interface{}
				if err := sf.codec.Unmarshal([]byte(s), &arr); err == nil {
					if len(arr) == 2 {
						return bsonV2.M{key: bsonV2.M{"$gte": arr[0], "$lte": arr[1]}}
					}
				}
				if strings.Contains(s, ",") {
					parts := strings.SplitN(s, ",", 2)
					if len(parts) == 2 {
						a := strings.TrimSpace(parts[0])
						b := strings.TrimSpace(parts[1])
						return bsonV2.M{key: bsonV2.M{"$gte": a, "$lte": b}}
					}
				}
			}
			if len(values) == 2 {
				return bsonV2.M{key: bsonV2.M{"$gte": values[0], "$lte": values[1]}}
			}
			// fallback: if value is provided but not parsed above, use raw value
			if s, ok := toStr(value); ok && s != "" {
				return bsonV2.M{key: s}
			}
			return nil
		case pagination.Operator_IS_NULL:
			return bsonV2.M{key: nil}
		case pagination.Operator_IS_NOT_NULL:
			return bsonV2.M{key: bsonV2.M{"$ne": nil}}
		case pagination.Operator_CONTAINS:
			if s, ok := toStr(value); !ok || strings.TrimSpace(s) == "" {
				return nil
			} else {
				return bsonV2.M{key: bsonV2.M{"$regex": s}}
			}
		case pagination.Operator_ICONTAINS:
			if s, ok := toStr(value); !ok || strings.TrimSpace(s) == "" {
				return nil
			} else {
				return bsonV2.M{key: bsonV2.M{"$regex": s, "$options": "i"}}
			}
		case pagination.Operator_STARTS_WITH:
			if s, ok := toStr(value); !ok || strings.TrimSpace(s) == "" {
				return nil
			} else {
				return bsonV2.M{key: bsonV2.M{"$regex": "^" + s}}
			}
		case pagination.Operator_ISTARTS_WITH:
			if s, ok := toStr(value); !ok || strings.TrimSpace(s) == "" {
				return nil
			} else {
				return bsonV2.M{key: bsonV2.M{"$regex": "^" + s, "$options": "i"}}
			}
		case pagination.Operator_ENDS_WITH:
			if s, ok := toStr(value); !ok || strings.TrimSpace(s) == "" {
				return nil
			} else {
				return bsonV2.M{key: bsonV2.M{"$regex": s + "$"}}
			}
		case pagination.Operator_IENDS_WITH:
			if s, ok := toStr(value); !ok || strings.TrimSpace(s) == "" {
				return nil
			} else {
				return bsonV2.M{key: bsonV2.M{"$regex": s + "$", "$options": "i"}}
			}
		case pagination.Operator_EXACT:
			return bsonV2.M{key: value}
		case pagination.Operator_IEXACT:
			if s, ok := toStr(value); !ok || strings.TrimSpace(s) == "" {
				return nil
			} else {
				return bsonV2.M{key: bsonV2.M{"$regex": "^" + s + "$", "$options": "i"}}
			}
		case pagination.Operator_REGEXP:
			if s, ok := toStr(value); !ok || strings.TrimSpace(s) == "" {
				return nil
			} else {
				return bsonV2.M{key: bsonV2.M{"$regex": s}}
			}
		case pagination.Operator_IREGEXP:
			if s, ok := toStr(value); !ok || strings.TrimSpace(s) == "" {
				return nil
			} else {
				return bsonV2.M{key: bsonV2.M{"$regex": s, "$options": "i"}}
			}
		case pagination.Operator_SEARCH:
			if s, ok := toStr(value); !ok || strings.TrimSpace(s) == "" {
				return nil
			} else {
				return bsonV2.M{key: bsonV2.M{"$regex": s}}
			}
		default:
			return nil
		}
	}

	// handle AND filters
	if strings.TrimSpace(andFilterJsonString) != "" {
		maps, err := unmarshalToMaps(andFilterJsonString)
		if err != nil {
			return builder, err
		}
		for _, qm := range maps {
			for k, v := range qm {
				keys := strings.Split(k, QueryDelimiter)
				if len(keys) == 0 {
					continue
				}
				field := keys[0]
				if strings.TrimSpace(field) == "" {
					continue
				}
				not := false
				var op pagination.Operator
				var ok bool
				if len(keys) == 1 {
					op = pagination.Operator_EQ
					ok = true
				} else {
					op, ok = opFromStr(keys[1])
				}
				if !ok {
					continue
				}
				if len(keys) == 3 && strings.ToLower(keys[2]) == "not" {
					not = true
				}
				cond := buildCondition(op, field, v, nil)
				if cond == nil {
					continue
				}
				if not {
					// wrap as $not by using $nor for single condition: {$nor: [cond]}
					builder.SetFilter(bsonV2.M{"$nor": bsonV2.A{cond}})
				} else {
					builder.SetFilter(cond)
				}
			}
		}
	}

	// handle OR filters
	if strings.TrimSpace(orFilterJsonString) != "" {
		maps, err := unmarshalToMaps(orFilterJsonString)
		if err != nil {
			return builder, err
		}
		for _, qm := range maps {
			var orParts bsonV2.A
			for k, v := range qm {
				keys := strings.Split(k, QueryDelimiter)
				if len(keys) == 0 {
					continue
				}
				field := keys[0]
				if strings.TrimSpace(field) == "" {
					continue
				}
				not := false
				var op pagination.Operator
				var ok bool
				if len(keys) == 1 {
					op = pagination.Operator_EQ
					ok = true
				} else {
					op, ok = opFromStr(keys[1])
				}
				if !ok {
					continue
				}
				if len(keys) == 3 && strings.ToLower(keys[2]) == "not" {
					not = true
				}
				cond := buildCondition(op, field, v, nil)
				if cond == nil {
					continue
				}
				if not {
					// represent NOT by using $nor with single element
					orParts = append(orParts, bsonV2.M{"$nor": bsonV2.A{cond}})
				} else {
					orParts = append(orParts, cond)
				}
			}
			if len(orParts) > 0 {
				builder.SetFilter(bsonV2.M{"$or": orParts})
			}
		}
	}

	return builder, nil
}

// opFromStr 将字符串映射为 pagination.Operator（不区分大小写）。
// 返回对应的 operator 及是否匹配成功。
func opFromStr(s string) (pagination.Operator, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "eq", "equal", "equals":
		return pagination.Operator_EQ, true
	case "neq", "ne", "not_equal", "not-equal":
		return pagination.Operator_NEQ, true
	case "in":
		return pagination.Operator_IN, true
	case "nin", "not_in", "not-in":
		return pagination.Operator_NIN, true
	case "gte", "ge", "greater_or_equal", "greater-equal":
		return pagination.Operator_GTE, true
	case "gt", "greater":
		return pagination.Operator_GT, true
	case "lte", "le", "less_or_equal", "less-equal":
		return pagination.Operator_LTE, true
	case "lt", "less":
		return pagination.Operator_LT, true
	case "between", "range":
		return pagination.Operator_BETWEEN, true
	case "is_null", "null":
		return pagination.Operator_IS_NULL, true
	case "is_not_null", "not_null", "notnull":
		return pagination.Operator_IS_NOT_NULL, true
	case "contains":
		return pagination.Operator_CONTAINS, true
	case "icontains", "i_contains", "contains_i":
		return pagination.Operator_ICONTAINS, true
	case "starts_with", "startswith":
		return pagination.Operator_STARTS_WITH, true
	case "istarts_with", "istartswith":
		return pagination.Operator_ISTARTS_WITH, true
	case "ends_with", "endswith":
		return pagination.Operator_ENDS_WITH, true
	case "iends_with", "iendswith":
		return pagination.Operator_IENDS_WITH, true
	case "exact":
		return pagination.Operator_EXACT, true
	case "iexact":
		return pagination.Operator_IEXACT, true
	case "regexp":
		return pagination.Operator_REGEXP, true
	case "iregexp":
		return pagination.Operator_IREGEXP, true
	case "search":
		return pagination.Operator_SEARCH, true
	default:
		return pagination.Operator(0), false
	}
}
