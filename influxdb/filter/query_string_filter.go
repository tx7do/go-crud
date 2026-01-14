package filter

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-kratos/kratos/v2/encoding"
	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/influxdb/query"
)

const (
	QueryDelimiter     = "__" // 分隔符
	JsonFieldDelimiter = "."  // JSON 字段分隔符
)

// QueryStringFilter 字符串过滤器 (InfluxDB 版)，使用 Processor 构建 WHERE 子句
type QueryStringFilter struct {
	codec     encoding.Codec
	processor *Processor
}

// NewQueryStringFilter 创建 InfluxDB 用的 QueryStringFilter
func NewQueryStringFilter() *QueryStringFilter {
	return &QueryStringFilter{
		codec:     encoding.GetCodec("json"),
		processor: NewProcessor(),
	}
}

// BuildSelectors 将 and/or JSON 字符串解析并把过滤条件追加到 builder 中。
// 对于 andFilterJsonString：各 key => 条件以 AND 追加（直接多次调用 processor.Process）。
// 对于 orFilterJsonString：仅在每个或组为单个条件时直接处理；复杂或组合因 query.Builder 不支持 OR，暂不转换。
func (sf *QueryStringFilter) BuildSelectors(builder *query.Builder, andFilterJsonString, orFilterJsonString string) (*query.Builder, error) {
	if builder == nil {
		builder = query.NewQueryBuilder("m")
	}

	// parse helper
	unmarshalToMaps := func(strJson string) ([]map[string]string, error) {
		var arr []map[string]string
		if strings.TrimSpace(strJson) == "" {
			return nil, nil
		}
		// try codec into array
		if err := sf.codec.Unmarshal([]byte(strJson), &arr); err == nil {
			return arr, nil
		}
		// try codec into single map
		var single map[string]string
		if err := sf.codec.Unmarshal([]byte(strJson), &single); err == nil {
			return []map[string]string{single}, nil
		}
		// fallback to standard json
		if err := json.Unmarshal([]byte(strJson), &arr); err == nil {
			return arr, nil
		}
		if err := json.Unmarshal([]byte(strJson), &single); err == nil {
			return []map[string]string{single}, nil
		}
		return nil, fmt.Errorf("invalid filter json")
	}

	// helper to map "not" flag to counterpart operator where possible
	negateOp := func(op paginationV1.Operator) (paginationV1.Operator, bool) {
		switch op {
		case paginationV1.Operator_EQ:
			return paginationV1.Operator_NEQ, true
		case paginationV1.Operator_NEQ:
			return paginationV1.Operator_EQ, true
		case paginationV1.Operator_IN:
			return paginationV1.Operator_NIN, true
		case paginationV1.Operator_NIN:
			return paginationV1.Operator_IN, true
		default:
			// other operators: not supported via simple negation here
			return paginationV1.Operator(0), false
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
				var op paginationV1.Operator
				var ok bool
				if len(keys) == 1 {
					op = paginationV1.Operator_EQ
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
				if not {
					if neg, can := negateOp(op); can {
						op = neg
						// delegate to processor with negated op
						sf.processor.Process(builder, op, field, v, nil)
						continue
					}
					// unsupported negation for this op: skip
					continue
				}
				sf.processor.Process(builder, op, field, v, nil)
			}
		}
	}

	// handle OR filters (limited support)
	if strings.TrimSpace(orFilterJsonString) != "" {
		maps, err := unmarshalToMaps(orFilterJsonString)
		if err != nil {
			return builder, err
		}
		for _, qm := range maps {
			// If group contains exactly one condition, process it (treated as OR group of size 1).
			// Complex OR groups across multiple fields are not supported by query.Builder and are skipped.
			if len(qm) == 1 {
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
					var op paginationV1.Operator
					var ok bool
					if len(keys) == 1 {
						op = paginationV1.Operator_EQ
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
					if not {
						if neg, can := negateOp(op); can {
							op = neg
							sf.processor.Process(builder, op, field, v, nil)
							continue
						}
						// unsupported negation: skip
						continue
					}
					sf.processor.Process(builder, op, field, v, nil)
				}
			}
			// otherwise skip complex OR group (unsupported)
		}
	}

	return builder, nil
}

// opFromStr 将字符串表示的操作符转换为 paginationV1.Operator。
// 返回 (op, true) 表示识别成功，(0, false) 表示未知操作符。
func opFromStr(s string) (paginationV1.Operator, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	switch s {
	case "eq", "=", "equals":
		return paginationV1.Operator_EQ, true
	case "ne", "neq", "!=", "not_eq", "not-eq":
		return paginationV1.Operator_NEQ, true
	case "in":
		return paginationV1.Operator_IN, true
	case "nin", "notin", "not_in", "not-in":
		return paginationV1.Operator_NIN, true
	case "gte", ">=", "ge":
		return paginationV1.Operator_GTE, true
	case "gt", ">":
		return paginationV1.Operator_GT, true
	case "lte", "<=", "le":
		return paginationV1.Operator_LTE, true
	case "lt", "<":
		return paginationV1.Operator_LT, true
	case "between", "range":
		return paginationV1.Operator_BETWEEN, true
	case "is_null", "isnull", "null":
		return paginationV1.Operator_IS_NULL, true
	case "is_not_null", "isnotnull", "not_null", "notnull":
		return paginationV1.Operator_IS_NOT_NULL, true
	case "contains":
		return paginationV1.Operator_CONTAINS, true
	case "icontains", "i_contains", "contains_i":
		return paginationV1.Operator_ICONTAINS, true
	case "starts_with", "startswith":
		return paginationV1.Operator_STARTS_WITH, true
	case "istarts_with", "istartswith", "i_starts_with":
		return paginationV1.Operator_ISTARTS_WITH, true
	case "ends_with", "endswith":
		return paginationV1.Operator_ENDS_WITH, true
	case "iends_with", "iendswith", "i_ends_with":
		return paginationV1.Operator_IENDS_WITH, true
	case "exact":
		return paginationV1.Operator_EXACT, true
	case "iexact", "i_exact":
		return paginationV1.Operator_IEXACT, true
	case "regexp", "regex", "=~":
		return paginationV1.Operator_REGEXP, true
	case "iregexp", "iregex":
		return paginationV1.Operator_IREGEXP, true
	case "search":
		return paginationV1.Operator_SEARCH, true
	default:
		return paginationV1.Operator(0), false
	}
}
