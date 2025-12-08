package filter

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-kratos/kratos/v2/encoding"
	_ "github.com/go-kratos/kratos/v2/encoding/json"

	pagination "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/clickhouse/query"
)

const (
	QueryDelimiter     = "__" // 分隔符
	JsonFieldDelimiter = "."  // JSONB字段分隔符
)

// QueryStringFilter 字符串过滤器 (ClickHouse 版)，使用 Processor 构建子表达式
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

func (sf *QueryStringFilter) BuildSelectors(builder *query.Builder, andFilterJsonString, orFilterJsonString string) (*query.Builder, error) {
	if builder == nil {
		builder = query.NewQueryBuilder("", nil)
	}

	var andBuilder *query.Builder
	if strings.TrimSpace(andFilterJsonString) != "" {
		andBuilder = query.NewQueryBuilder(builder.TableName(), builder.Logger())
		if _, err := sf.QueryCommandToWhereConditions(builder, andFilterJsonString, false); err != nil {
			return builder, err
		}
	}

	var orBuilder *query.Builder
	if strings.TrimSpace(orFilterJsonString) != "" {
		orBuilder = query.NewQueryBuilder(builder.TableName(), builder.Logger())
		if _, err := sf.QueryCommandToWhereConditions(builder, orFilterJsonString, true); err != nil {
			return builder, err
		}
	}

	if andBuilder != nil {
		sql, args := andBuilder.Build()
		where := extractWhere(sql)
		if strings.TrimSpace(where) != "" {
			if len(args) > 0 {
				builder.Where(where, args...)
			} else {
				builder.Where(where)
			}
		}
	}

	if orBuilder != nil {
		sql, args := orBuilder.Build()
		where := extractWhere(sql)
		if strings.TrimSpace(where) != "" {
			if len(args) > 0 {
				builder.Where(where, args...)
			} else {
				builder.Where(where)
			}
		}
	}

	return builder, nil
}

// QueryCommandToWhereConditions 将 JSON 字符串解析并把条件追加到 builder 中。
// isOr 为 true 时在每个 map 内部以 OR 方式合并子条件（不同 map 之间按顺序合并）。
func (sf *QueryStringFilter) QueryCommandToWhereConditions(builder *query.Builder, strJson string, isOr bool) (*query.Builder, error) {
	if strings.TrimSpace(strJson) == "" {
		return builder, nil
	}

	maps, err := sf.unmarshalToMaps(strJson)
	if err != nil {
		return nil, fmt.Errorf("invalid filter json: %w", err)
	}

	return sf.makeClosureFromMaps(builder, maps, isOr)
}

func (sf *QueryStringFilter) unmarshalToMaps(strJson string) ([]map[string]string, error) {
	var arr []map[string]string
	// try codec into array
	if err := sf.codec.Unmarshal([]byte(strJson), &arr); err == nil {
		return arr, nil
	}
	// try codec into single map
	var single map[string]string
	if err := sf.codec.Unmarshal([]byte(strJson), &single); err == nil {
		return []map[string]string{single}, nil
	}
	// fallback to standard json for array
	if err := json.Unmarshal([]byte(strJson), &arr); err == nil {
		return arr, nil
	}
	// fallback to standard json for single map
	if err := json.Unmarshal([]byte(strJson), &single); err == nil {
		return []map[string]string{single}, nil
	}
	return nil, fmt.Errorf("unmarshal failed")
}

// makeClosureFromMaps 将一组 map (每个 map 为一个组) 转换并直接追加到 builder。
// 使用 Processor 生成每个单条件表达式与参数，再按 AND/OR/NOT 规则组合。
func (sf *QueryStringFilter) makeClosureFromMaps(builder *query.Builder, maps []map[string]string, isOr bool) (*query.Builder, error) {
	if builder == nil {
		return nil, fmt.Errorf("builder is nil")
	}

	// helper: 使用 Processor 在临时 builder 上生成子表达式与 args（从临时 SQL 中提取 WHERE 部分）
	buildWithProcessor := func(field string, op pagination.Operator, val string, vals []string) (string, []interface{}) {
		tmp := query.NewQueryBuilder("", nil)
		sf.processor.Process(tmp, op, field, val, vals)
		sql, args := tmp.Build()
		where := extractWhere(sql)
		if strings.TrimSpace(where) == "" {
			return "", nil
		}
		return strings.TrimSpace(where), args
	}

	for _, qm := range maps {
		if isOr {
			var orParts []string
			var orArgs []interface{}
			for k, v := range qm {
				keys := sf.splitQueryKey(k)
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

				clause, args := buildWithProcessor(field, op, v, nil)
				if clause == "" {
					continue
				}
				if not {
					clause = fmt.Sprintf("NOT (%s)", clause)
				}
				if !strings.HasPrefix(clause, "(") && !strings.HasSuffix(clause, ")") {
					clause = "(" + clause + ")"
				}
				orParts = append(orParts, clause)
				if len(args) > 0 {
					orArgs = append(orArgs, args...)
				}
			}
			if len(orParts) > 0 {
				combined := "(" + strings.Join(orParts, " OR ") + ")"
				if len(orArgs) > 0 {
					builder.Where(combined, orArgs...)
				} else {
					builder.Where(combined)
				}
			}
		} else {
			// AND 模式：每个 key/value 生成一个子表达式并直接追加（支持 NOT）
			for k, v := range qm {
				keys := sf.splitQueryKey(k)
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

				clause, args := buildWithProcessor(field, op, v, nil)
				if clause == "" {
					continue
				}
				if not {
					clause = fmt.Sprintf("NOT (%s)", clause)
				}
				if len(args) > 0 {
					builder.Where(clause, args...)
				} else {
					builder.Where(clause)
				}
			}
		}
	}

	return builder, nil
}

// extractWhere 从完整 SQL 中提取 WHERE 子句（去掉后续的 ORDER/LIMIT/GROUP 等）
func extractWhere(sql string) string {
	up := strings.ToUpper(sql)
	idx := strings.Index(up, "WHERE")
	if idx == -1 {
		return ""
	}
	start := idx + len("WHERE")
	restUp := up[start:]
	rest := sql[start:]

	// 寻找下一个可能的子句关键字
	candidates := []string{"ORDER BY", "LIMIT", "GROUP BY", "HAVING", "FORMAT"}
	end := len(rest)
	for _, kw := range candidates {
		pos := strings.Index(restUp, kw)
		if pos >= 0 && pos < end {
			end = pos
		}
	}
	return strings.TrimSpace(rest[:end])
}

// splitQueryKey 分割查询键
func (sf *QueryStringFilter) splitQueryKey(key string) []string {
	return strings.Split(key, QueryDelimiter)
}

// splitJsonFieldKey 分割 JSON 字段键
func (sf *QueryStringFilter) splitJsonFieldKey(key string) []string {
	return strings.Split(key, JsonFieldDelimiter)
}

// isJsonFieldKey 是否为 JSON 字段键
func (sf *QueryStringFilter) isJsonFieldKey(key string) bool {
	return strings.Contains(key, JsonFieldDelimiter)
}

func opFromStr(s string) (pagination.Operator, bool) {
	switch strings.ToLower(s) {
	case "eq", "equals", "exact":
		return pagination.Operator_EQ, true
	case "neq", "ne", "not", "not_eq":
		return pagination.Operator_NEQ, true
	case "in":
		return pagination.Operator_IN, true
	case "nin", "not_in":
		return pagination.Operator_NIN, true
	case "gte":
		return pagination.Operator_GTE, true
	case "gt":
		return pagination.Operator_GT, true
	case "lte":
		return pagination.Operator_LTE, true
	case "lt":
		return pagination.Operator_LT, true
	case "between":
		return pagination.Operator_BETWEEN, true
	case "is_null", "isnull":
		return pagination.Operator_IS_NULL, true
	case "is_not_null", "isnotnull":
		return pagination.Operator_IS_NOT_NULL, true
	case "contains", "like":
		return pagination.Operator_CONTAINS, true
	case "icontains", "i_contains":
		return pagination.Operator_ICONTAINS, true
	case "starts_with":
		return pagination.Operator_STARTS_WITH, true
	case "istarts_with", "i_starts_with":
		return pagination.Operator_ISTARTS_WITH, true
	case "ends_with":
		return pagination.Operator_ENDS_WITH, true
	case "iends_with", "i_ends_with":
		return pagination.Operator_IENDS_WITH, true
	case "iexact", "i_exact":
		return pagination.Operator_IEXACT, true
	case "regexp":
		return pagination.Operator_REGEXP, true
	case "iregexp":
		return pagination.Operator_IREGEXP, true
	case "search":
		return pagination.Operator_SEARCH, true
	default:
		return pagination.Operator_EQ, false
	}
}
