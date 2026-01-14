package filter

import (
	"encoding/json"
	"fmt"
	"strings"

	"gorm.io/gorm"

	"github.com/go-kratos/kratos/v2/encoding"
	_ "github.com/go-kratos/kratos/v2/encoding/json"

	"github.com/tx7do/go-utils/stringcase"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/pagination"
)

const (
	QueryDelimiter     = "__" // 分隔符
	JsonFieldDelimiter = "."  // JSONB字段分隔符
)

// QueryStringFilter 字符串过滤器 (GORM 版)
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

// BuildSelectors 构建可应用于 *gorm.DB 的过滤闭包 slice
func (sf QueryStringFilter) BuildSelectors(andFilterJsonString, orFilterJsonString string) ([]func(*gorm.DB) *gorm.DB, error) {
	var selectors []func(*gorm.DB) *gorm.DB

	if andFilterJsonString != "" {
		andSel, err := sf.QueryCommandToWhereConditions(andFilterJsonString, false)
		if err != nil {
			return nil, err
		}
		if andSel != nil {
			selectors = append(selectors, andSel)
		}
	}

	if orFilterJsonString != "" {
		orSel, err := sf.QueryCommandToWhereConditions(orFilterJsonString, true)
		if err != nil {
			return nil, err
		}
		if orSel != nil {
			selectors = append(selectors, orSel)
		}
	}

	return selectors, nil
}

// QueryCommandToWhereConditions 将 JSON 字符串解析为可应用于 *gorm.DB 的闭包。
// isOr 为 true 时在闭包内部以 OR 方式组合子条件。
func (sf QueryStringFilter) QueryCommandToWhereConditions(strJson string, isOr bool) (func(*gorm.DB) *gorm.DB, error) {
	if strings.TrimSpace(strJson) == "" {
		return nil, nil
	}

	// 支持两种结构：map[string]string 或 []map[string]string
	var single map[string]string
	if err := sf.codec.Unmarshal([]byte(strJson), &single); err == nil {
		return sf.makeClosureFromMaps([]map[string]string{single}, isOr), nil
	}

	var arr []map[string]string
	if err := sf.codec.Unmarshal([]byte(strJson), &arr); err == nil {
		return sf.makeClosureFromMaps(arr, isOr), nil
	}

	// 尝试标准 json.Unmarshal 作为 fallback
	if err := json.Unmarshal([]byte(strJson), &single); err == nil {
		return sf.makeClosureFromMaps([]map[string]string{single}, isOr), nil
	}
	if err := json.Unmarshal([]byte(strJson), &arr); err == nil {
		return sf.makeClosureFromMaps(arr, isOr), nil
	}

	return nil, fmt.Errorf("invalid filter json")
}

// makeClosureFromMaps 将一组 map (每个 map 为一个 AND 组) 转换为闭包。
// 当 isOr == false：闭包在 DB 上依次应用所有 key/value (AND)。
// 当 isOr == true：闭包会将每个单独的 key/value 作为 OR 条件加入。
func (sf QueryStringFilter) makeClosureFromMaps(maps []map[string]string, isOr bool) func(*gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if db == nil {
			return db
		}

		// 每个 map 表示一组 AND 条件；如果 maps 长度 >1，按顺序合并这些组（每组内部为 AND）
		for _, qm := range maps {
			for k, v := range qm {
				keys := sf.splitQueryKey(k)
				if fn := sf.MakeFieldFilter(keys, v); fn != nil {
					if isOr {
						// 将这个单一条件作为 OR 子句加入
						db = db.Or(func(tx *gorm.DB) *gorm.DB {
							return fn(tx)
						})
					} else {
						db = fn(db)
					}
				}
			}
		}
		return db
	}
}

// MakeFieldFilter 将 keys/value 转为对 *gorm.DB 的单一应用闭包（返回 nil 表示跳过）
func (sf QueryStringFilter) MakeFieldFilter(keys []string, value string) func(*gorm.DB) *gorm.DB {
	if len(keys) == 0 || strings.TrimSpace(value) == "" {
		return nil
	}

	field := keys[0]
	if strings.TrimSpace(field) == "" {
		return nil
	}

	// helper: 把操作字符串映射为 paginationV1.Operator
	opFromStr := func(s string) (paginationV1.Operator, bool) {
		switch strings.ToLower(s) {
		case "eq", "equals", "exact":
			return paginationV1.Operator_EQ, true
		case "neq", "ne", "not":
			return paginationV1.Operator_NEQ, true
		case "in":
			return paginationV1.Operator_IN, true
		case "nin", "not_in":
			return paginationV1.Operator_NIN, true
		case "gte":
			return paginationV1.Operator_GTE, true
		case "gt":
			return paginationV1.Operator_GT, true
		case "lte":
			return paginationV1.Operator_LTE, true
		case "lt":
			return paginationV1.Operator_LT, true
		case "between":
			return paginationV1.Operator_BETWEEN, true
		case "is_null":
			return paginationV1.Operator_IS_NULL, true
		case "is_not_null":
			return paginationV1.Operator_IS_NOT_NULL, true
		case "contains":
			return paginationV1.Operator_CONTAINS, true
		case "icontains", "i_contains":
			return paginationV1.Operator_ICONTAINS, true
		case "starts_with":
			return paginationV1.Operator_STARTS_WITH, true
		case "istarts_with", "i_starts_with":
			return paginationV1.Operator_ISTARTS_WITH, true
		case "ends_with":
			return paginationV1.Operator_ENDS_WITH, true
		case "iends_with", "i_ends_with":
			return paginationV1.Operator_IENDS_WITH, true
		case "iexact", "i_exact":
			return paginationV1.Operator_IEXACT, true
		case "regexp":
			return paginationV1.Operator_REGEXP, true
		case "iregexp":
			return paginationV1.Operator_IREGEXP, true
		case "search":
			return paginationV1.Operator_SEARCH, true
		default:
			return 0, false
		}
	}

	// JSON 字段处理： "meta.title" -> column meta, json key title
	handleJson := func(col string, jsonKey string, op paginationV1.Operator) func(*gorm.DB) *gorm.DB {
		return func(db *gorm.DB) *gorm.DB {
			if db == nil {
				return db
			}
			// 先尝试构造表达式
			expr, _ := sf.processor.JsonbFieldExpr(db, jsonKey, col)
			if expr == "" {
				return db
			}
			// 使用 Processor 统一构建条件（Processor 支持普通字段表达式）
			return sf.processor.Process(db, op, expr, value, nil)
		}
	}

	// 普通字段处理器
	handleField := func(col string, op paginationV1.Operator) func(*gorm.DB) *gorm.DB {
		return func(db *gorm.DB) *gorm.DB {
			if db == nil {
				return db
			}
			return sf.processor.Process(db, op, col, value, nil)
		}
	}

	// 单字段（默认等于）
	if len(keys) == 1 {
		if sf.isJsonFieldKey(field) {
			parts := sf.splitJsonFieldKey(field)
			col := stringcase.ToSnakeCase(parts[0])
			jsonKey := strings.Join(parts[1:], ".")
			return handleJson(col, jsonKey, paginationV1.Operator_EQ)
		}
		col := stringcase.ToSnakeCase(field)
		return handleField(col, paginationV1.Operator_EQ)
	}

	// 两段： field__op
	if len(keys) == 2 {
		opStr := keys[1]
		op, ok := opFromStr(opStr)
		if !ok {
			return nil
		}
		if sf.isJsonFieldKey(field) {
			parts := sf.splitJsonFieldKey(field)
			col := stringcase.ToSnakeCase(parts[0])
			jsonKey := strings.Join(parts[1:], ".")
			return handleJson(col, jsonKey, op)
		}
		col := stringcase.ToSnakeCase(field)
		return handleField(col, op)
	}

	// 三段： field__datePart__op 或 field__op__not 等（支持 date part + op）
	if len(keys) == 3 {
		op1 := strings.ToLower(keys[1])
		op2 := strings.ToLower(keys[2])

		// 如果 op1 看起来像日期部分
		if sf.hasDatePart(op1) {
			datePart := op1
			// op2 为比较操作
			op, ok := opFromStr(op2)
			if !ok {
				return nil
			}
			// 对 json 字段先提取 json expr 再按 date 部分处理
			if sf.isJsonFieldKey(field) {
				parts := sf.splitJsonFieldKey(field)
				col := stringcase.ToSnakeCase(parts[0])
				jsonKey := strings.Join(parts[1:], ".")
				return func(db *gorm.DB) *gorm.DB {
					if db == nil {
						return db
					}
					// 先得到 json expr
					expr, _ := sf.processor.JsonbFieldExpr(db, jsonKey, col)
					if expr == "" {
						return db
					}
					// 构建 DatePart 子句并再应用比较
					db = sf.processor.DatePart(db, datePart, expr)
					// DatePart 只是构造了部分表达式，接下来用 Process 做比较（将 field 设为 expr）
					return sf.processor.Process(db, op, expr, value, nil)
				}
			}
			// 普通字段
			col := stringcase.ToSnakeCase(field)
			return func(db *gorm.DB) *gorm.DB {
				if db == nil {
					return db
				}
				db = sf.processor.DatePart(db, datePart, col)
				return sf.processor.Process(db, op, col, value, nil)
			}
		}

		// 否则 treat as other combinations (e.g. json + op + not) - try interpret op2
		// 支持 field.json__op__not -> 取 op，再用 Not 逻辑
		// For simplicity, if op2 == "not" treat as negation of op1
		if strings.ToLower(keys[2]) == "not" {
			op, ok := opFromStr(op1)
			if !ok {
				return nil
			}
			// 使用 Processor.Process 然后 wrap 为 NOT 通过 gorm.Not
			if sf.isJsonFieldKey(field) {
				parts := sf.splitJsonFieldKey(field)
				col := stringcase.ToSnakeCase(parts[0])
				jsonKey := strings.Join(parts[1:], ".")
				return func(db *gorm.DB) *gorm.DB {
					if db == nil {
						return db
					}
					return db.Not(func(tx *gorm.DB) *gorm.DB {
						return handleJson(col, jsonKey, op)(tx)
					})
				}
			}
			col := stringcase.ToSnakeCase(field)
			return func(db *gorm.DB) *gorm.DB {
				if db == nil {
					return db
				}
				return db.Not(func(tx *gorm.DB) *gorm.DB {
					return handleField(col, op)(tx)
				})
			}
		}
	}

	// 不支持的模式
	return nil
}

// splitQueryKey 分割查询键
func (sf QueryStringFilter) splitQueryKey(key string) []string {
	return strings.Split(key, QueryDelimiter)
}

// splitJsonFieldKey 分割 JSON 字段键
func (sf QueryStringFilter) splitJsonFieldKey(key string) []string {
	return strings.Split(key, JsonFieldDelimiter)
}

// isJsonFieldKey 是否为 JSON 字段键
func (sf QueryStringFilter) isJsonFieldKey(key string) bool {
	return strings.Contains(key, JsonFieldDelimiter)
}

// hasOperations 是否有操作
func (sf QueryStringFilter) hasOperations(str string) bool {
	str = strings.ToLower(str)
	return pagination.IsValidOperatorString(str)
}

// hasDatePart 是否有日期部分
func (sf QueryStringFilter) hasDatePart(str string) bool {
	str = strings.ToLower(str)
	return pagination.IsValidDatePartString(str)
}
