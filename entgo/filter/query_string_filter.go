package filter

import (
	"strings"

	"entgo.io/ent/dialect/sql"
	"github.com/tx7do/go-crud/pagination"

	"github.com/go-kratos/kratos/v2/encoding"
	_ "github.com/go-kratos/kratos/v2/encoding/json"
	"github.com/go-kratos/kratos/v2/log"

	"github.com/tx7do/go-utils/stringcase"
)

const (
	QueryDelimiter     = "__" // 分隔符
	JsonFieldDelimiter = "."  // JSONB字段分隔符
)

// QueryStringFilter 字符串过滤器,解析基于 `query` 字段的字符串过滤器.
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

// BuildSelectors 构建过滤选择器
func (sf QueryStringFilter) BuildSelectors(andFilterJsonString, orFilterJsonString string) ([]func(s *sql.Selector), error) {
	var err error
	var queryConditions []func(s *sql.Selector)

	var andSelector func(s *sql.Selector)
	andSelector, err = sf.QueryCommandToWhereConditions(andFilterJsonString, false)
	if err != nil {
		log.Errorf("Error in QueryCommandToWhereConditions: %v", err)
		return nil, err
	}
	if andSelector != nil {
		queryConditions = append(queryConditions, andSelector)
	}

	var orSelector func(s *sql.Selector)
	orSelector, err = sf.QueryCommandToWhereConditions(orFilterJsonString, true)
	if err != nil {
		log.Errorf("Error in QueryCommandToWhereConditions: %v", err)
		return nil, err
	}
	if orSelector != nil {
		queryConditions = append(queryConditions, orSelector)
	}

	return queryConditions, nil
}

// QueryCommandToWhereConditions 查询命令转换为选择条件
func (sf QueryStringFilter) QueryCommandToWhereConditions(strJson string, isOr bool) (func(s *sql.Selector), error) {
	if len(strJson) == 0 {
		return nil, nil
	}

	queryMap := make(map[string]string)
	var queryMapArray []map[string]string
	if err1 := sf.codec.Unmarshal([]byte(strJson), &queryMap); err1 != nil {
		if err2 := sf.codec.Unmarshal([]byte(strJson), &queryMapArray); err2 != nil {
			return nil, err2
		}
	}

	return func(s *sql.Selector) {
		var ps []*sql.Predicate
		ps = append(ps, sf.processQueryMap(s, queryMap)...)
		for _, v := range queryMapArray {
			ps = append(ps, sf.processQueryMap(s, v)...)
		}

		if isOr {
			s.Where(sql.Or(ps...))
		} else {
			s.Where(sql.And(ps...))
		}
	}, nil
}

// processQueryMap 处理查询映射表
func (sf QueryStringFilter) processQueryMap(s *sql.Selector, queryMap map[string]string) []*sql.Predicate {
	var ps []*sql.Predicate
	for k, v := range queryMap {
		keys := sf.splitQueryKey(k)

		if cond := sf.MakeFieldFilter(s, keys, v); cond != nil {
			ps = append(ps, cond)
		}
	}

	return ps
}

// MakeFieldFilter 构建一个字段过滤器
func (sf QueryStringFilter) MakeFieldFilter(s *sql.Selector, keys []string, value string) *sql.Predicate {
	if len(keys) == 0 {
		return nil
	}
	if len(value) == 0 {
		return nil
	}

	field := keys[0]
	if len(field) == 0 {
		return nil
	}

	p := sql.P()

	switch len(keys) {
	case 1:
		if sf.isJsonFieldKey(field) {
			jsonFields := sf.splitJsonFieldKey(field)
			if len(jsonFields) != 2 {
				field = stringcase.ToSnakeCase(field)
				return sf.processor.Equal(s, p, field, value)
			}
			//value = "'" + value + "'"
			return sf.processor.Jsonb(
				s, p,
				stringcase.ToSnakeCase(jsonFields[1]),
				stringcase.ToSnakeCase(jsonFields[0]),
			).
				EQ("", value)
		}
		field = stringcase.ToSnakeCase(field)
		return sf.processor.Equal(s, p, field, value)

	case 2:
		op := keys[1]
		if len(op) == 0 {
			return nil
		}

		if sf.isJsonFieldKey(field) {
			jsonFields := sf.splitJsonFieldKey(field)
			if len(jsonFields) == 2 {
				field = sf.processor.JsonbField(s,
					stringcase.ToSnakeCase(jsonFields[1]),
					stringcase.ToSnakeCase(jsonFields[0]),
				)
				//value = "'" + value + "'"
			}
		} else {
			field = stringcase.ToSnakeCase(field)
		}

		var cond *sql.Predicate
		if sf.hasOperations(op) {
			return sf.processor.Process(s, p, pagination.ConverterStringToOperator(op), field, value, nil)
		} else if sf.hasDatePart(op) {
			cond = sf.processor.DatePart(s, p, op, field).EQ("", value)
		} else {
			cond = sf.processor.Jsonb(s, p, op, field).EQ("", value)
		}

		return cond

	case 3:
		op1 := keys[1]
		if len(op1) == 0 {
			return nil
		}

		op2 := keys[2]
		if len(op2) == 0 {
			return nil
		}

		// 第二个参数，要么是提取日期，要么是json字段。

		//var cond *sql.Predicate
		if sf.hasDatePart(op1) {
			if sf.isJsonFieldKey(field) {
				jsonFields := sf.splitJsonFieldKey(field)
				if len(jsonFields) == 2 {
					field = sf.processor.JsonbField(s, jsonFields[1], jsonFields[0])
					//value = "'" + value + "'"
				}
			} else {
				field = stringcase.ToSnakeCase(field)
			}

			str := sf.processor.DatePartField(s, op1, field)

			if sf.hasOperations(op2) {
				return sf.processor.Process(s, p, pagination.ConverterStringToOperator(op2), str, value, nil)
			}

			return nil
		} else {
			str := sf.processor.JsonbField(s, op1, field)

			if sf.hasOperations(op2) {
				return sf.processor.Process(s, p, pagination.ConverterStringToOperator(op2), str, value, nil)
			} else if sf.hasDatePart(op2) {
				return sf.processor.DatePart(s, p, op2, str)
			}
			return nil
		}

	default:
		return nil
	}
}

// splitQueryKey 分割查询键
func (sf QueryStringFilter) splitQueryKey(key string) []string {
	return strings.Split(key, QueryDelimiter)
}

// splitJsonFieldKey 分割JSON字段键
func (sf QueryStringFilter) splitJsonFieldKey(key string) []string {
	return strings.Split(key, JsonFieldDelimiter)
}

// isJsonFieldKey 是否为JSON字段键
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
