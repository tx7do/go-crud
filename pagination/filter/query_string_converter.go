package filter

import (
	"fmt"
	"strings"

	"github.com/go-kratos/kratos/v2/encoding"
	_ "github.com/go-kratos/kratos/v2/encoding/json"
	"github.com/tx7do/go-utils/stringcase"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/pagination"
)

const (
	QueryDelimiter     = "__" // 分隔符
	JsonFieldDelimiter = "."  // JSON字段分隔符
)

type QueryMap map[string]any
type QueryMapArray []QueryMap

type QueryStringConverter struct {
	codec encoding.Codec
}

func NewQueryStringConverter() *QueryStringConverter {
	return &QueryStringConverter{
		codec: encoding.GetCodec("json"),
	}
}

// QueryStringToMap 将查询字符串转换为 map
func (qsc *QueryStringConverter) QueryStringToMap(queryString string) (QueryMapArray, error) {
	if queryString == "" {
		return nil, nil
	}

	var obj QueryMap
	errObj := qsc.codec.Unmarshal([]byte(queryString), &obj)
	if errObj == nil {
		return QueryMapArray{obj}, nil
	}

	var arr QueryMapArray
	errArr := qsc.codec.Unmarshal([]byte(queryString), &arr)
	if errArr == nil {
		return arr, nil
	}

	return nil, fmt.Errorf("parse as object failed: %v; parse as array failed: %v", errObj, errArr)
}

func (qsc *QueryStringConverter) Convert(andQueryString, orQueryString string) (*paginationV1.FilterExpr, error) {
	if len(andQueryString) == 0 && len(orQueryString) == 0 {
		return nil, nil
	}

	addQueryMapArray, err := qsc.QueryStringToMap(andQueryString)
	if err != nil {
		return nil, err
	}

	orQueryMapArray, err := qsc.QueryStringToMap(orQueryString)
	if err != nil {
		return nil, err
	}

	var filterExpr *paginationV1.FilterExpr
	filterExpr = &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
	}

	for _, queryMap := range addQueryMapArray {
		err = qsc.processQueryMap(filterExpr, queryMap, false)
		if err != nil {
			return nil, err
		}
	}

	for _, queryMap := range orQueryMapArray {
		err = qsc.processQueryMap(filterExpr, queryMap, true)
		if err != nil {
			return nil, err
		}
	}

	return filterExpr, nil
}

// processQueryMap 处理查询映射表
func (qsc *QueryStringConverter) processQueryMap(filterExpr *paginationV1.FilterExpr, queryMap QueryMap, isOr bool) error {
	if len(queryMap) == 0 {
		return nil
	}

	if isOr {
		orFilterExpr := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_OR,
		}

		for k, v := range queryMap {
			keys := qsc.splitQueryKey(k)
			if err := qsc.MakeFieldFilter(orFilterExpr, keys, v); err != nil {
				return err
			}
		}

		// 仅在 OR 组中有实际条件或子组时追加
		if len(orFilterExpr.Conditions) > 0 || len(orFilterExpr.Groups) > 0 {
			filterExpr.Groups = append(filterExpr.Groups, orFilterExpr)
		}
		return nil
	}

	for k, v := range queryMap {
		keys := qsc.splitQueryKey(k)
		if err := qsc.MakeFieldFilter(filterExpr, keys, v); err != nil {
			return err
		}
	}

	return nil
}

func (qsc *QueryStringConverter) addCondition(filterExpr *paginationV1.FilterExpr, op paginationV1.Operator, field string, value any) {
	filterExpr.Conditions = append(filterExpr.Conditions, &paginationV1.FilterCondition{
		Field:      field,
		Op:         op,
		ValueOneof: &paginationV1.FilterCondition_Value{Value: pagination.AnyToString(value)},
	})
}

func (qsc *QueryStringConverter) addJsonCondition(filterExpr *paginationV1.FilterExpr, op paginationV1.Operator, field, jsonPath string, value any) {
	filterExpr.Conditions = append(filterExpr.Conditions, &paginationV1.FilterCondition{
		Field:      field,
		Op:         op,
		JsonPath:   &jsonPath,
		ValueOneof: &paginationV1.FilterCondition_JsonValue{JsonValue: pagination.AnyToStructValue(value)},
	})
}

func (qsc *QueryStringConverter) addDatePartCondition(filterExpr *paginationV1.FilterExpr, op paginationV1.Operator, datePart paginationV1.DatePart, field string, value any) {
	filterExpr.Conditions = append(filterExpr.Conditions, &paginationV1.FilterCondition{
		Field:      field,
		Op:         op,
		DatePart:   &datePart,
		ValueOneof: &paginationV1.FilterCondition_Value{Value: pagination.AnyToString(value)},
	})
}

func (qsc *QueryStringConverter) Equal(filterExpr *paginationV1.FilterExpr, field string, value any) {
	qsc.addCondition(filterExpr, paginationV1.Operator_EQ, field, value)
}

// MakeFieldFilter 构建一个字段过滤器
func (qsc *QueryStringConverter) MakeFieldFilter(filterExpr *paginationV1.FilterExpr, keys []string, value any) error {
	if len(keys) == 0 {
		return nil
	}

	field := keys[0]
	if len(field) == 0 {
		return nil
	}

	switch len(keys) {
	case 1:
		// "amount": "500"
		field = stringcase.ToSnakeCase(field)
		qsc.Equal(filterExpr, field, value)
		return nil

	case 2:
		// "amount__lt": "500"

		op := keys[1]
		if len(op) == 0 {
			return nil
		}

		operator := pagination.ConverterStringToOperator(op)

		filterCondition := &paginationV1.FilterCondition{}

		if qsc.isJsonFieldKey(field) {
			jsonFields := qsc.splitJsonFieldKey(field)
			if len(jsonFields) == 2 {
				filterCondition.Field = jsonFields[0]
				filterCondition.JsonPath = &jsonFields[1]
				filterCondition.ValueOneof = &paginationV1.FilterCondition_JsonValue{JsonValue: pagination.AnyToStructValue(value)}
			} else {
				field = stringcase.ToSnakeCase(field)
				filterCondition.Field = field
				filterCondition.ValueOneof = &paginationV1.FilterCondition_Value{Value: pagination.AnyToString(value)}
			}
		} else {
			field = stringcase.ToSnakeCase(field)
			filterCondition.Field = field
			filterCondition.ValueOneof = &paginationV1.FilterCondition_Value{Value: pagination.AnyToString(value)}
		}

		filterCondition.Op = operator
		filterExpr.Conditions = append(filterExpr.Conditions, filterCondition)
		return nil

	case 3:
		// "created_at__date__eq": "2023-10-01"
		// "dept.name__contains": "技术部"

		op1 := keys[1]
		if len(op1) == 0 {
			return nil
		}

		op2 := keys[2]
		if len(op2) == 0 {
			return nil
		}

		// 第二个参数，要么是提取日期，要么是json字段。

		field = stringcase.ToSnakeCase(field)

		filterCondition := &paginationV1.FilterCondition{}

		//var cond *sql.Predicate
		if qsc.hasDatePart(op1) {
			if qsc.isJsonFieldKey(field) {
				jsonFields := qsc.splitJsonFieldKey(field)
				if len(jsonFields) == 2 {
					filterCondition.Field = jsonFields[0]
					filterCondition.JsonPath = &jsonFields[1]
					filterCondition.ValueOneof = &paginationV1.FilterCondition_JsonValue{JsonValue: pagination.AnyToStructValue(value)}
				} else {
					filterCondition.Field = field
					filterCondition.ValueOneof = &paginationV1.FilterCondition_Value{Value: pagination.AnyToString(value)}
				}
			} else {
				filterCondition.Field = field
				filterCondition.ValueOneof = &paginationV1.FilterCondition_Value{Value: pagination.AnyToString(value)}
			}

			filterCondition.DatePart = pagination.ConverterStringToDatePart(op1)

			if qsc.hasOperations(op2) {
				operator := pagination.ConverterStringToOperator(op2)
				filterCondition.Op = operator
				filterExpr.Conditions = append(filterExpr.Conditions, filterCondition)
				return nil
			}

			return nil
		} else {
			// JSON字段
			if qsc.isJsonFieldKey(field) {
				jsonFields := qsc.splitJsonFieldKey(field)
				if len(jsonFields) == 2 {
					filterCondition.Field = jsonFields[0]
					filterCondition.JsonPath = &jsonFields[1]
					filterCondition.ValueOneof = &paginationV1.FilterCondition_JsonValue{JsonValue: pagination.AnyToStructValue(value)}
				} else {
					filterCondition.Field = field
					filterCondition.ValueOneof = &paginationV1.FilterCondition_Value{Value: pagination.AnyToString(value)}
				}
			} else {
				filterCondition.Field = field
				filterCondition.ValueOneof = &paginationV1.FilterCondition_Value{Value: pagination.AnyToString(value)}
			}

			if qsc.hasOperations(op2) {
				operator := pagination.ConverterStringToOperator(op2)
				filterCondition.Op = operator
				filterExpr.Conditions = append(filterExpr.Conditions, filterCondition)
				return nil
			}

			return nil
		}

	default:
		return nil
	}
}

// splitQueryKey 分割查询键
func (qsc *QueryStringConverter) splitQueryKey(key string) []string {
	return strings.Split(key, QueryDelimiter)
}

// splitJsonFieldKey 分割JSON字段键
func (qsc *QueryStringConverter) splitJsonFieldKey(key string) []string {
	return strings.Split(key, JsonFieldDelimiter)
}

// isJsonFieldKey 是否为JSON字段键
func (qsc *QueryStringConverter) isJsonFieldKey(key string) bool {
	return strings.Contains(key, JsonFieldDelimiter)
}

// hasOperations 是否有操作
func (qsc *QueryStringConverter) hasOperations(str string) bool {
	str = strings.ToLower(str)
	return pagination.IsValidOperatorString(str)
}

// hasDatePart 是否有日期部分
func (qsc *QueryStringConverter) hasDatePart(str string) bool {
	str = strings.ToLower(str)
	return pagination.IsValidDatePartString(str)
}
