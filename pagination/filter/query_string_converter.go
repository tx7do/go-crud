package filter

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/go-kratos/kratos/v2/encoding"
	_ "github.com/go-kratos/kratos/v2/encoding/json"
	"github.com/tx7do/go-utils/stringcase"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/pagination"
)

const (
	QueryDelimiter          = "__"   // 分隔符
	QueryJsonFieldDelimiter = "."    // JSON字段分隔符
	QueryAnd                = "$and" // 与
	QueryOr                 = "$or"  // 或
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

// Convert 将查询字符串转换为 FilterExpr
func (qsc *QueryStringConverter) Convert(queryJSON string) (*paginationV1.FilterExpr, error) {
	return qsc.ParseQuery(queryJSON)
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

// ParseQuery 入口函数：解析JSON格式的query字符串
// 支持两种顶层格式：
// 1. 数组：[{"deptId":1}, {"entryTime__gte":"2024-01-01"}] → 等价于$and
// 2. 对象：{"$and":[...], "$or":[...]}
func (qsc *QueryStringConverter) ParseQuery(queryJSON string) (*paginationV1.FilterExpr, error) {
	// 先将JSON字符串解析为any（兼容数组/对象）
	var raw any
	var err error
	if err = json.Unmarshal([]byte(queryJSON), &raw); err != nil {
		return nil, fmt.Errorf("json解析失败: %w", err)
	}

	root := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
	}

	// 递归解析核心逻辑
	if err = qsc.parseRawQuery(root, raw); err != nil {
		return nil, err
	}

	return root, nil
}

// parseRawQuery 递归解析原始的any类型query
func (qsc *QueryStringConverter) parseRawQuery(node *paginationV1.FilterExpr, raw any) error {
	switch v := raw.(type) {
	// 场景1：顶层是数组（默认AND逻辑）
	case []any:
		andFilterExpr := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
		}
		node.Groups = append(node.Groups, andFilterExpr)

		for _, item := range v {
			// 如果元素本身是数组，扁平化：把子元素直接解析到当前 andFilterExpr
			if subArr, ok := item.([]any); ok {
				for _, sub := range subArr {
					if err := qsc.parseRawQuery(andFilterExpr, sub); err != nil {
						return err
					}
				}
				continue
			}
			// 兼容 []interface{}
			if s, ok := item.([]interface{}); ok {
				for _, sub := range s {
					if err := qsc.parseRawQuery(andFilterExpr, sub); err != nil {
						return err
					}
				}
				continue
			}

			if err := qsc.parseRawQuery(andFilterExpr, item); err != nil {
				return err
			}
		}

		return nil

	// 场景2：顶层是对象（处理$and/$or，或基础条件）
	case map[string]any:
		// 先判断是否是逻辑节点（包含$and/$or）
		andNodes, hasAnd := v[QueryAnd]
		orNodes, hasOr := v[QueryOr]

		// 逻辑节点校验：一个对象只能有$and 或 $or，不能同时有
		if hasAnd && hasOr {
			return errors.New("单个逻辑节点不能同时包含$and和$or")
		}

		// 处理$and逻辑
		if hasAnd {
			andList, ok := andNodes.([]any)
			if !ok {
				// 兼容 []interface{}
				if s, ok2 := andNodes.([]interface{}); ok2 {
					andList = make([]any, len(s))
					for i := range s {
						andList[i] = s[i]
					}
				} else {
					return errors.New("$and的值必须是数组")
				}
			}

			andFilterExpr := &paginationV1.FilterExpr{
				Type: paginationV1.ExprType_AND,
			}
			node.Groups = append(node.Groups, andFilterExpr)

			for _, item := range andList {
				if err := qsc.parseRawQuery(andFilterExpr, item); err != nil {
					return err
				}
			}

			return nil
		}

		// 处理$or逻辑
		if hasOr {
			orList, ok := orNodes.([]any)
			if !ok {
				// 兼容 []interface{}
				if s, ok2 := orNodes.([]interface{}); ok2 {
					orList = make([]any, len(s))
					for i := range s {
						orList[i] = s[i]
					}
				} else {
					return errors.New("$or的值必须是数组")
				}
			}

			orFilterExpr := &paginationV1.FilterExpr{
				Type: paginationV1.ExprType_OR,
			}
			node.Groups = append(node.Groups, orFilterExpr)

			for _, item := range orList {
				if err := qsc.parseRawQuery(orFilterExpr, item); err != nil {
					return err
				}
			}

			return nil
		}

		// 不是逻辑节点 → 基础条件（如{"deptId":1}、{"entryTime__gte":"2024-01-01"}）
		return qsc.parseBaseCondition(node, v)

	// 非法类型
	default:
		return fmt.Errorf("不支持的query类型: %T", raw)
	}
}

// parseBaseCondition 解析基础条件对象（如{"deptId":1} → Condition{Field:"deptId", Op:"eq", Value:1}）
func (qsc *QueryStringConverter) parseBaseCondition(node *paginationV1.FilterExpr, conditionMap map[string]any) error {
	for k, v := range conditionMap {
		keys := qsc.splitQueryKey(k)
		if err := qsc.MakeFieldFilter(node, keys, v); err != nil {
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
	return strings.Split(key, QueryJsonFieldDelimiter)
}

// isJsonFieldKey 是否为JSON字段键
func (qsc *QueryStringConverter) isJsonFieldKey(key string) bool {
	return strings.Contains(key, QueryJsonFieldDelimiter)
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
