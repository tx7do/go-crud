package pagination

import (
	"encoding/json"
	"fmt"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

// AnyToStructValue 将任意值转换为 structpb.Value（nil 安全）
func AnyToStructValue(v any) *structpb.Value {
	sv, err := structpb.NewValue(v)
	if err != nil {
		return nil
	}
	return sv
}

// StructValueToString 将 structpb.Value 转换为字符串表现形式
func StructValueToString(sv *structpb.Value) string {
	if sv == nil {
		return ""
	}

	switch k := sv.Kind.(type) {
	case *structpb.Value_StringValue:
		// 直接返回纯字符串
		return k.StringValue
	case *structpb.Value_NumberValue:
		return fmt.Sprintf("%v", k.NumberValue)
	case *structpb.Value_BoolValue:
		return fmt.Sprintf("%v", k.BoolValue)
	case *structpb.Value_NullValue:
		return ""
	case *structpb.Value_ListValue, *structpb.Value_StructValue:
		// 将数组/对象序列化为 JSON 字符串（紧凑形式）
		if v := sv.AsInterface(); v != nil {
			if b, err := json.Marshal(v); err == nil {
				return string(b)
			}
		}
		// 回退到 protobuf 的字符串表现
		return sv.String()
	default:
		return sv.String()
	}
}

// AnyToString 将任意值转换为 *string（nil 安全）
func AnyToString(v any) string {
	if v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		s := t
		return s
	case *string:
		return *t
	case fmt.Stringer:
		s := t.String()
		return s
	case []byte:
		s := string(t)
		return s
	default:
		// 对于数字、bool 等使用 fmt.Sprintf 回退
		s := fmt.Sprintf("%v", t)
		return s
	}
}

// RemoveExcludedConditions 从 filterExpr 中移除指定的字段条件（就地修改），
// 并返回被移除的条件列表。
func RemoveExcludedConditions(filterExpr *paginationV1.FilterExpr, excludeFields []string) []*paginationV1.FilterCondition {
	if filterExpr == nil || len(filterExpr.Conditions) == 0 {
		return []*paginationV1.FilterCondition{}
	}

	exclude := make(map[string]struct{}, len(excludeFields))
	for _, f := range excludeFields {
		if f == "" {
			continue
		}
		exclude[f] = struct{}{}
	}

	includeConditions := make([]*paginationV1.FilterCondition, 0, len(filterExpr.Conditions))
	excludeConditions := make([]*paginationV1.FilterCondition, 0, len(filterExpr.Conditions))
	for _, cond := range filterExpr.Conditions {
		if cond == nil || cond.Field == "" {
			continue
		}
		if _, skip := exclude[cond.Field]; skip {
			excludeConditions = append(excludeConditions, cond)
			continue
		}
		includeConditions = append(includeConditions, cond)
	}

	filterExpr.Conditions = includeConditions

	return excludeConditions
}

// ClearFilterExprByFieldNames 从 FilterExpr 中移除指定字段名的所有条件（就地修改）
func ClearFilterExprByFieldNames(expr *paginationV1.FilterExpr, fieldName string) {
	if expr == nil {
		return
	}

	for i := len(expr.GetConditions()) - 1; i >= 0; i-- {
		cond := expr.GetConditions()[i]
		if cond.GetField() == fieldName {
			expr.Conditions = append(expr.Conditions[:i], expr.Conditions[i+1:]...)
		}
	}

	for _, subExpr := range expr.GetGroups() {
		ClearFilterExprByFieldNames(subExpr, fieldName)
	}
}
