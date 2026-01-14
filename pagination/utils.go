package pagination

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"
)

func AnyToStructValue(v any) *structpb.Value {
	sv, err := structpb.NewValue(v)
	if err != nil {
		return nil
	}
	return sv
}

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
