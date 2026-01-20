package rule

import (
	"reflect"

	"entgo.io/ent"
)

// GetClientFromMutation 通过反射从 m 上调用 Client 方法。
// 返回 (client, true) 表示成功获取；否则返回 (nil, false)。
func GetClientFromMutation(m ent.Mutation) (any, bool) {
	if m == nil {
		return nil, false
	}
	rv := reflect.ValueOf(m)
	mf := rv.MethodByName("Client")
	if !mf.IsValid() || mf.Kind() != reflect.Func {
		return nil, false
	}
	// 确保无入参
	if mf.Type().NumIn() != 0 {
		return nil, false
	}
	results := mf.Call(nil)
	if len(results) == 0 {
		return nil, false
	}
	// 仅返回一个值：视为 client
	if len(results) == 1 {
		return results[0].Interface(), true
	}
	// 两个返回值：期望第二个为 error
	if len(results) == 2 {
		errType := reflect.TypeOf((*error)(nil)).Elem()
		if results[1].Type().Implements(errType) {
			// 如果 error 非 nil，则视为失败
			if !results[1].IsNil() {
				return nil, false
			}
			return results[0].Interface(), true
		}
	}
	return nil, false
}
