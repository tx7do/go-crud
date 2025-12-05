package entgo

import (
	"reflect"

	"github.com/tx7do/go-utils/trans"
)

// NodeConstraint 泛型节点约束接口
// ID: 节点ID的类型
// T: 具体节点类型
type NodeConstraint[ID ~string | ~int32 | ~int64 | ~uint32 | ~uint64, T any] interface {
	// GetId 返回当前节点的唯一ID
	GetId() ID

	// GetParentId 返回父节点ID
	GetParentId() ID

	// GetChildren 返回子节点切片的指针
	GetChildren() []T
}

// TravelChild 泛型实现：递归查找父节点并添加子节点
// ID: 节点ID类型（如string、int64）
// T: 节点类型（需实现NodeConstraint[ID, T]接口）
// nodes: 待遍历的节点切片指针
// node: 待添加的节点
// appendChild: 将子节点添加到父节点的函数
// return: 是否成功添加节点
func TravelChild[ID ~string | ~int32 | ~int64 | ~uint32 | ~uint64, T NodeConstraint[ID, T]](
	nodes *[]T,
	node T,
	appendChild func(parent T, node T),
) bool {
	// 入参校验：nodes切片指针不能为nil
	if nodes == nil {
		return false
	}
	if isNil(node) {
		return false
	}

	// 根节点（无父节点）：直接添加到顶层节点列表
	var zeroID ID
	if node.GetParentId() == zeroID {
		*nodes = append(*nodes, node)
		return true
	}

	// 遍历当前层级节点，查找父节点
	for _, parentCandidate := range *nodes {
		if isNil(parentCandidate) {
			continue
		}

		// 找到父节点：添加子节点并返回成功
		if parentCandidate.GetId() == node.GetParentId() {
			appendChild(parentCandidate, node)
			return true
		}

		// 递归查找子节点
		if TravelChild(trans.Ptr(parentCandidate.GetChildren()), node, appendChild) {
			return true
		}
	}

	// 未找到父节点（如父节点不存在于节点树中）
	return false
}

// isNil 检查接口值是否为 nil 或指向 nil
func isNil(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Interface, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan:
		return rv.IsNil()
	default:
		return false
	}
}

// getStringField 从结构体（或指向结构体的指针）中按候选字段名读取 string 或 *string 值
// 返回 (value, true) 表示成功并且不为零值；否则返回 ("", false)
func getStringField(v any, names []string) (string, bool) {
	if v == nil {
		return "", false
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return "", false
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return "", false
	}
	for _, name := range names {
		f := rv.FieldByName(name)
		if !f.IsValid() {
			continue
		}
		// 处理 string
		switch f.Kind() {
		case reflect.String:
			s := f.String()
			if s == "" {
				return "", false
			}
			return s, true
		case reflect.Ptr:
			if f.IsNil() {
				return "", false
			}
			fe := f.Elem()
			if fe.Kind() == reflect.String {
				s := fe.String()
				if s == "" {
					return "", false
				}
				return s, true
			}
		default:
			panic("unhandled default case")
		}
	}
	return "", false
}

// appendChild 尝试把 child 追加到 parent 的 Children 字段中（字段名候选："Children"）
// 成功返回 true；否则返回 false
func appendChild(parent any, child any) bool {
	if parent == nil || child == nil {
		return false
	}
	rv := reflect.ValueOf(parent)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return false
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return false
	}
	// 候选的子切片字段名
	candidates := []string{"Children", "Childrens", "Child"}
	for _, name := range candidates {
		f := rv.FieldByName(name)
		if !f.IsValid() || !f.CanSet() {
			continue
		}
		// 必须是 slice 类型
		if f.Kind() != reflect.Slice {
			continue
		}
		// child 的类型必须与 slice 的 elem 类型兼容
		childVal := reflect.ValueOf(child)
		// 如果 slice elem 是非指针但 child 是指针，尝试解指针
		elemType := f.Type().Elem()
		if !childVal.Type().AssignableTo(elemType) {
			// 尝试调整 child 类型（如果 child 是指且 elem 是指向相同类型）
			if childVal.Kind() == reflect.Ptr && childVal.Elem().Type().AssignableTo(elemType) {
				childVal = childVal.Elem()
			} else if elemType.Kind() == reflect.Ptr && childVal.Type().AssignableTo(elemType.Elem()) {
				// 将 child 转为指针：创建新指针并设置
				ptr := reflect.New(childVal.Type())
				ptr.Elem().Set(childVal)
				if !ptr.Type().AssignableTo(elemType) {
					// 不能匹配
					continue
				}
				childVal = ptr
			} else {
				continue
			}
		}
		// append 并设置回字段
		newSlice := reflect.Append(f, childVal)
		f.Set(newSlice)
		return true
	}
	return false
}
