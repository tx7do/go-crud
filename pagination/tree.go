package pagination

import "reflect"

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

// BuildTreeConstraint 构建树形结构的泛型方法（基于接口约束 + 高效映射）
// ID: 节点ID的类型（如 string, int64, uint32 等）
// T: 节点指针类型，必须实现 NodeConstraint[ID, T] 接口
// nodes: 扁平的节点列表
// appendChild: 将子节点添加到父节点的函数（用于处理接口返回值的限制）
// 返回：根节点列表（包含完整的子树）
// 时间复杂度：O(n)，空间复杂度：O(n)
//
// 使用示例：
//
//	roots := BuildTreeConstraint(
//	    dtos,
//	    func(parent *OrgUnit, child *OrgUnit) {
//	        parent.Children = append(parent.Children, child)
//	    },
//	)
func BuildTreeConstraint[ID ~string | ~int32 | ~int64 | ~uint32 | ~uint64, T NodeConstraint[ID, T]](
	nodes []T,
	appendChild func(parent T, child T),
) []T {
	if len(nodes) == 0 {
		return []T{}
	}

	// 构建映射表，用于快速查找
	nodeMap := make(map[ID]T)
	rootNodes := make([]T, 0)

	// 第一次遍历：建立 ID -> Node 的映射
	for _, node := range nodes {
		if IsNil(node) {
			continue
		}
		id := node.GetId()
		var zeroID ID
		if id != zeroID {
			nodeMap[id] = node
		}
	}

	// 第二次遍历：构建树结构
	var zeroID ID
	for _, node := range nodes {
		if IsNil(node) {
			continue
		}
		parentId := node.GetParentId()
		if parentId == zeroID {
			// 根节点
			rootNodes = append(rootNodes, node)
		} else {
			// 子节点：查找父节点并添加
			if parent, ok := nodeMap[parentId]; !IsNil(parent) && ok {
				appendChild(parent, node)
			}
			// 如果找不到父节点，则该节点被跳过（孤儿节点）
		}
	}

	return rootNodes
}

// BuildTree 构建树形结构的泛型方法
// T: 节点类型，必须包含 Id、ParentId 和 Children 字段
// getId: 获取节点ID的函数
// getParentId: 获取父节点ID的函数
// getChildren: 获取子节点切片的指针的函数
// nodes: 扁平的节点列表
// 返回：根节点列表（包含完整的子树）
// 时间复杂度：O(n)，空间复杂度：O(n)
//
// 使用示例：
//
//	dtos = BuildTree(
//	    dtos,
//	    func(node *OrgUnit) *uint32 { return node.Id },
//	    func(node *OrgUnit) *uint32 { return node.ParentId },
//	    func(node *OrgUnit) *[]*OrgUnit { return &node.Children },
//	)
func BuildTree[T any](
	nodes []*T,
	getId func(node *T) *uint32,
	getParentId func(node *T) *uint32,
	getChildren func(node *T) *[]*T,
) []*T {
	if len(nodes) == 0 {
		return []*T{}
	}

	// 构建映射表，用于快速查找
	nodeMap := make(map[uint32]*T)
	rootNodes := make([]*T, 0)

	// 第一次遍历：建立 ID -> Node 的映射
	for _, node := range nodes {
		if id := getId(node); id != nil {
			nodeMap[*id] = node
		}
	}

	// 第二次遍历：构建树结构
	for _, node := range nodeMap {
		parentId := getParentId(node)
		if parentId == nil || *parentId == 0 {
			// 根节点
			rootNodes = append(rootNodes, node)
		} else {
			// 子节点：查找父节点并添加
			if parent, ok := nodeMap[*parentId]; ok {
				children := getChildren(parent)
				if *children == nil {
					// 初始化子节点切片
					*children = make([]*T, 0)
				}
				*children = append(*children, node)
			}
			// 如果找不到父节点，则该节点被跳过（孤儿节点）
		}
	}

	return rootNodes
}

// IsNil 检查接口值是否为 nil 或指向 nil
func IsNil(v any) bool {
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

// GetStringField 从结构体（或指向结构体的指针）中按候选字段名读取 string 或 *string 值
// 返回 (value, true) 表示成功并且不为零值；否则返回 ("", false)
func GetStringField(v any, names []string) (string, bool) {
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

// AppendChild 尝试把 child 追加到 parent 的 Children 字段中（字段名候选："Children"）
// 成功返回 true；否则返回 false
func AppendChild(parent any, child any) bool {
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
