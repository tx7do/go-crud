package pagination

import (
	"testing"
)

// ============ 测试数据结构 ============

// TreeNodeString 用于测试 string ID 的节点
type TreeNodeString struct {
	ID       string
	ParentID string
	Children []*TreeNodeString
}

func (n *TreeNodeString) GetId() string {
	return n.ID
}

func (n *TreeNodeString) GetParentId() string {
	return n.ParentID
}

func (n *TreeNodeString) GetChildren() []*TreeNodeString {
	return n.Children
}

// TreeNodeInt64 用于测试 int64 ID 的节点
type TreeNodeInt64 struct {
	ID       int64
	ParentID int64
	Children []TreeNodeInt64
}

func (n TreeNodeInt64) GetId() int64 {
	return n.ID
}

func (n TreeNodeInt64) GetParentId() int64 {
	return n.ParentID
}

func (n TreeNodeInt64) GetChildren() []TreeNodeInt64 {
	return n.Children
}

// TreeNodeUint32 用于测试 uint32 ID 的传统节点类型
type TreeNodeUint32 struct {
	ID       *uint32
	ParentId *uint32
	Children []*TreeNodeUint32
}

// ============ BuildTreeConstraint 测试 ============

func TestBuildTreeConstraint_StringID_EmptyInput(t *testing.T) {
	var nodes []*TreeNodeString

	result := BuildTreeConstraint(
		nodes,
		func(parent *TreeNodeString, child *TreeNodeString) {
			parent.Children = append(parent.Children, child)
		},
	)

	if len(result) != 0 {
		t.Errorf("expected empty result, got %d nodes", len(result))
	}
}

func TestBuildTreeConstraint_StringID_SingleNode(t *testing.T) {
	nodes := []*TreeNodeString{
		{ID: "1", ParentID: ""},
	}

	result := BuildTreeConstraint(
		nodes,
		func(parent *TreeNodeString, child *TreeNodeString) {
			parent.Children = append(parent.Children, child)
		},
	)

	if len(result) != 1 {
		t.Fatalf("expected 1 root node, got %d", len(result))
	}
	if result[0].ID != "1" {
		t.Errorf("expected root ID '1', got '%s'", result[0].ID)
	}
}

func TestBuildTreeConstraint_StringID_TwoLevels(t *testing.T) {
	nodes := []*TreeNodeString{
		{ID: "root", ParentID: ""},
		{ID: "child1", ParentID: "root"},
		{ID: "child2", ParentID: "root"},
	}

	result := BuildTreeConstraint(
		nodes,
		func(parent *TreeNodeString, child *TreeNodeString) {
			parent.Children = append(parent.Children, child)
		},
	)

	if len(result) != 1 {
		t.Fatalf("expected 1 root node, got %d", len(result))
	}

	root := result[0]
	if root.ID != "root" {
		t.Errorf("expected root ID 'root', got '%s'", root.ID)
	}

	if len(root.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(root.Children))
	}
}

func TestBuildTreeConstraint_StringID_ThreeLevels(t *testing.T) {
	nodes := []*TreeNodeString{
		{ID: "root", ParentID: ""},
		{ID: "child1", ParentID: "root"},
		{ID: "child2", ParentID: "root"},
		{ID: "grandchild1", ParentID: "child1"},
		{ID: "grandchild2", ParentID: "child1"},
		{ID: "grandchild3", ParentID: "child2"},
	}

	result := BuildTreeConstraint(
		nodes,
		func(parent *TreeNodeString, child *TreeNodeString) {
			parent.Children = append(parent.Children, child)
		},
	)

	if len(result) != 1 {
		t.Fatalf("expected 1 root node, got %d", len(result))
	}

	root := result[0]
	if root.ID != "root" || len(root.Children) != 2 {
		t.Errorf("root structure incorrect")
	}

	child1 := root.Children[0]
	if child1.ID != "child1" || len(child1.Children) != 2 {
		t.Errorf("child1 structure incorrect")
	}

	child2 := root.Children[1]
	if child2.ID != "child2" || len(child2.Children) != 1 {
		t.Errorf("child2 structure incorrect")
	}
}

func TestBuildTreeConstraint_StringID_MultipleRoots(t *testing.T) {
	nodes := []*TreeNodeString{
		{ID: "root1", ParentID: ""},
		{ID: "root2", ParentID: ""},
		{ID: "root3", ParentID: ""},
		{ID: "child1", ParentID: "root1"},
		{ID: "child2", ParentID: "root2"},
	}

	result := BuildTreeConstraint(
		nodes,
		func(parent *TreeNodeString, child *TreeNodeString) {
			parent.Children = append(parent.Children, child)
		},
	)

	if len(result) != 3 {
		t.Fatalf("expected 3 root nodes, got %d", len(result))
	}
}

func TestBuildTreeConstraint_StringID_OrphanNodes(t *testing.T) {
	nodes := []*TreeNodeString{
		{ID: "root", ParentID: ""},
		{ID: "orphan", ParentID: "nonexistent"},
		{ID: "child", ParentID: "root"},
	}

	result := BuildTreeConstraint(
		nodes,
		func(parent *TreeNodeString, child *TreeNodeString) {
			parent.Children = append(parent.Children, child)
		},
	)

	if len(result) != 1 {
		t.Fatalf("expected 1 root node, got %d", len(result))
	}

	root := result[0]
	if len(root.Children) != 1 || root.Children[0].ID != "child" {
		t.Error("orphan node should be skipped, only 'child' should be under root")
	}
}

func TestBuildTreeConstraint_Int64ID(t *testing.T) {
	nodes := []TreeNodeInt64{
		{ID: 1, ParentID: 0},
		{ID: 2, ParentID: 1},
		{ID: 3, ParentID: 1},
		{ID: 4, ParentID: 2},
	}

	result := BuildTreeConstraint(
		nodes,
		func(parent TreeNodeInt64, child TreeNodeInt64) {
			// 注意：这里的 append 不会修改原始节点，因为传递的是值
			// 这只是演示方法，实际使用中需要注意这一点
			_ = parent
			_ = child
		},
	)

	// 由于值语义，children 不会被实际添加，但树结构应该构建成功
	if len(result) != 1 {
		t.Fatalf("expected 1 root node, got %d", len(result))
	}

	root := result[0]
	if root.ID != 1 {
		t.Errorf("expected root ID 1, got %d", root.ID)
	}
}

func TestBuildTreeConstraint_NilNodes(t *testing.T) {
	nodes := []*TreeNodeString{
		nil,
		{ID: "root", ParentID: ""},
		nil,
		{ID: "child", ParentID: "root"},
	}

	result := BuildTreeConstraint(
		nodes,
		func(parent *TreeNodeString, child *TreeNodeString) {
			parent.Children = append(parent.Children, child)
		},
	)

	if len(result) != 1 {
		t.Fatalf("expected 1 root node, got %d", len(result))
	}

	root := result[0]
	if len(root.Children) != 1 || root.Children[0].ID != "child" {
		t.Error("nil nodes should be skipped")
	}
}

// ============ BuildTree 测试 ============

func TestBuildTree_EmptyInput(t *testing.T) {
	var nodes []*TreeNodeUint32

	result := BuildTree(
		nodes,
		func(node *TreeNodeUint32) *uint32 { return node.ID },
		func(node *TreeNodeUint32) *uint32 { return node.ParentId },
		func(node *TreeNodeUint32) *[]*TreeNodeUint32 { return &node.Children },
	)

	if len(result) != 0 {
		t.Errorf("expected empty result, got %d nodes", len(result))
	}
}

func TestBuildTree_SingleNode(t *testing.T) {
	id := uint32(1)
	nodes := []*TreeNodeUint32{
		{ID: &id, ParentId: nil},
	}

	result := BuildTree(
		nodes,
		func(node *TreeNodeUint32) *uint32 { return node.ID },
		func(node *TreeNodeUint32) *uint32 { return node.ParentId },
		func(node *TreeNodeUint32) *[]*TreeNodeUint32 { return &node.Children },
	)

	if len(result) != 1 {
		t.Fatalf("expected 1 root node, got %d", len(result))
	}
	if *result[0].ID != 1 {
		t.Errorf("expected root ID 1, got %d", *result[0].ID)
	}
}

func TestBuildTree_TwoLevels(t *testing.T) {
	rootID := uint32(1)
	childID1 := uint32(2)
	childID2 := uint32(3)

	nodes := []*TreeNodeUint32{
		{ID: &rootID, ParentId: nil},
		{ID: &childID1, ParentId: &rootID},
		{ID: &childID2, ParentId: &rootID},
	}

	result := BuildTree(
		nodes,
		func(node *TreeNodeUint32) *uint32 { return node.ID },
		func(node *TreeNodeUint32) *uint32 { return node.ParentId },
		func(node *TreeNodeUint32) *[]*TreeNodeUint32 { return &node.Children },
	)

	if len(result) != 1 {
		t.Fatalf("expected 1 root node, got %d", len(result))
	}

	root := result[0]
	if *root.ID != rootID {
		t.Errorf("expected root ID %d, got %d", rootID, *root.ID)
	}

	if len(root.Children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(root.Children))
	}
}

func TestBuildTree_ThreeLevels(t *testing.T) {
	rootID := uint32(1)
	child1ID := uint32(2)
	child2ID := uint32(3)
	grandchild1ID := uint32(4)
	grandchild2ID := uint32(5)
	grandchild3ID := uint32(6)

	nodes := []*TreeNodeUint32{
		{ID: &rootID, ParentId: nil},
		{ID: &child1ID, ParentId: &rootID},
		{ID: &child2ID, ParentId: &rootID},
		{ID: &grandchild1ID, ParentId: &child1ID},
		{ID: &grandchild2ID, ParentId: &child1ID},
		{ID: &grandchild3ID, ParentId: &child2ID},
	}

	result := BuildTree(
		nodes,
		func(node *TreeNodeUint32) *uint32 { return node.ID },
		func(node *TreeNodeUint32) *uint32 { return node.ParentId },
		func(node *TreeNodeUint32) *[]*TreeNodeUint32 { return &node.Children },
	)

	if len(result) != 1 {
		t.Fatalf("expected 1 root node, got %d", len(result))
	}

	root := result[0]
	if *root.ID != rootID || len(root.Children) != 2 {
		t.Errorf("root structure incorrect")
	}

	child1 := root.Children[0]
	if *child1.ID != child1ID || len(child1.Children) != 2 {
		t.Errorf("child1 structure incorrect")
	}

	child2 := root.Children[1]
	if *child2.ID != child2ID || len(child2.Children) != 1 {
		t.Errorf("child2 structure incorrect")
	}
}

func TestBuildTree_MultipleRoots(t *testing.T) {
	root1ID := uint32(1)
	root2ID := uint32(2)
	root3ID := uint32(3)
	child1ID := uint32(4)
	child2ID := uint32(5)

	nodes := []*TreeNodeUint32{
		{ID: &root1ID, ParentId: nil},
		{ID: &root2ID, ParentId: nil},
		{ID: &root3ID, ParentId: nil},
		{ID: &child1ID, ParentId: &root1ID},
		{ID: &child2ID, ParentId: &root2ID},
	}

	result := BuildTree(
		nodes,
		func(node *TreeNodeUint32) *uint32 { return node.ID },
		func(node *TreeNodeUint32) *uint32 { return node.ParentId },
		func(node *TreeNodeUint32) *[]*TreeNodeUint32 { return &node.Children },
	)

	if len(result) != 3 {
		t.Fatalf("expected 3 root nodes, got %d", len(result))
	}
}

func TestBuildTree_OrphanNodes(t *testing.T) {
	rootID := uint32(1)
	orphanID := uint32(2)
	childID := uint32(3)
	missingParentID := uint32(999) // 指向一个不存在的父节点ID

	nodes := []*TreeNodeUint32{
		{ID: &rootID, ParentId: nil},
		{ID: &orphanID, ParentId: &missingParentID}, // 指向不存在的父节点
		{ID: &childID, ParentId: &rootID},
	}

	result := BuildTree(
		nodes,
		func(node *TreeNodeUint32) *uint32 { return node.ID },
		func(node *TreeNodeUint32) *uint32 { return node.ParentId },
		func(node *TreeNodeUint32) *[]*TreeNodeUint32 { return &node.Children },
	)

	if len(result) != 1 {
		t.Fatalf("expected 1 root node, got %d", len(result))
	}

	root := result[0]
	if len(root.Children) != 1 || *root.Children[0].ID != childID {
		t.Error("orphan node should be skipped, only 'child' should be under root")
	}
}

func TestBuildTree_ParentIdZero(t *testing.T) {
	zero := uint32(0)
	rootID := uint32(1)

	nodes := []*TreeNodeUint32{
		{ID: &rootID, ParentId: &zero},
	}

	result := BuildTree(
		nodes,
		func(node *TreeNodeUint32) *uint32 { return node.ID },
		func(node *TreeNodeUint32) *uint32 { return node.ParentId },
		func(node *TreeNodeUint32) *[]*TreeNodeUint32 { return &node.Children },
	)

	if len(result) != 1 {
		t.Fatalf("expected 1 root node (parentId=0 means root), got %d", len(result))
	}
}

// ============ IsNil 测试 ============

func TestIsNil_True_Cases(t *testing.T) {
	testCases := []struct {
		name  string
		input any
	}{
		{"nil interface", nil},
		{"nil pointer", (*int)(nil)},
		{"nil slice", ([]int)(nil)},
		{"nil map", (map[int]int)(nil)},
		{"nil channel", (chan int)(nil)},
		{"nil func", (func())(nil)},
		{"nil interface", (*string)(nil)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsNil(tc.input)
			if !result {
				t.Errorf("IsNil(%v) = false, want true", tc.input)
			}
		})
	}
}

func TestIsNil_False_Cases(t *testing.T) {
	testCases := []struct {
		name  string
		input any
	}{
		{"int", 42},
		{"string", "hello"},
		{"non-nil pointer", new(int)},
		{"non-nil slice", []int{1, 2, 3}},
		{"non-nil map", map[string]int{"a": 1}},
		{"non-nil channel", make(chan int)},
		{"struct", struct{ ID int }{ID: 1}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := IsNil(tc.input)
			if result {
				t.Errorf("IsNil(%v) = true, want false", tc.input)
			}
		})
	}
}

// ============ GetStringField 测试 ============

// testStruct 用于GetStringField测试
type testStruct struct {
	Name        string
	Age         int
	NamePtr     *string
	Description string
}

func TestGetStringField_Success(t *testing.T) {
	name := "John"

	testCases := []struct {
		name     string
		input    any
		fields   []string
		expected string
	}{
		{
			name:     "string field",
			input:    testStruct{Name: "Alice"},
			fields:   []string{"Name"},
			expected: "Alice",
		},
		{
			name:     "pointer to struct with string field",
			input:    &testStruct{Name: "Bob"},
			fields:   []string{"Name"},
			expected: "Bob",
		},
		{
			name:     "*string field",
			input:    testStruct{NamePtr: &name},
			fields:   []string{"NamePtr"},
			expected: "John",
		},
		{
			name:     "first field match",
			input:    testStruct{Name: "Charlie", Description: "Test"},
			fields:   []string{"Name", "Description"},
			expected: "Charlie",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, ok := GetStringField(tc.input, tc.fields)
			if !ok {
				t.Errorf("GetStringField(%v, %v) returned ok=false, want true", tc.input, tc.fields)
			}
			if result != tc.expected {
				t.Errorf("GetStringField(%v, %v) = %q, want %q", tc.input, tc.fields, result, tc.expected)
			}
		})
	}
}

func TestGetStringField_Failure(t *testing.T) {
	testCases := []struct {
		name   string
		input  any
		fields []string
	}{
		{"nil input", nil, []string{"Name"}},
		{"empty string field", testStruct{Name: ""}, []string{"Name"}},
		{"nil pointer field", testStruct{NamePtr: nil}, []string{"NamePtr"}},
		{"non-existent field", testStruct{Name: "Alice"}, []string{"NonExistent"}},
		{"wrong type", "just a string", []string{"Name"}},
		{"nil pointer value", (*testStruct)(nil), []string{"Name"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, ok := GetStringField(tc.input, tc.fields)
			if ok {
				t.Errorf("GetStringField(%v, %v) returned ok=true, want false", tc.input, tc.fields)
			}
			if result != "" {
				t.Errorf("GetStringField(%v, %v) = %q, want empty string", tc.input, tc.fields, result)
			}
		})
	}
}

// ============ AppendChild 测试 ============

// childParent 用于AppendChild测试
type childParent struct {
	ID       string
	Children []*childChild
}

// childChild 用于AppendChild测试
type childChild struct {
	ID string
}

// childParentAlt 用于测试不同的Children字段名
type childParentAlt struct {
	ID         string
	Childrens  []*childChildAlt
	ChildField []*childChildAlt
}

// childChildAlt 用于childParentAlt测试
type childChildAlt struct {
	ID string
}

func TestAppendChild_Success(t *testing.T) {
	parent := &childParent{ID: "parent1"}
	child := &childChild{ID: "child1"}

	result := AppendChild(parent, child)
	if !result {
		t.Error("AppendChild() = false, want true")
	}
	if len(parent.Children) != 1 {
		t.Errorf("len(parent.Children) = %d, want 1", len(parent.Children))
	}
	if parent.Children[0].ID != "child1" {
		t.Errorf("parent.Children[0].ID = %q, want %q", parent.Children[0].ID, "child1")
	}
}

func TestAppendChild_MultipleChildren(t *testing.T) {
	parent := &childParent{ID: "parent1"}
	child1 := &childChild{ID: "child1"}
	child2 := &childChild{ID: "child2"}

	AppendChild(parent, child1)
	AppendChild(parent, child2)

	if len(parent.Children) != 2 {
		t.Errorf("len(parent.Children) = %d, want 2", len(parent.Children))
	}
}

func TestAppendChild_NilInputs(t *testing.T) {
	testCases := []struct {
		name   string
		parent any
		child  any
	}{
		{"nil parent", nil, &childChild{ID: "child1"}},
		{"nil child", &childParent{ID: "parent1"}, nil},
		{"both nil", nil, nil},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := AppendChild(tc.parent, tc.child)
			if result {
				t.Errorf("AppendChild(nil inputs) = true, want false")
			}
		})
	}
}

func TestAppendChild_NonStructPointer(t *testing.T) {
	testCases := []struct {
		name   string
		parent any
		child  any
	}{
		{"string parent", "not a struct", &childChild{ID: "child1"}},
		{"int parent", 123, &childChild{ID: "child1"}},
		{"slice parent", []int{1, 2}, &childChild{ID: "child1"}},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := AppendChild(tc.parent, tc.child)
			if result {
				t.Errorf("AppendChild(non-struct parent) = true, want false")
			}
		})
	}
}

func TestAppendChild_NoChildrenField(t *testing.T) {
	type noChildrenField struct {
		ID   string
		Name string
	}

	parent := &noChildrenField{ID: "parent1"}
	child := &childChild{ID: "child1"}

	result := AppendChild(parent, child)
	if result {
		t.Error("AppendChild(no Children field) = true, want false")
	}
}

func TestAppendChild_ToDifferentFieldName(t *testing.T) {
	parent := &childParentAlt{ID: "parent1"}
	child := &childChildAlt{ID: "child1"}

	// 测试 Childrens 字段
	result := AppendChild(parent, child)
	if !result {
		t.Error("AppendChild() to Childrens field = false, want true")
	}
	if len(parent.Childrens) != 1 {
		t.Errorf("len(parent.Childrens) = %d, want 1", len(parent.Childrens))
	}
}

func TestAppendChild_ValueType(t *testing.T) {
	parent := childParent{ID: "parent1"} // 值类型，不是指针
	child := &childChild{ID: "child1"}

	result := AppendChild(parent, child)
	if result {
		t.Error("AppendChild(value type parent) = true, want false")
	}
}

func TestAppendChild_TypeMismatch(t *testing.T) {
	parent := &childParent{ID: "parent1"}
	child := "not a child" // 类型不匹配

	result := AppendChild(parent, child)
	if result {
		t.Error("AppendChild(type mismatch) = true, want false")
	}
}
