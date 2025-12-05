package entgo

import "testing"

// 测试用的节点类型
type TreeNode struct {
	ID       string
	ParentID string
	Children []*TreeNode
}

func (n *TreeNode) GetId() string {
	return n.ID
}
func (n *TreeNode) GetParentId() string {
	return n.ParentID
}
func (n *TreeNode) GetChildren() []*TreeNode {
	return n.Children
}

func TestTravelChild_AddRoot(t *testing.T) {
	roots := make([]*TreeNode, 0)
	root := &TreeNode{ID: "root", ParentID: ""}

	ok := TravelChild(&roots, root, func(parent *TreeNode, node *TreeNode) {
		parent.Children = append(parent.Children, node)
	})
	if !ok {
		t.Fatalf("expected true, got false")
	}
	if len(roots) != 1 || roots[0] != root {
		t.Fatalf("root not added correctly, roots=%v", roots)
	}
}

func TestTravelChild_AddChild(t *testing.T) {
	root := &TreeNode{ID: "root", ParentID: ""}
	roots := []*TreeNode{root}

	child := &TreeNode{ID: "c1", ParentID: "root"}
	ok := TravelChild(&roots, child, func(parent *TreeNode, node *TreeNode) {
		parent.Children = append(parent.Children, node)
	})
	if !ok {
		t.Fatalf("expected true when adding child, got false")
	}
	if len(root.Children) != 1 || root.Children[0] != child {
		t.Fatalf("child not added to root.Children, got=%v", root.Children)
	}
}

func TestTravelChild_AddGrandchild(t *testing.T) {
	root := &TreeNode{ID: "root", ParentID: ""}
	roots := []*TreeNode{root}

	child := &TreeNode{ID: "c1", ParentID: "root"}
	if !TravelChild(&roots, child, func(parent *TreeNode, node *TreeNode) {
		parent.Children = append(parent.Children, node)
	}) {
		t.Fatalf("failed to add child")
	}

	grand := &TreeNode{ID: "g1", ParentID: "c1"}
	if !TravelChild(&roots, grand, func(parent *TreeNode, node *TreeNode) {
		parent.Children = append(parent.Children, node)
	}) {
		t.Fatalf("failed to add grandchild")
	}
	if len(root.Children) != 1 || len(root.Children[0].Children) != 1 || root.Children[0].Children[0] != grand {
		t.Fatalf("grandchild not nested correctly, tree=%+v", roots)
	}
}

func TestTravelChild_MissingParent(t *testing.T) {
	var roots []*TreeNode
	child := &TreeNode{ID: "orphan", ParentID: "nope"}
	ok := TravelChild(&roots, child, func(parent *TreeNode, node *TreeNode) {
		parent.Children = append(parent.Children, node)
	})
	if ok {
		t.Fatalf("expected false when parent missing, got true")
	}
	if len(roots) != 0 {
		t.Fatalf("roots should remain empty, got=%v", roots)
	}
}
