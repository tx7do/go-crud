package mixin

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/mixin"
)

// 确保 TreePathIDs 实现了 ent.Mixin 接口
var _ ent.Mixin = (*TreePathIDs)(nil)

type TreePathIDs struct {
	mixin.Schema
}

func (TreePathIDs) Fields() []ent.Field {
	return []ent.Field{
		field.JSON("ancestor_ids", []uint32{}).
			Comment("祖先 ID 列表，按从根到父的顺序; 存储为 JSON 数组").
			Optional(),
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// 确保 TreePathIDs64 实现了 ent.Mixin 接口
var _ ent.Mixin = (*TreePathIDs64)(nil)

type TreePathIDs64 struct {
	mixin.Schema
}

func (TreePathIDs64) Fields() []ent.Field {
	return []ent.Field{
		field.JSON("ancestor_ids", []uint64{}).
			Comment("祖先 ID 列表，按从根到父的顺序; 存储为 JSON 数组").
			Optional(),
	}
}
