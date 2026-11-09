package mixin

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/mixin"
)

var _ ent.Mixin = (*ParentID)(nil)

type ParentID struct {
	mixin.Schema
}

func (t ParentID) Fields() []ent.Field {
	return []ent.Field{
		field.Uint32("parent_id").
			Comment("父节点ID").
			Optional().
			Nillable(),
	}
}

var _ ent.Mixin = (*ParentID64)(nil)

type ParentID64 struct {
	mixin.Schema
}

func (t ParentID64) Fields() []ent.Field {
	return []ent.Field{
		field.Uint64("parent_id").
			Comment("父节点ID").
			Optional().
			Nillable(),
	}
}

type TableInterface interface {
	Type()
}

type Tree[T TableInterface] struct {
	mixin.Schema
}

func (Tree[T]) Fields() []ent.Field {
	var fields []ent.Field
	fields = append(fields, ParentID{}.Fields()...)
	return fields
}

// Edges of the Tree.
func (Tree[T]) Edges() []ent.Edge {
	return []ent.Edge{
		edge.
			To("children", T.Type).
			From("parent").Unique().Field("parent_id"),
	}
}

// Tree64 是 Tree 的 64 位 ID 变体：parent_id 列为 uint64。
type Tree64[T TableInterface] struct {
	mixin.Schema
}

func (Tree64[T]) Fields() []ent.Field {
	var fields []ent.Field
	fields = append(fields, ParentID64{}.Fields()...)
	return fields
}

// Edges of the Tree64.
func (Tree64[T]) Edges() []ent.Edge {
	return []ent.Edge{
		edge.
			To("children", T.Type).
			From("parent").Unique().Field("parent_id"),
	}
}
