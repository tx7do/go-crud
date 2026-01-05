package mixin

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/mixin"
)

// 确保 IsEnabled 实现了 ent.Mixin 接口
var _ ent.Mixin = (*IsEnabled)(nil)

type IsEnabled struct {
	mixin.Schema
}

func (IsEnabled) Fields() []ent.Field {
	return []ent.Field{
		field.Bool("is_enabled").
			Comment("是否启用").
			Optional().
			Nillable().
			Default(true),
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// 确保 Enabled 实现了 ent.Mixin 接口
var _ ent.Mixin = (*Enabled)(nil)

type Enabled struct {
	mixin.Schema
}

func (Enabled) Fields() []ent.Field {
	return []ent.Field{
		field.Bool("enabled").
			Comment("是否启用").
			Optional().
			Nillable().
			Default(true),
	}
}
