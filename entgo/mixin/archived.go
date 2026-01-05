package mixin

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/mixin"
)

// 确保 Archived 实现了 ent.Mixin 接口
var _ ent.Mixin = (*Archived)(nil)

type Archived struct{ mixin.Schema }

func (Archived) Fields() []ent.Field {
	return []ent.Field{
		field.Bool("archived").
			Comment("是否已归档").
			Default(false),

		field.Time("archived_at").
			Comment("归档时间").
			Nillable().
			Optional(),
	}
}
