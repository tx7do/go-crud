package mixin

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/mixin"
)

// 确保 SwitchStatus 实现了 ent.Mixin 接口
var _ ent.Mixin = (*SwitchStatus)(nil)

type SwitchStatus struct {
	mixin.Schema
}

func (SwitchStatus) Fields() []ent.Field {
	return []ent.Field{
		field.Enum("status").
			Comment("状态").
			Nillable().
			Default("ON").
			NamedValues(
				"Off", "OFF",
				"On", "ON",
			),
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// 确保 BoolStatus 实现了 ent.Mixin 接口
var _ ent.Mixin = (*BoolStatus)(nil)

type BoolStatus struct {
	mixin.Schema
}

func (BoolStatus) Fields() []ent.Field {
	return []ent.Field{
		field.Bool("status").
			Comment("状态").
			Nillable().
			Default(true),
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// 确保 TinyIntStatus 实现了 ent.Mixin 接口
var _ ent.Mixin = (*TinyIntStatus)(nil)

type TinyIntStatus struct {
	mixin.Schema
}

func (TinyIntStatus) Fields() []ent.Field {
	return []ent.Field{
		field.Uint8("status").
			Comment("状态: 1=启用, 0=禁用").
			Nillable().
			Default(1),
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// 确保 EnableEnumStatus 实现了 ent.Mixin 接口
var _ ent.Mixin = (*EnableEnumStatus)(nil)

type EnableEnumStatus struct {
	mixin.Schema
}

func (EnableEnumStatus) Fields() []ent.Field {
	return []ent.Field{
		field.Enum("status").
			Comment("状态").
			Nillable().
			Default("ENABLED").
			NamedValues(
				"Enabled", "ENABLED",
				"Disabled", "DISABLED",
			),
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// 确保 ActiveStatus 实现了 ent.Mixin 接口
var _ ent.Mixin = (*ActiveStatus)(nil)

type ActiveStatus struct {
	mixin.Schema
}

func (ActiveStatus) Fields() []ent.Field {
	return []ent.Field{
		field.Enum("status").
			Comment("状态").
			Nillable().
			Default("ACTIVE").
			NamedValues(
				"Active", "ACTIVE",
				"Inactive", "INACTIVE",
				"Pending", "PENDING",
			),
	}
}
