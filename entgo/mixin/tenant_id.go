package mixin

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/mixin"
)

// 确保 TenantID 实现了 ent.Mixin 接口
var _ ent.Mixin = (*TenantID)(nil)

type TenantID struct{ mixin.Schema }

func (TenantID) Fields() []ent.Field {
	return []ent.Field{
		field.Uint32("tenant_id").
			Comment("租户ID").
			Immutable().
			Nillable().
			Optional(),
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// 确保 TenantID64 实现了 ent.Mixin 接口
var _ ent.Mixin = (*TenantID64)(nil)

type TenantID64 struct{ mixin.Schema }

func (TenantID64) Fields() []ent.Field {
	return []ent.Field{
		field.Uint64("tenant_id").
			Comment("租户ID").
			Immutable().
			Nillable().
			Optional(),
	}
}
