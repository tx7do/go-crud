package mixin

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/mixin"

	"github.com/tx7do/go-crud/entgo/rule"
)

type TenantID[IDT uint32 | uint64] struct{ mixin.Schema }

func (TenantID[IDT]) Fields() []ent.Field {
	return []ent.Field{
		field.Uint32("tenant_id").
			Comment("租户ID").
			Immutable().
			Default(0).
			Nillable().
			Optional(),
	}
}

func (TenantID[IDT]) Policy() ent.Policy {
	return rule.TenantPrivacy[IDT]{}
}

//func (TenantID[IDT]) Interceptors() []ent.Interceptor {
//	return []ent.Interceptor{
//		interceptor.TenantInterceptor(),
//	}
//}
