package mixin

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/mixin"

	"github.com/tx7do/go-crud/entgo/rule"
)

type TenantID[IDT uint32 | int64 | uint64] struct{ mixin.Schema }

// Fields 按泛型参数决定 tenant_id 列的宽度：uint32→field.Uint32，uint64→field.Uint64。
func (TenantID[IDT]) Fields() []ent.Field {
	var zero IDT
	if _, isI64 := any(zero).(int64); isI64 {
		return []ent.Field{
			field.Int64("tenant_id").
				Comment("租户ID").
				Immutable().
				Default(0).
				Nillable().
				Optional(),
		}
	}
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
