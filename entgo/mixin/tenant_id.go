package mixin

import (
	"context"
	"fmt"
	"reflect"

	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/mixin"

	"github.com/tx7do/go-crud/viewer"
)

// tenantIDMutator 提取的 mutator，负责从 TenantContext 读取 tenant id 并注入 Mutation。
// 假设生成的 SetTenantID 使用 uint32 类型。
func tenantIDMutator[T uint32 | uint64](next ent.Mutator) ent.Mutator {
	return ent.MutateFunc(func(ctx context.Context, m ent.Mutation) (ent.Value, error) {
		vc, exist := viewer.FromContext(ctx)
		if !exist {
			return nil, fmt.Errorf("missing ViewerContext in context")
		}

		op := m.Op()
		if !op.Is(ent.OpCreate) {
			return next.Mutate(ctx, m)
		}

		tid := vc.TenantID()

		if vc.IsPlatformContext() {
			// 如果管理员在代码里写了 .SetTenantID(101)，则尊重管理员的选择
			if _, set := m.Field("tenant_id"); set {
				return next.Mutate(ctx, m)
			}
			// 如果管理员没设置，且当前上下文也没指定目标租户，则按管理员逻辑执行（可能设为 0）
			return next.Mutate(ctx, m)
		}

		// 普通用户：强制覆盖，防止越权
		// 优先使用强类型接口（生成代码常见）
		if s, ok := m.(interface{ SetTenantID(T) }); ok {
			s.SetTenantID(T(tid))
			return next.Mutate(ctx, m)
		}

		// 兜底：尝试通过反射调用 SetField，以避免编译期因方法签名差异导致的模糊错误
		rv := reflect.ValueOf(m)
		if mf := rv.MethodByName("SetField"); mf.IsValid() && mf.Kind() == reflect.Func {
			// 仅在方法接受两个参数时调用，避免 panic
			if mf.Type().NumIn() == 2 {
				mf.Call([]reflect.Value{reflect.ValueOf("tenant_id"), reflect.ValueOf(tid)})
				return next.Mutate(ctx, m)
			}
		}

		// 如果都不可用，则直接返回错误以便上层可感知（也可选择直接 next.Mutate）
		return nil, fmt.Errorf("unable to set tenant_id on mutation")
	})
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// 确保 TenantID 实现了 ent.Mixin 接口
var _ ent.Mixin = (*TenantID)(nil)

type TenantID struct{ mixin.Schema }

func (TenantID) Fields() []ent.Field {
	return []ent.Field{
		field.Uint32("tenant_id").
			Comment("租户ID").
			Immutable().
			Default(0).
			Nillable().
			Optional(),
	}
}

// Hooks 强制注入 tenant_id
func (TenantID) Hooks() []ent.Hook {
	return []ent.Hook{
		tenantIDMutator[uint32],
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
			Default(0).
			Nillable().
			Optional(),
	}
}

// Hooks 强制注入 tenant_id
func (TenantID64) Hooks() []ent.Hook {
	return []ent.Hook{
		tenantIDMutator[uint64],
	}
}
