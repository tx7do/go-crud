package rule

import (
	"context"
	"fmt"
	"reflect"

	"entgo.io/ent"
	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/entql"
	"entgo.io/ent/privacy"

	"github.com/tx7do/go-crud/viewer"
)

type (
	// Filter is the interface that wraps the Where function
	// for filtering nodes in queries and mutations.
	Filter interface {
		// Where applies a filter on the executed query/mutation.
		Where(entql.P)
	}
)

// AllowIfAdmin is a rule that returns Allow decision if the viewer is admin.
func AllowIfAdmin() privacy.QueryMutationRule {
	return privacy.ContextQueryMutationRule(func(ctx context.Context) error {
		vc, exist := viewer.FromContext(ctx)
		if !exist {
			return privacy.Skip
		}
		if vc.IsPlatformContext() || vc.IsSystemContext() {
			return privacy.Allow
		}
		// Skip to the next privacy rule (equivalent to returning nil).
		return privacy.Skip
	})
}

// TenantFilterRule 是一个通用的租户过滤规则，用于在查询时注入租户过滤条件。
// 该规则会根据当前 ViewerContext 中的租户信息，动态添加租户过滤谓词，确保数据隔离和安全性。
// 适用于包含 tenant_id 字段的实体查询。
func TenantFilterRule(ctx context.Context, f Filter) error {
	vc, exist := viewer.FromContext(ctx)
	// 如果身份丢失，安全起见应直接拒绝操作（Deny），而不是跳过
	if !exist {
		return fmt.Errorf("security: missing ViewerContext in context")
	}

	// 平台管理视图/系统视图放行：允许查看全量数据
	if vc.IsPlatformContext() || vc.IsSystemContext() {
		return nil // 在 Privacy 逻辑中 nil 相当于 Skip
	}

	tid := vc.TenantID()

	tenantPred := entql.Uint64EQ(tid).Field("tenant_id")

	// 注入租户过滤谓词
	f.Where(tenantPred)

	return nil
}

type TenantPrivacy[T uint32 | uint64] struct {
	decision error
}

func (f TenantPrivacy[T]) EvalQuery(ctx context.Context, query ent.Query) error {
	vc, exist := viewer.FromContext(ctx)
	// 如果身份丢失，安全起见应直接拒绝操作（Deny），而不是跳过
	if !exist {
		return fmt.Errorf("security: missing ViewerContext in context")
	}

	// 平台管理视图/系统视图放行：允许查看全量数据
	if vc.IsPlatformContext() || vc.IsSystemContext() {
		return nil
	}

	tid := vc.TenantID()

	if err := f.injectTenantWhere(query, T(tid)); err != nil {
		return err
	}

	return nil
}

func (f TenantPrivacy[T]) EvalMutation(ctx context.Context, m ent.Mutation) error {
	vc, exist := viewer.FromContext(ctx)
	if !exist {
		return fmt.Errorf("missing ViewerContext in context")
	}

	op := m.Op()
	if !op.Is(ent.OpCreate) {
		return nil
	}

	tid := vc.TenantID()

	if vc.IsPlatformContext() {
		// 如果管理员在代码里写了 .SetTenantID(101)，则尊重管理员的选择
		if _, set := m.Field("tenant_id"); set {
			return nil
		}
		// 如果管理员没设置，且当前上下文也没指定目标租户，则按管理员逻辑执行（可能设为 0）
		return nil
	}

	// 普通用户：强制覆盖，防止越权
	// 优先使用强类型接口（生成代码常见）
	if s, ok := m.(interface{ SetTenantID(T) }); ok {
		s.SetTenantID(T(tid))
		return nil
	}

	// 兜底：尝试通过反射调用 SetField，以避免编译期因方法签名差异导致的模糊错误
	rv := reflect.ValueOf(m)
	if mf := rv.MethodByName("SetField"); mf.IsValid() && mf.Kind() == reflect.Func {
		// 仅在方法接受两个参数时调用，避免 panic
		if mf.Type().NumIn() == 2 {
			mf.Call([]reflect.Value{reflect.ValueOf("tenant_id"), reflect.ValueOf(tid)})
			return nil
		}
	}

	// 如果都不可用，则直接返回错误以便上层可感知（也可选择直接 next.Mutate）
	return fmt.Errorf("unable to set tenant_id on mutation")
}

// injectTenantWhere 尝试通过反射在 query 上调用 Where\(...\) 并注入 tenant_id 过滤。
// 返回可能被 Where 链式调用替换后的 ent.Query（若 Where 返回链式值）。
func (f TenantPrivacy[T]) injectTenantWhere(query ent.Query, tenantID T) error {
	rv := reflect.ValueOf(query)
	mf := rv.MethodByName("Where")
	if !mf.IsValid() || mf.Kind() != reflect.Func {
		return nil
	}

	mt := mf.Type()
	// 期待形如 Where(...T) 且只有一个参数（变参）
	if !mt.IsVariadic() || mt.NumIn() != 1 {
		return nil
	}

	// mt.In(0) 是 slice 元素类型（可能为命名类型），取其 Elem
	elem := mt.In(0).Elem()
	// 元素应为函数且第一个参数为 *sql.Selector
	selPtrType := reflect.TypeOf((*sql.Selector)(nil))
	if elem.Kind() != reflect.Func || elem.NumIn() < 1 || elem.In(0) != selPtrType {
		return nil
	}

	// 通用实现（原生类型 func(*sql.Selector)）
	fn := func(s *sql.Selector) {
		s.Where(sql.EQ(s.C("tenant_id"), tenantID))
	}
	valFn := reflect.ValueOf(fn)

	// 若目标类型与匿名函数类型不一致，尝试转换或用 MakeFunc 生成目标类型
	if valFn.Type() != elem {
		if valFn.Type().ConvertibleTo(elem) {
			valFn = valFn.Convert(elem)
		} else {
			valFn = reflect.MakeFunc(elem, func(in []reflect.Value) []reflect.Value {
				// 第一个参数为 *sql.Selector
				s := in[0].Interface().(*sql.Selector)
				fn(s)
				return nil
			})
		}
	}

	// 构造变参 slice 并调用 Where
	slice := reflect.MakeSlice(reflect.SliceOf(elem), 1, 1)
	slice.Index(0).Set(valFn)
	mf.CallSlice([]reflect.Value{slice})

	return nil
}

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
