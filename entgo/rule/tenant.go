package rule

import (
	"context"
	"fmt"
	"reflect"

	"entgo.io/ent"
	"entgo.io/ent/dialect/sql"
	"entgo.io/ent/entql"

	"github.com/tx7do/go-crud/viewer"
)

// Filter is the interface that wraps the Where function for filtering nodes
// in queries and mutations. Retained for system.go's OwnerOnlyRule /
// PermissionRule / SoftDeleteRule helpers (themselves currently call-site-free;
// cleanup tracked separately).
type (
	Filter interface {
		Where(entql.P)
	}
)

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

	// 更新/删除：ent mutation 不支持注入行级谓词（生成的 mutation 无 AddPredicate），
	// 行级租户过滤依赖查询侧 EvalQuery + 拦截器；此处至少阻断跨租户逃逸——
	// 非平台上下文触碰 tenant_id 时，仅允许"值不变的冗余设置"（旧行、新值、
	// 当前访问者三者同租户），把记录移到其他租户或改他租户记录一律拒绝。
	if !op.Is(ent.OpCreate) {
		if vc.IsPlatformContext() || vc.IsSystemContext() {
			return nil
		}
		if val, set := m.Field("tenant_id"); set {
			if old, ok := m.(interface {
				OldTenantID(context.Context) (*T, error)
			}); ok {
				prev, err := old.OldTenantID(ctx)
				if err != nil {
					return fmt.Errorf("security: tenant rule cannot verify tenant_id change: %w", err)
				}
				viewerTid := fmt.Sprint(T(vc.TenantID()))
				if prev == nil || fmt.Sprint(*prev) != fmt.Sprint(val) || viewerTid != fmt.Sprint(val) {
					return fmt.Errorf("security: cross-tenant tenant_id change denied")
				}
			} else {
				return fmt.Errorf("security: tenant rule cannot verify tenant_id change on %s", op)
			}
		}
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
		// fail-closed：反射注入失败时必须报错而非静默跳过，
		// 否则租户过滤悄悄消失（ent 升级改签名即触发）。
		return fmt.Errorf("security: tenant where-injection failed: query has no usable Where method (%T)", query)
	}

	mt := mf.Type()
	// 期待形如 Where(...T) 且只有一个参数（变参）
	if !mt.IsVariadic() || mt.NumIn() != 1 {
		return fmt.Errorf("security: tenant where-injection failed: unexpected Where signature (%T)", query)
	}

	// mt.In(0) 是 slice 元素类型（可能为命名类型），取其 Elem
	elem := mt.In(0).Elem()
	// 元素应为函数且第一个参数为 *sql.Selector
	selPtrType := reflect.TypeOf((*sql.Selector)(nil))
	if elem.Kind() != reflect.Func || elem.NumIn() < 1 || elem.In(0) != selPtrType {
		return fmt.Errorf("security: tenant where-injection failed: unexpected predicate signature (%T)", query)
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

// InjectTenantWhereIntoBuilder 是 injectTenantWhere 的通用版本，作用于
// UpdateBuilder / DeleteBuilder / UpdateOneBuilder / DeleteOneBuilder 等
// 暴露 Where(...func(*sql.Selector)) 的变更构建器（而非 ent.Query）。
//
// 这用于闭合 entgo 写侧行级缺口（R-1）：EvalQuery 只在查询侧注入 tenant_id，
// 生成的 Update/Delete mutation 无 AddPredicate，但 builder.Where(predicates...)
// 会流入 _spec.Predicate → UpdateNodes/DeleteNodes 的行级 WHERE。本函数在
// repository 层把这些构建器纳入与查询侧一致的租户谓词注入。
//
// 语义与 EnforceTenant/TenantPrivacy 一致：缺身份 fail-closed，平台/系统
// 放行，租户业务视图注入 tenant_id = ?。
//
// 与 doris/clickhouse/mongodb 的 InjectTenantFilterIntoBuilder 对齐：仅对
// 实现 viewer.ScopedModel 的 ENTITY 类型生效（非 tenant 实体直接放行），
// 类型门控置于最前，避免对无 tenant_id 列的非 tenant 表注入谓词。
func InjectTenantWhereIntoBuilder[ENTITY any](ctx context.Context, builder any) error {
	if !viewer.IsTenantScopedType[ENTITY]() {
		return nil
	}
	dec, err := viewer.EnforceTenant(ctx)
	if err != nil {
		return err
	}
	if !dec.Enforce {
		return nil
	}
	return injectTenantWhereReflect(builder, dec.TenantID)
}

// injectTenantWhereReflect 是 InjectTenantWhereIntoBuilder 的反射核心，
// 与 TenantPrivacy.injectTenantWhere 同构，区别仅在于目标对象不是 ent.Query。
func injectTenantWhereReflect(builder any, tenantID uint64) error {
	if builder == nil {
		return nil
	}
	rv := reflect.ValueOf(builder)
	mf := rv.MethodByName("Where")
	if !mf.IsValid() || mf.Kind() != reflect.Func {
		// fail-closed：反射注入失败时必须报错而非静默跳过，
		// 否则租户过滤悄悄消失（ent 升级改签名即触发）。
		return fmt.Errorf("security: tenant where-injection failed: builder has no usable Where method (%T)", builder)
	}

	mt := mf.Type()
	if !mt.IsVariadic() || mt.NumIn() != 1 {
		return fmt.Errorf("security: tenant where-injection failed: unexpected Where signature (%T)", builder)
	}
	elem := mt.In(0).Elem()
	selPtrType := reflect.TypeOf((*sql.Selector)(nil))
	if elem.Kind() != reflect.Func || elem.NumIn() < 1 || elem.In(0) != selPtrType {
		return fmt.Errorf("security: tenant where-injection failed: unexpected predicate signature (%T)", builder)
	}

	fn := func(s *sql.Selector) {
		s.Where(sql.EQ(s.C("tenant_id"), tenantID))
	}
	valFn := reflect.ValueOf(fn)

	if valFn.Type() != elem {
		if valFn.Type().ConvertibleTo(elem) {
			valFn = valFn.Convert(elem)
		} else {
			valFn = reflect.MakeFunc(elem, func(in []reflect.Value) []reflect.Value {
				s := in[0].Interface().(*sql.Selector)
				fn(s)
				return nil
			})
		}
	}

	slice := reflect.MakeSlice(reflect.SliceOf(elem), 1, 1)
	slice.Index(0).Set(valFn)
	mf.CallSlice([]reflect.Value{slice})

	return nil
}
