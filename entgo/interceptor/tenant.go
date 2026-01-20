package interceptor

import (
	"context"
	"fmt"
	"reflect"

	"entgo.io/ent"
	"entgo.io/ent/dialect/sql"
	"github.com/tx7do/go-crud/viewer"
)

// TenantInterceptor 这是一个通用的租户拦截器
func TenantInterceptor() ent.Interceptor {
	return ent.InterceptFunc(func(next ent.Querier) ent.Querier {
		return ent.QuerierFunc(func(ctx context.Context, query ent.Query) (ent.Value, error) {
			vc, exist := viewer.FromContext(ctx)
			// 如果身份丢失，安全起见应直接拒绝操作（Deny），而不是跳过
			if !exist {
				return nil, fmt.Errorf("security: missing ViewerContext in context")
			}

			// 平台管理视图/系统视图放行：允许查看全量数据
			if vc.IsPlatformContext() || vc.IsSystemContext() {
				return next.Query(ctx, query)
			}

			tid := vc.TenantID()

			if err := injectTenantWhere(query, tid); err != nil {
				return nil, err
			}

			return next.Query(ctx, query)
		})
	})
}

// injectTenantWhere 尝试通过反射在 query 上调用 Where\(...\) 并注入 tenant_id 过滤。
// 返回可能被 Where 链式调用替换后的 ent.Query（若 Where 返回链式值）。
func injectTenantWhere(query ent.Query, tenantID uint64) error {
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
