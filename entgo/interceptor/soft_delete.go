package interceptor

import (
	"context"
	"fmt"
	"reflect"

	"entgo.io/ent"
	"entgo.io/ent/dialect/sql"
)

// SoftDeleteInterceptor implements a soft delete interceptor
func SoftDeleteInterceptor() ent.Interceptor {
	return ent.InterceptFunc(func(next ent.Querier) ent.Querier {
		return ent.QuerierFunc(func(ctx context.Context, query ent.Query) (ent.Value, error) {
			if err := injectSoftDeleteWhere(query); err != nil {
				return nil, err
			}

			return next.Query(ctx, query)
		})
	})
}

func injectSoftDeleteWhere(query ent.Query) error {
	rv := reflect.ValueOf(query)
	mf := rv.MethodByName("Where")
	if !mf.IsValid() || mf.Kind() != reflect.Func {
		// fail-closed：注入失败必须报错而非静默跳过，
		// 否则 deleted_at 过滤悄悄消失、已删除数据被重新暴露。
		return fmt.Errorf("security: soft-delete where-injection failed: query has no usable Where method (%T)", query)
	}

	mt := mf.Type()
	if !mt.IsVariadic() || mt.NumIn() != 1 {
		return fmt.Errorf("security: soft-delete where-injection failed: unexpected Where signature (%T)", query)
	}

	elem := mt.In(0).Elem()
	selPtrType := reflect.TypeOf((*sql.Selector)(nil))
	if elem.Kind() != reflect.Func || elem.NumIn() < 1 || elem.In(0) != selPtrType {
		return fmt.Errorf("security: soft-delete where-injection failed: unexpected predicate signature (%T)", query)
	}

	fn := func(s *sql.Selector) {
		s.Where(sql.IsNull(s.C("deleted_at")))
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
