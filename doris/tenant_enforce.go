package doris

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/tx7do/go-crud/doris/query"
	"github.com/tx7do/go-crud/viewer"
)

// InjectTenantFilterIntoBuilder 在租户业务视图下向 doris query.Builder
// 注入 tenant_id 谓词。语义与 entgo TenantPrivacy.EvalQuery 一致：
//   - 缺 ViewerContext → ErrMissingViewer（fail-closed）
//   - 平台/系统视图 → 不注入（放行）
//   - 租户业务视图 → 注入 "tenant_id = ?" 参数化谓词
//
// 仅对 ENTITY 类型实现 viewer.ScopedModel 生效（非 tenant 实体跳过）。
func InjectTenantFilterIntoBuilder[ENTITY any](ctx context.Context, qb *query.Builder) error {
	if qb == nil {
		return nil
	}
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
	qb.Where("tenant_id = ?", dec.TenantID)
	return nil
}

// InjectTenantFilterIntoBaseWhere 处理 Count/Exists/Get 的原始 baseWhere
// 路径（这些方法绕过 query.Builder，直接接收 SQL 片段与参数）。
// 在租户业务视图下追加 " AND tenant_id = ?" 并把 tid 加入 whereArgs。
// 返回新的 baseWhere 与 whereArgs；非 tenant 实体或平台视图原样返回。
func InjectTenantFilterIntoBaseWhere[ENTITY any](ctx context.Context, baseWhere string, whereArgs []any) (string, []any, error) {
	if !viewer.IsTenantScopedType[ENTITY]() {
		return baseWhere, whereArgs, nil
	}
	dec, err := viewer.EnforceTenant(ctx)
	if err != nil {
		return "", nil, err
	}
	if !dec.Enforce {
		return baseWhere, whereArgs, nil
	}
	var newWhere string
	bw := strings.TrimSpace(baseWhere)
	if bw == "" {
		newWhere = "WHERE tenant_id = ?"
	} else {
		upper := strings.ToUpper(bw)
		if !strings.HasPrefix(upper, "WHERE") {
			newWhere = "WHERE " + bw + " AND tenant_id = ?"
		} else {
			newWhere = bw + " AND tenant_id = ?"
		}
	}
	newArgs := append(whereArgs, dec.TenantID)
	return newWhere, newArgs, nil
}

// enforceTenantOnBatchItem 对批量插入的单个元素执行租户强制。
//
// B.12 修复：BatchInsert 的 map 路径此前仅校验 viewer 是否存在，不向 map
// 强制写入 tenant_id，调用方可传不含 tenant_id 的 map 导致 NULL 落库
// （对租户读不可见但持久存在——数据隔离缺口）。struct 路径同样未做
// per-item EnforceOnScopedInstance（与 Create 不一致）。
//
// 本函数在每个元素提取列/值之前统一强制：
//   - map 路径：tenant-scoped 实体在租户业务视图下强制覆盖 map["tenant_id"]
//     （覆盖调用方值，与 Create 的 force-set 语义一致）。
//   - struct 路径：若可取址，调用 EnforceOnScopedInstanceAny 强制 SetTenantID。
//     非可取址（按值数组）→ fail-closed 拒绝。
//   - 非 tenant-scoped 实体（IsTenantScopedType=false）直接返回 nil。
func enforceTenantOnBatchItem[ENTITY any](ctx context.Context, rv reflect.Value, idx int) error {
	if !viewer.IsTenantScopedType[ENTITY]() {
		return nil
	}
	item := rv.Index(idx)
	switch item.Kind() {
	case reflect.Map:
		m, ok := item.Interface().(map[string]any)
		if !ok {
			return nil
		}
		dec, err := viewer.EnforceTenant(ctx)
		if err != nil {
			return err
		}
		if dec.Enforce {
			m["tenant_id"] = uint32(dec.TenantID)
		}
		return nil
	case reflect.Struct:
		if !item.CanAddr() {
			return fmt.Errorf("security: cannot enforce tenant_id on non-addressable struct in batch")
		}
		return viewer.EnforceOnScopedInstanceAny(ctx, item.Addr().Interface())
	}
	return nil
}
