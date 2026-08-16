package doris

import (
	"context"
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
