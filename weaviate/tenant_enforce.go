package weaviate

import (
	"context"
	"math"

	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"

	"github.com/tx7do/go-wind/errors"

	"github.com/tx7do/go-crud/viewer"
)

// InjectTenantFilter 在租户业务视图下向 Where 条件注入 tenant_id 匹配条件。
// 语义与 entgo TenantPrivacy.EvalQuery / qdrant InjectTenantFilterIntoQdrantFilter 一致：
//   - 缺 ViewerContext → ErrMissingViewer（fail-closed）
//   - 平台/系统视图 → 不注入（放行）
//   - 租户业务视图 → 注入 tenant_id == tid 条件，并与调用方已有条件
//     And 合并（原条件语义保留）
//
// 仅当 ENTITY 类型实现 viewer.ScopedModel（嵌入 weaviate/mixin.TenantID）才注入。
func InjectTenantFilter[ENTITY any](ctx context.Context, fb *filters.WhereBuilder) (*filters.WhereBuilder, error) {
	// 类型层检测：ENTITY 是否嵌入了 TenantID mixin（实现 ScopedModel）。
	if !viewer.IsTenantScopedType[ENTITY]() {
		return fb, nil
	}
	dec, err := viewer.EnforceTenant(ctx)
	if err != nil {
		return nil, err
	}
	if !dec.Enforce {
		return fb, nil
	}

	tenantCond := tenantIDFilter(int64(dec.TenantID))
	if fb == nil {
		return tenantCond, nil
	}
	return filters.Where().WithOperator(filters.And).WithOperands([]*filters.WhereBuilder{fb, tenantCond}), nil
}

// tenantIDFilter 构造 tenant_id 相等的 Where 条件（int 属性）。
func tenantIDFilter(tid int64) *filters.WhereBuilder {
	return filters.Where().
		WithPath([]string{"tenant_id"}).
		WithOperator(filters.Equal).
		WithValueInt(tid)
}

// errTenantMismatch 表示取回对象的属性租户与当前 Viewer 租户不一致。
// 不导出：对外语义统一折叠为 ErrPointNotFound（未找到），避免存在性泄露。
var errTenantMismatch = errors.PermissionDenied("TENANT_MISMATCH")

// propsTenantID 从对象属性中读取 tenant_id 字段（mixin.TenantID 写入的
// 整数属性）。字段缺失、类型不符或超出 uint32 范围均视为无租户标识。
// 服务端 JSON 数值为 float64，离线替身/内部路径可能给出整型。
func propsTenantID(props map[string]any) (uint32, bool) {
	v, ok := props["tenant_id"]
	if !ok || v == nil {
		return 0, false
	}
	switch iv := v.(type) {
	case float64:
		if iv < 0 || iv > math.MaxUint32 {
			return 0, false
		}
		return uint32(iv), true
	case int64:
		if iv < 0 || iv > math.MaxUint32 {
			return 0, false
		}
		return uint32(iv), true
	case int:
		if iv < 0 || iv > math.MaxUint32 {
			return 0, false
		}
		return uint32(iv), true
	case uint32:
		return iv, true
	default:
		return 0, false
	}
}

// verifyTenantOnProps 校验按 UUID 直取的对象的属性租户与当前 Viewer 一致。
//
// 语义与 InjectTenantFilter 一致：
//   - 非 tenant-scoped 实体 → 放行；
//   - 缺 ViewerContext → ErrMissingViewer（fail-closed）；
//   - 平台/系统视图 → 放行；
//   - 租户业务视图 → 属性租户与 Viewer 租户不一致 → errTenantMismatch。
//
// Query/Count/Delete 等路径的 Where 已服务端注入租户条件，本函数仅作
// 纵深防御（GetByUUID 直取路径叠加校验）。
func verifyTenantOnProps[ENTITY any](ctx context.Context, props map[string]any) error {
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
	tid, ok := propsTenantID(props)
	if !ok || tid != uint32(dec.TenantID) {
		return errTenantMismatch
	}
	return nil
}
