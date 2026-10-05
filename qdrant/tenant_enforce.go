package qdrant

import (
	"context"
	"math"

	qdrant "github.com/qdrant/go-client/qdrant"

	"github.com/tx7do/go-wind/errors"

	"github.com/tx7do/go-crud/viewer"
)

// InjectTenantFilterIntoQdrantFilter 在租户业务视图下向 Qdrant Filter
// 注入 tenant_id 匹配条件。语义与 entgo TenantPrivacy.EvalQuery 一致：
//   - 缺 ViewerContext → ErrMissingViewer（fail-closed）
//   - 平台/系统视图 → 不注入（放行）
//   - 租户业务视图 → 注入 tenant_id == tid 条件，并与调用方已有条件做 AND 合并
//
// 仅当 ENTITY 类型实现 viewer.ScopedModel（嵌入 mixin.TenantID）才注入。
// 调用方传入的原 Filter 通过 FilterAsCondition 包装为单一条件后与新条件
// 一并入 Must（AND 语义），原有 must/should/must_not 结构保持不变。
func InjectTenantFilterIntoQdrantFilter[ENTITY any](ctx context.Context, filter *qdrant.Filter) (*qdrant.Filter, error) {
	// 类型层检测：ENTITY 是否嵌入了 TenantID mixin（实现 ScopedModel）。
	if !viewer.IsTenantScopedType[ENTITY]() {
		return filter, nil
	}
	dec, err := viewer.EnforceTenant(ctx)
	if err != nil {
		return nil, err
	}
	if !dec.Enforce {
		return filter, nil
	}

	tenantCond := qdrant.NewMatchInt("tenant_id", int64(dec.TenantID))
	if filter == nil {
		return &qdrant.Filter{Must: []*qdrant.Condition{tenantCond}}, nil
	}
	return &qdrant.Filter{
		Must: []*qdrant.Condition{
			qdrant.NewFilterAsCondition(filter),
			tenantCond,
		},
	}, nil
}

// errTenantMismatch 表示取回点的载荷租户与当前 Viewer 租户不一致。
// 不导出：对外语义统一折叠为 ErrPointNotFound（未找到），避免存在性泄露。
var errTenantMismatch = errors.PermissionDenied("TENANT_MISMATCH")

// payloadTenantID 从载荷中读取 tenant_id 字段（mixin.TenantID 写入的整数载荷）。
// 字段缺失、类型不符或超出 uint32 范围均视为无租户标识。
func payloadTenantID(payload map[string]*qdrant.Value) (uint32, bool) {
	v, ok := payload["tenant_id"]
	if !ok || v == nil || v.Kind == nil {
		return 0, false
	}
	iv, ok := v.Kind.(*qdrant.Value_IntegerValue)
	if !ok {
		return 0, false
	}
	if iv.IntegerValue < 0 || iv.IntegerValue > math.MaxUint32 {
		return 0, false
	}
	return uint32(iv.IntegerValue), true
}

// verifyTenantOnPayload 校验按 ID 直接取回的点的载荷租户与当前 Viewer 一致。
//
// Qdrant 的 GetPoints 协议没有 Filter 字段，按 ID 取点无法服务端过滤，
// 故在客户端做等价校验（Query/Search 路径有 Filter，本函数仅作纵深防御）。
// 语义与 InjectTenantFilterIntoQdrantFilter 一致：
//   - 非 tenant-scoped 实体 → 放行；
//   - 缺 ViewerContext → ErrMissingViewer（fail-closed）；
//   - 平台/系统视图 → 放行；
//   - 租户业务视图 → 载荷租户与 Viewer 租户不一致 → errTenantMismatch。
func verifyTenantOnPayload[ENTITY any](ctx context.Context, payload map[string]*qdrant.Value) error {
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
	tid, ok := payloadTenantID(payload)
	if !ok || tid != uint32(dec.TenantID) {
		return errTenantMismatch
	}
	return nil
}
