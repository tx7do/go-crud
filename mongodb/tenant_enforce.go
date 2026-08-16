package mongodb

import (
	"context"

	bsonV2 "go.mongodb.org/mongo-driver/v2/bson"

	"github.com/tx7do/go-crud/mongodb/query"
	"github.com/tx7do/go-crud/viewer"
)

// InjectTenantFilterIntoBuilder 在租户业务视图下向 mongodb query.Builder
// 注入 tenant_id 谓词。语义与 entgo TenantPrivacy.EvalQuery 一致：
//   - 缺 ViewerContext → ErrMissingViewer（fail-closed）
//   - 平台/系统视图 → 不注入（放行）
//   - 租户业务视图 → 注入 {tenant_id: tid} 合并进现有 filter
//
// onlyIfScoped 为 true 时，仅当 ENTITY 类型实现 viewer.ScopedModel 才注入
// （非 tenant 实体跳过）；为 false 时强制注入（用于已确认 scoped 的路径）。
func InjectTenantFilterIntoBuilder[ENTITY any](ctx context.Context, qb *query.Builder) error {
	if qb == nil {
		return nil
	}
	// 类型层检测：ENTITY 是否嵌入了 TenantID mixin（实现 ScopedModel）。
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
	// 合并 tenant_id 谓词进现有 filter（Where 保留调用方其余条件）。
	qb.Where(bsonV2.M{"tenant_id": dec.TenantID})
	return nil
}
