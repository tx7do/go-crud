package cassandra

import (
	"context"
	"math"

	"github.com/tx7do/go-wind/errors"

	"github.com/tx7do/go-crud/viewer"
)

// InjectTenantWhere 在租户业务视图下向 WHERE 子句追加 tenant_id 匹配谓词。
// 语义与 entgo TenantPrivacy.EvalQuery / 其余引擎的注入器一致：
//   - 缺 ViewerContext → ErrMissingViewer（fail-closed）
//   - 平台/系统视图 → 不注入（放行）
//   - 租户业务视图 → WHERE 追加 `tenant_id = ?`（占位符参数化，值取当前
//     Viewer 租户并按位置追加到参数表尾）
//
// 仅当 ENTITY 类型实现 viewer.ScopedModel（嵌入 cassandra/mixin.TenantID）
// 才注入。CQL 使用位置占位符（?），调用方条件被括号包裹后与新谓词 AND
// 合并，无参数名冲突问题。
//
// 注意：谓词命中非主键列——表须建 tenant_id 二级索引，或调用方在 Query
// 上置 AllowFiltering，否则服务端拒绝（模块 README 有 DDL 示例）。
func InjectTenantWhere[ENTITY any](ctx context.Context, where string, args []any) (string, []any, error) {
	// 类型层检测：ENTITY 是否嵌入了 TenantID mixin（实现 ScopedModel）。
	if !viewer.IsTenantScopedType[ENTITY]() {
		return where, args, nil
	}
	dec, err := viewer.EnforceTenant(ctx)
	if err != nil {
		return "", nil, err
	}
	if !dec.Enforce {
		return where, args, nil
	}

	merged := make([]any, 0, len(args)+1)
	merged = append(merged, args...)
	merged = append(merged, int64(dec.TenantID))

	if where == "" {
		return "tenant_id = ?", merged, nil
	}
	return "(" + where + ") AND tenant_id = ?", merged, nil
}

// errTenantMismatch 表示取回行的列租户与当前 Viewer 租户不一致。
// 不导出：对外语义统一折叠为 ErrPointNotFound（未找到），避免存在性泄露。
var errTenantMismatch = errors.PermissionDenied("TENANT_MISMATCH")

// rowTenantID 从行映射中读取 tenant_id 列（mixin.TenantID 写入的 bigint）。
// 列缺失、类型不符或超出 uint32 范围均视为无租户标识。
func rowTenantID(row map[string]any) (uint32, bool) {
	v, ok := row["tenant_id"]
	if !ok || v == nil {
		return 0, false
	}
	var iv int64
	switch n := v.(type) {
	case int64:
		iv = n
	case int:
		iv = int64(n)
	default:
		return 0, false
	}
	if iv < 0 || iv > math.MaxUint32 {
		return 0, false
	}
	return uint32(iv), true
}

// verifyTenantOnRow 校验按主键直取的行的列租户与当前 Viewer 一致。
//
// 语义与 InjectTenantWhere 一致：
//   - 非 tenant-scoped 实体 → 放行；
//   - 缺 ViewerContext → ErrMissingViewer（fail-closed）；
//   - 平台/系统视图 → 放行；
//   - 租户业务视图 → 列租户与 Viewer 租户不一致 → errTenantMismatch。
//
// 主键直取（Get/GetByUUID）路径不叠加服务端过滤（CQL 的主键外 WHERE 需
// 索引且 DELETE 不支持），本函数承担该路径的租户校验；Query 等可注入
// 路径的 WHERE 已服务端注入，此处仅作纵深防御。
func verifyTenantOnRow[ENTITY any](ctx context.Context, row map[string]any) error {
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
	tid, ok := rowTenantID(row)
	if !ok || tid != uint32(dec.TenantID) {
		return errTenantMismatch
	}
	return nil
}
