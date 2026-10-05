package neo4j

import (
	"context"
	"fmt"
	"math"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"

	"github.com/tx7do/go-wind/errors"

	"github.com/tx7do/go-crud/viewer"
)

// tenantPredicateParam 租户谓词的保留参数名。
const tenantPredicateParam = "__tid"

// tenantPredicateBody 租户谓词的 Cypher 片段（值经保留参数传递，不在文本中拼接）。
const tenantPredicateBody = "n.`tenant_id` = $__tid"

// InjectTenantPredicate 在租户业务视图下向 WHERE 子句追加 tenant_id 匹配谓词。
// 语义与 entgo TenantPrivacy.EvalQuery / qdrant InjectTenantFilterIntoQdrantFilter 一致：
//   - 缺 ViewerContext → ErrMissingViewer（fail-closed）
//   - 平台/系统视图 → 不注入（放行）
//   - 租户业务视图 → WHERE 追加 tenant_id 匹配谓词（参数化，值取当前 Viewer 租户）
//
// 仅当 ENTITY 类型实现 viewer.ScopedModel（嵌入 neo4j/mixin.TenantID）才注入。
// Cypher 的 WHERE 是可组合的字符串片段：调用方条件被括号包裹后与新谓词
// AND 合并；调用方参数表被复制后并入保留参数（不原地改动调用方表），
// 与保留参数名冲突时拒绝。
func InjectTenantPredicate[ENTITY any](ctx context.Context, where string, params map[string]any) (string, map[string]any, error) {
	// 类型层检测：ENTITY 是否嵌入了 TenantID mixin（实现 ScopedModel）。
	if !viewer.IsTenantScopedType[ENTITY]() {
		return where, params, nil
	}
	dec, err := viewer.EnforceTenant(ctx)
	if err != nil {
		return "", nil, err
	}
	if !dec.Enforce {
		return where, params, nil
	}
	if _, exists := params[tenantPredicateParam]; exists {
		return "", nil, fmt.Errorf("%w: param %q is reserved for tenant isolation", ErrInvalidRequest, tenantPredicateParam)
	}

	merged := make(map[string]any, len(params)+1)
	for k, v := range params {
		merged[k] = v
	}
	merged[tenantPredicateParam] = int64(dec.TenantID)

	if where == "" {
		return tenantPredicateBody, merged, nil
	}
	return "(" + where + ") AND " + tenantPredicateBody, merged, nil
}

// errTenantMismatch 表示取回节点的属性租户与当前 Viewer 租户不一致。
// 不导出：对外语义统一折叠为 ErrPointNotFound（未找到），避免存在性泄露。
var errTenantMismatch = errors.PermissionDenied("TENANT_MISMATCH")

// nodeTenantID 从节点属性中读取 tenant_id 字段（mixin.TenantID 写入的整数属性）。
// 字段缺失、类型不符或超出 uint32 范围均视为无租户标识。
func nodeTenantID(node neo4j.Node) (uint32, bool) {
	v, ok := node.Props["tenant_id"]
	if !ok {
		return 0, false
	}
	iv, ok := v.(int64)
	if !ok {
		return 0, false
	}
	if iv < 0 || iv > math.MaxUint32 {
		return 0, false
	}
	return uint32(iv), true
}

// verifyTenantOnNode 校验按 element id 直取的节点的属性租户与当前 Viewer 一致。
//
// 语义与 InjectTenantPredicate 一致：
//   - 非 tenant-scoped 实体 → 放行；
//   - 缺 ViewerContext → ErrMissingViewer（fail-closed）；
//   - 平台/系统视图 → 放行；
//   - 租户业务视图 → 属性租户与 Viewer 租户不一致 → errTenantMismatch。
//
// Query/Count/Delete 等路径的 WHERE 已服务端注入租户谓词，本函数仅作
// 纵深防御（防御谓词注入失效或协议旁路）。
func verifyTenantOnNode[ENTITY any](ctx context.Context, node neo4j.Node) error {
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
	tid, ok := nodeTenantID(node)
	if !ok || tid != uint32(dec.TenantID) {
		return errTenantMismatch
	}
	return nil
}
