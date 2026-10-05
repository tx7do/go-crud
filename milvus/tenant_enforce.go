package milvus

import (
	"context"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-sdk-go/v2/client"

	"github.com/tx7do/go-wind/errors"

	"github.com/tx7do/go-crud/viewer"
)

// InjectTenantFilterIntoExpr 在租户业务视图下向 Milvus 查询表达式注入
// tenant_id 匹配谓词。语义与 entgo TenantPrivacy.EvalQuery 一致：
//   - 缺 ViewerContext → ErrMissingViewer（fail-closed）
//   - 平台/系统视图 → 不注入（放行）
//   - 租户业务视图 → 注入 tenant_id == tid 并与调用方已有表达式 AND 合并
//
// 仅当 ENTITY 类型实现 viewer.ScopedModel（嵌入 mixin.TenantID）才注入。
// 租户值经 strconv.FormatUint 数值化，无注入面。
func InjectTenantFilterIntoExpr[ENTITY any](ctx context.Context, expr string) (string, error) {
	if !viewer.IsTenantScopedType[ENTITY]() {
		return expr, nil
	}
	dec, err := viewer.EnforceTenant(ctx)
	if err != nil {
		return "", err
	}
	if !dec.Enforce {
		return expr, nil
	}
	tenantPred := "tenant_id == " + strconv.FormatUint(dec.TenantID, 10)
	if strings.TrimSpace(expr) == "" {
		return tenantPred, nil
	}
	return "(" + expr + ") and " + tenantPred, nil
}

// errTenantMismatch 表示取回行的 tenant_id 列与当前 Viewer 租户不一致。
// 不导出：对外语义统一折叠为 ErrPointNotFound（未找到），避免存在性泄露。
var errTenantMismatch = errors.PermissionDenied("TENANT_MISMATCH")

// checkTenantColumn 校验按主键直取的行的 tenant_id 列与当前 Viewer 一致。
//
// 按主键取行的请求（QueryByPks）无法携带过滤表达式，无法服务端过滤，
// 故在客户端做等价校验（Query/Search 路径有表达式注入，本函数仅作纵深防御）。
// 语义与 InjectTenantFilterIntoExpr 一致：
//   - 非 tenant-scoped 实体 → 放行；
//   - 缺 ViewerContext → ErrMissingViewer（fail-closed）；
//   - 平台/系统视图 → 放行；
//   - 租户业务视图 → tenant_id 列缺失或与 Viewer 租户不一致 → errTenantMismatch。
func checkTenantColumn[ENTITY any](ctx context.Context, rs client.ResultSet, rowIdx int) error {
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
	col := rs.GetColumn("tenant_id")
	if col == nil {
		return errTenantMismatch
	}
	v, gerr := col.Get(rowIdx)
	if gerr != nil {
		return errTenantMismatch
	}
	tid, ok := v.(int64)
	if !ok || tid <= 0 || tid != int64(dec.TenantID) {
		return errTenantMismatch
	}
	return nil
}
