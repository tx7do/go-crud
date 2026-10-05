package mixin

import (
	"math"

	"github.com/tx7do/go-crud/viewer"
)

// TenantID 是 Neo4j 可复用的 mixin，表示租户 ID。
// 节点属性仅有 64 位整数类型，租户标识以 int64 属性存储（名称固定
// tenant_id，见 neo4j 模块的属性映射器）。嵌入此 mixin 的实体自动启用
// 租户隔离强制（与 entgo TenantPrivacy 一致）：
//   - Create/BatchCreate：租户业务视图下强制覆盖 tenant_id 属性。
//   - GetByUUID/Query/Count/Exists/DeleteByUUIDs/DeleteByWhere：
//     repository 在 WHERE 子句注入 tenant_id 匹配谓词（服务端过滤）；
//     按 element id 直取的路径另有客户端属性租户校验（纵深防御）。
//   - 缺 ViewerContext → 中止（fail-closed）；平台/系统视图 → 放行。
//
// 实现 viewer.ScopedModel 标记接口供 repository 类型断言识别（无需反射检测）。
type TenantID struct {
	TenantID int64 `neo4j:"name:tenant_id"`
}

// GetTenantID 实现 viewer.ScopedModel。
// int64 → uint32 有损转换：非正数或超出 uint32 范围视为无租户标识。
func (m *TenantID) GetTenantID() *uint32 {
	if m.TenantID <= 0 || m.TenantID > math.MaxUint32 {
		return nil
	}
	v := uint32(m.TenantID)
	return &v
}

// SetTenantID 实现 viewer.ScopedModel，供 Create 路径强制注入。
func (m *TenantID) SetTenantID(tid uint32) {
	m.TenantID = int64(tid)
}

// 接口断言（编译期保证）。
var _ viewer.ScopedModel = (*TenantID)(nil)
