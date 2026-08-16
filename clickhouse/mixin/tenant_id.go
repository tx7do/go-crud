package mixin

import (
	"github.com/tx7do/go-crud/viewer"
)

// TenantID 是 ClickHouse 可复用的 mixin，表示租户 ID。
// 嵌入此 mixin 的实体自动启用租户隔离强制（与 entgo TenantPrivacy 一致）：
//   - Create：租户业务视图下强制覆盖 tenant_id 为当前 Viewer 的租户。
//   - Query/Update/Delete：repository 注入 tenant_id 谓词限定到当前 Viewer 租户。
//   - 缺 ViewerContext → 中止（fail-closed）；平台/系统视图 → 放行。
//
// 实现 viewer.ScopedModel 标记接口供 repository 类型断言识别。
type TenantID struct {
	TenantID *uint32 `db:"tenant_id" json:"tenant_id"`
}

// GetTenantID 实现 viewer.ScopedModel。
func (m *TenantID) GetTenantID() *uint32 { return m.TenantID }

// SetTenantID 实现 viewer.ScopedModel，供 Create 路径强制注入。
func (m *TenantID) SetTenantID(tid uint32) {
	v := tid
	m.TenantID = &v
}

var _ viewer.ScopedModel = (*TenantID)(nil)
