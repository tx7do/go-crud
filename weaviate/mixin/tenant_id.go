package mixin

import (
	"github.com/tx7do/go-crud/viewer"
)

// TenantID 是 Weaviate 可复用的 mixin，表示租户 ID。
// Weaviate 对象属性为 JSON 语义，租户标识以整数属性（tenant_id）存储；
// 嵌入此 mixin 的实体自动启用租户隔离强制（与 entgo TenantPrivacy 一致）：
//   - Create：租户业务视图下强制覆盖 tenant_id 为当前 Viewer 的租户。
//   - GetByUUID/Query/Count/DeleteByUUIDs/DeleteByFilter/SearchByVector：
//     repository 注入 tenant_id 相等 Where 条件限定到当前 Viewer 租户
//     （配合属性倒排索引）；按 UUID 直取的路径客户端校验属性租户。
//   - 缺 ViewerContext → 中止（fail-closed）；平台/系统视图 → 放行。
//
// 注：Weaviate 原生多租户（collection tenants，租户级硬隔离）不在本模块
// 适配面内——本模块与其余引擎保持同一编程模型（属性级隔离 + fail-closed）；
// 需要租户级硬隔离的场景建议直接使用官方客户端的 tenants API。
//
// 实现 viewer.ScopedModel 标记接口供 repository 类型断言识别（无需反射检测）。
type TenantID struct {
	TenantID *uint32 `json:"tenant_id,omitempty"`
}

// GetTenantID 实现 viewer.ScopedModel。
func (m *TenantID) GetTenantID() *uint32 { return m.TenantID }

// SetTenantID 实现 viewer.ScopedModel，供 Create 路径强制注入。
func (m *TenantID) SetTenantID(tid uint32) {
	v := tid
	m.TenantID = &v
}

// 接口断言（编译期保证）。
var _ viewer.ScopedModel = (*TenantID)(nil)
