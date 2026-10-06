package mixin

import (
	"math"

	"github.com/tx7do/go-crud/viewer"
)

// TenantID 是 Cassandra 可复用的 mixin，表示租户 ID。
// CQL 仅有有符号整数类型，租户标识以 bigint 列存储（名称固定 tenant_id，
// 见 cassandra 模块的列映射器）。嵌入此 mixin 的实体自动启用租户隔离
// 强制（与 entgo TenantPrivacy 一致）：
//   - Create/BatchCreate：租户业务视图下强制覆盖 tenant_id 列。
//   - Get/GetByUUID：主键直取路径客户端校验行租户（CQL 的 DELETE 与
//     主键外条件不兼容，直取亦不叠加服务端过滤）。
//   - Query/Count/Exists/DeleteByIDs/DeleteByUUIDs/DeleteByWhere：
//     WHERE 注入 tenant_id = ? 谓词（服务端过滤；表须建 tenant_id
//     二级索引，或查询置 AllowFiltering，见模块 README）。
//   - 缺 ViewerContext → 中止（fail-closed）；平台/系统视图 → 放行。
//
// 实现 viewer.ScopedModel 标记接口供 repository 类型断言识别（无需反射检测）。
type TenantID struct {
	TenantID int64 `cql:"name:tenant_id"`
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
