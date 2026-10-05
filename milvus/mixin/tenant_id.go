package mixin

import (
	"math"

	"github.com/tx7do/go-crud/viewer"
)

// TenantID 是 Milvus 可复用的 mixin，表示租户 ID。
// Milvus 无无符号整数类型，租户标识以 int64 存储（名称固定 tenant_id，
// 并在集合创建时标记为 partition key，见 milvus 模块 schema 构建器）。
// 嵌入此 mixin 的实体自动启用租户隔离强制（与 entgo TenantPrivacy 一致）：
//   - Create：租户业务视图下强制覆盖 tenant_id 为当前 Viewer 的租户。
//   - Get/QueryByExpr/DeleteByIDs/DeleteByExpr/Count/Exists/SearchByVector：
//     repository 注入 tenant_id == tid 谓词限定到当前 Viewer 租户
//     （可注入 expr 的路径）；按主键直取的路径在客户端校验租户列。
//   - 缺 ViewerContext → 中止（fail-closed）；平台/系统视图 → 放行。
//
// 实现 viewer.ScopedModel 标记接口供 repository 类型断言识别（无需反射检测）。
type TenantID struct {
	TenantID int64 `milvus:"name:tenant_id"`
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
