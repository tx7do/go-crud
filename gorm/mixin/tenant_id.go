package mixin

import (
	"gorm.io/gorm"

	gormcrud "github.com/tx7do/go-crud/gorm"
)

// TenantID 是 GORM 可复用的 mixin，表示租户 ID（可为空）。
// 使用指针以支持 nullable，并在数据库中建立索引。
//
// 嵌入此 mixin 的实体自动启用租户隔离强制（与 entgo TenantPrivacy 一致）：
//   - Create：租户业务视图下强制覆盖 tenant_id 为当前 Viewer 的租户，
//     防止越权写入他租户数据。
//   - Query/Update/Delete：由 gorm callback 注入 tenant_id = ? 谓词，
//     限定到当前 Viewer 的租户范围。
//   - 缺 ViewerContext → 中止（fail-closed）；平台/系统视图 → 放行。
//
// 不在钩子中强制不可变性（ent 的 Immutable 在 GORM 中需在业务层或更复杂的钩子中处理）。
type TenantID struct {
	TenantID *uint32 `gorm:"column:tenant_id;type:int unsigned;index" json:"tenant_id,omitempty"`
}

// BeforeCreate 在创建记录前执行租户强制：租户业务视图下覆盖 tenant_id。
func (m *TenantID) BeforeCreate(tx *gorm.DB) (err error) {
	// 委托给 gorm 模块的强制 helper，语义同 entgo EvalMutation Create。
	return gormcrud.SetTenantIDOnCreateBeforeHook(tx)
}
