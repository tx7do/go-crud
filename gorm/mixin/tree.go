package mixin

import "gorm.io/gorm"

// ParentID 是 GORM 可复用的 mixin，表示父节点 ID（可为空）。
// 使用指针以支持 nullable，并在数据库中建立索引。
type ParentID struct {
	ParentID *uint32 `gorm:"column:parent_id;type:int unsigned;index" json:"parent_id,omitempty"`
}

// 钩子占位：保持行为一致（可在此处添加校验不可变性等逻辑）。

func (m *ParentID) BeforeCreate(tx *gorm.DB) (err error) { return nil }
func (m *ParentID) BeforeSave(tx *gorm.DB) (err error)   { return nil }

// ParentID64 是 ParentID 的 64 位 ID 变体，表示父节点 ID（可为空）。
type ParentID64 struct {
	ParentID *uint64 `gorm:"column:parent_id;type:bigint unsigned;index" json:"parent_id,omitempty"`
}

func (m *ParentID64) BeforeCreate(tx *gorm.DB) (err error) { return nil }
func (m *ParentID64) BeforeSave(tx *gorm.DB) (err error)   { return nil }

// Tree64 是 Tree 的 64 位 ID 变体，嵌入 ParentID64 并提供 Parent/Children 关系。
type Tree64[T any] struct {
	ParentID64

	// Parent 指向父节点（可为 nil）
	Parent *T `gorm:"foreignKey:ParentID;references:ID" json:"parent,omitempty"`

	// Children 列表，使用子表的 parent_id 字段作为外键
	Children []T `gorm:"foreignKey:ParentID;references:ID" json:"children,omitempty"`
}

// Tree 是通用的 GORM mixin，嵌入 ParentID 并提供 Parent/Children 关系。
// T 应该是包含 `ID` 字段的实体类型（GORM 在运行时通过反射匹配字段）。
type Tree[T any] struct {
	ParentID

	// Parent 指向父节点（可为 nil）
	Parent *T `gorm:"foreignKey:ParentID;references:ID" json:"parent,omitempty"`

	// Children 列表，使用子表的 parent_id 字段作为外键
	Children []T `gorm:"foreignKey:ParentID;references:ID" json:"children,omitempty"`
}
