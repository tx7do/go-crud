package mixin

import (
	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

// AutoIncrementID 是 GORM 可复用的 mixin，包含自增主键字段。
// 在模型中嵌入：
//
//	type User struct {
//	    mixin.AutoIncrementID
//	    Name string
//	}
type AutoIncrementID struct {
	ID uint32 `gorm:"column:id;primaryKey;autoIncrement" json:"id,omitempty"`
}

func (AutoIncrementID) GormDBDataType(db *gorm.DB, _ *schema.Field) string {
	// 保持默认行为或根据 db.Dialector 返回特定类型
	return ""
}

// AutoIncrementID64 是 AutoIncrementID 的 64 位 ID 变体。
// 在模型中嵌入：
//
//	type User struct {
//	    mixin.AutoIncrementID64
//	    Name string
//	}
type AutoIncrementID64 struct {
	ID uint64 `gorm:"column:id;primaryKey;autoIncrement" json:"id,omitempty"`
}

func (AutoIncrementID64) GormDBDataType(db *gorm.DB, _ *schema.Field) string {
	// 保持默认行为或根据 db.Dialector 返回特定类型
	return ""
}
