package mixin

import (
	"errors"

	"gorm.io/gorm"
)

// Version 是 GORM 可复用的 mixin，表示版本号/乐观锁。
// 注意：钩子只负责在创建时设置初始版本。乐观锁的检查与递增
// 需要在更新操作时在仓储/业务层使用带 WHERE version = ? 的更新语句来保证原子性。
type Version struct {
	Version uint32 `gorm:"column:version;type:int unsigned;default:1;not null;index" json:"version"`
}

func (m *Version) BeforeCreate(tx *gorm.DB) (err error) {
	if m.Version == 0 {
		m.Version = 1
	}
	return nil
}

// OptimisticUpdate 是一个简单的辅助函数示例：在单个事务内
// 使用 WHERE version = oldVersion 执行更新并检查 RowsAffected，
// 若为 0 则表示版本冲突（需要重试或返回错误）。
func OptimisticUpdate(tx *gorm.DB, model any, oldVersion uint32, updates map[string]any) error {
	res := tx.Model(model).Where("version = ?", oldVersion).Updates(updates)
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected == 0 {
		return errors.New("optimistic lock: version mismatch")
	}
	return nil
}
