package field

import (
	"strings"

	"gorm.io/gorm"
)

// Selector 字段选择器，用于构建 GORM 查询中的字段列表。
type Selector struct{}

// NewFieldSelector 返回一个新的 Selector。
func NewFieldSelector() *Selector { return &Selector{} }

// BuildSelect 将 fields 应用到传入的 *gorm.DB，并返回修改后的 *gorm.DB。
func (fs Selector) BuildSelect(db *gorm.DB, fields []string) *gorm.DB {
	if db == nil || len(fields) == 0 {
		return db
	}
	fields = NormalizePaths(fields)
	// 使用逗号连接作为 Select 参数
	return db.Select(strings.Join(fields, ", "))
}

// BuildSelector 返回一个可直接应用到 *gorm.DB 的闭包；当 fields 为空时返回 (nil, nil)。
func (fs Selector) BuildSelector(fields []string) (func(*gorm.DB) *gorm.DB, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	// 捕获 fields 的当前值
	fsFields := make([]string, len(fields))
	copy(fsFields, fields)
	return func(db *gorm.DB) *gorm.DB {
		return fs.BuildSelect(db, fsFields)
	}, nil
}
