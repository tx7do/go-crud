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
	// 跳过 NormalizePaths 因含非法标识符段而置空的路径，避免逗号残缺
	// 生成畸形 SELECT 列。与 clickhouse/doris/entgo/influxdb/mongodb/
	// opensearch 六模块的 field_selector 对齐（B 回归）。
	filtered := make([]string, 0, len(fields))
	for _, f := range fields {
		if f != "" {
			filtered = append(filtered, f)
		}
	}
	if len(filtered) == 0 {
		return db
	}
	return db.Select(strings.Join(filtered, ", "))
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
