package sorting

import (
	"fmt"
	"strings"

	"gorm.io/gorm"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

// StructuredSorting 用于把结构化的排序指令转换为 GORM 的 order scope
type StructuredSorting struct{}

// NewStructuredSorting 创建实例
func NewStructuredSorting() *StructuredSorting {
	return &StructuredSorting{}
}

// BuildScope 根据 orders 构建 GORM scope（可与 db.Scopes 一起使用）
func (ss StructuredSorting) BuildScope(orders []*paginationV1.Sorting) func(*gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if len(orders) == 0 {
			return db
		}
		for _, o := range orders {
			if o == nil {
				continue
			}
			field := strings.TrimSpace(o.GetField())
			if field == "" {
				continue
			}
			// 校验字段名，允许类似 "t.field"
			if !fieldNameRegexp.MatchString(field) {
				continue
			}
			dir := "ASC"
			if o.GetDirection() == paginationV1.Sorting_DESC {
				dir = "DESC"
			}
			db = db.Order(fmt.Sprintf("%s %s", field, dir))
		}
		return db
	}
}

// BuildScopeWithDefaultField 当 orders 为空时使用默认排序字段
// defaultOrderField 为空则不应用默认排序
func (ss StructuredSorting) BuildScopeWithDefaultField(orders []*paginationV1.Sorting, defaultOrderField string, defaultDesc bool) func(*gorm.DB) *gorm.DB {
	def := strings.TrimSpace(defaultOrderField)
	if len(orders) == 0 && def != "" && fieldNameRegexp.MatchString(def) {
		dir := "ASC"
		if defaultDesc {
			dir = "DESC"
		}
		return func(db *gorm.DB) *gorm.DB {
			return db.Order(fmt.Sprintf("%s %s", def, dir))
		}
	}
	return ss.BuildScope(orders)
}
