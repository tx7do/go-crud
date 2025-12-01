package pagination

import (
	"github.com/tx7do/go-curd/paginator"
	"gorm.io/gorm"
)

// PagePaginator 基于页码的分页器
type PagePaginator struct {
	impl paginator.Paginator
}

func NewPagePaginator() *PagePaginator {
	return &PagePaginator{
		impl: paginator.NewPagePaginatorWithDefault(),
	}
}

// BuildDB 根据传入的 page/size 更新内部状态并返回用于 GORM 的函数
// 使用示例： db = paginator.BuildDB(page, size)(db)
func (p *PagePaginator) BuildDB(page, size int) func(*gorm.DB) *gorm.DB {
	p.impl.
		WithPage(page).
		WithSize(size)

	return func(db *gorm.DB) *gorm.DB {
		if db == nil {
			return db
		}
		return db.
			Offset(p.impl.Offset()).
			Limit(p.impl.Limit())
	}
}
