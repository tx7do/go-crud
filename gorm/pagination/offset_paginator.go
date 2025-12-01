package pagination

import (
	"gorm.io/gorm"

	"github.com/tx7do/go-curd/paginator"
)

// OffsetPaginator 基于 Offset 的分页器
type OffsetPaginator struct {
	impl paginator.Paginator
}

func NewOffsetPaginator() *OffsetPaginator {
	return &OffsetPaginator{
		impl: paginator.NewOffsetPaginatorWithDefault(),
	}
}

// BuildDB 根据传入的 offset/limit 更新内部状态并返回用于 GORM 的函数
// 使用方式示例： db = paginator.BuildDB(offset, limit)(db)
func (p *OffsetPaginator) BuildDB(offset, limit int) func(*gorm.DB) *gorm.DB {
	p.impl.
		WithOffset(offset).
		WithLimit(limit)

	return func(db *gorm.DB) *gorm.DB {
		if db == nil {
			return db
		}
		return db.
			Offset(p.impl.Offset()).
			Limit(p.impl.Limit())
	}
}
