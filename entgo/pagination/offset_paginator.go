package pagination

import (
	"entgo.io/ent/dialect/sql"

	"github.com/tx7do/go-crud/pagination"
	"github.com/tx7do/go-crud/pagination/paginator"
)

// OffsetPaginator 基于 Offset 的分页器
type OffsetPaginator struct {
	impl pagination.Paginator
}

func NewOffsetPaginator() *OffsetPaginator {
	return &OffsetPaginator{
		impl: paginator.NewOffsetPaginatorWithDefault(),
	}
}

func (p *OffsetPaginator) BuildSelector(offset, limit int) func(*sql.Selector) {
	// 注意参数顺序：WithOffset(offset).WithLimit(limit)。
	// 此前两者写反（WithLimit(offset).WithOffset(limit)），导致 OFFSET/LIMIT 互换，
	// offset 分页全部返回错误的页窗口。
	p.impl.
		WithOffset(offset).
		WithLimit(limit)

	return func(s *sql.Selector) {
		s.
			Offset(p.impl.Offset()).
			Limit(p.impl.Limit())
	}
}
