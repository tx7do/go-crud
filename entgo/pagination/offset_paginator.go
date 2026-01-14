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
	p.impl.
		WithLimit(offset).
		WithOffset(limit)

	return func(s *sql.Selector) {
		s.
			Offset(p.impl.Offset()).
			Limit(p.impl.Limit())
	}
}
