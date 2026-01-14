package pagination

import (
	"entgo.io/ent/dialect/sql"
	"github.com/tx7do/go-crud/pagination"
	"github.com/tx7do/go-crud/pagination/paginator"
)

// PagePaginator 基于页码的分页器
type PagePaginator struct {
	impl pagination.Paginator
}

func NewPagePaginator() *PagePaginator {
	return &PagePaginator{
		impl: paginator.NewPagePaginatorWithDefault(),
	}
}

func (p *PagePaginator) BuildSelector(page, pageSize int) func(*sql.Selector) {
	p.impl.
		WithPage(page).
		WithSize(pageSize)

	return func(s *sql.Selector) {
		s.
			Offset(p.impl.Offset()).
			Limit(p.impl.Limit())
	}
}
