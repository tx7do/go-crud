package paginator

import "github.com/tx7do/go-crud/pagination"

var DefaultPage = 1
var DefaultPageSize = 10

type pagePaginator struct {
	page  int
	size  int
	total int64
}

func NewPagePaginator(page, size int) pagination.Paginator {
	if page < 1 {
		page = DefaultPage
	}
	if size < 1 {
		size = DefaultPageSize
	}
	return &pagePaginator{
		page: page,
		size: size,
	}
}

func NewPagePaginatorWithDefault() pagination.Paginator {
	return &pagePaginator{
		page: DefaultPage,
		size: DefaultPageSize,
	}
}

func (p *pagePaginator) Mode() pagination.PaginateMode { return pagination.ModePage }

func (p *pagePaginator) Page() int {
	if p.page < 1 {
		return 1
	}
	return p.page
}

func (p *pagePaginator) Size() int {
	if p.size < 1 {
		return 1
	}
	return p.size
}

func (p *pagePaginator) Offset() int { return (p.Page() - 1) * p.Size() }
func (p *pagePaginator) Limit() int  { return p.Size() }

func (p *pagePaginator) Token() string       { return "" }
func (p *pagePaginator) NextToken() string   { return "" }
func (p *pagePaginator) PrevToken() string   { return "" }
func (p *pagePaginator) SetToken(string)     {}
func (p *pagePaginator) SetNextToken(string) {}
func (p *pagePaginator) SetPrevToken(string) {}

func (p *pagePaginator) Total() int64 {
	if p.total < 0 {
		return 0
	}
	return p.total
}
func (p *pagePaginator) SetTotal(total int64) {
	if total < 0 {
		total = 0
	}
	p.total = total
}
func (p *pagePaginator) TotalPages() int {
	sz := p.Size()
	if sz == 0 {
		return 0
	}
	t := p.Total()
	pages := int((t + int64(sz) - 1) / int64(sz))
	if pages < 1 {
		return 1
	}
	return pages
}

func (p *pagePaginator) HasNext() bool { return p.Page() < p.TotalPages() }
func (p *pagePaginator) HasPrev() bool { return p.Page() > 1 }

func (p *pagePaginator) WithPage(page int) pagination.Paginator {
	if page < 1 {
		page = 1
	}
	p.page = page
	return p
}
func (p *pagePaginator) WithSize(size int) pagination.Paginator {
	if size < 1 {
		size = 1
	}
	p.size = size
	return p
}
func (p *pagePaginator) WithOffset(offset int) pagination.Paginator {
	// 支持从 offset 设置 page（向上取整）
	if offset < 0 {
		offset = 0
	}
	p.page = offset/p.Size() + 1
	return p
}
func (p *pagePaginator) WithLimit(limit int) pagination.Paginator { return p.WithSize(limit) }
func (p *pagePaginator) WithToken(string) pagination.Paginator    { return p }
