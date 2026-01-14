package paginator

import "github.com/tx7do/go-crud/pagination"

var DefaultLimit = 10
var DefaultOffset = 0

type offsetPaginator struct {
	offset int
	limit  int
	total  int64
}

func NewOffsetPaginator(offset, limit int) pagination.Paginator {
	if offset < 0 {
		offset = DefaultOffset
	}
	if limit < 1 {
		limit = DefaultLimit
	}
	return &offsetPaginator{
		offset: offset,
		limit:  limit,
	}
}

func NewOffsetPaginatorWithDefault() pagination.Paginator {
	return &offsetPaginator{
		offset: DefaultOffset,
		limit:  DefaultLimit,
	}
}

func (p *offsetPaginator) Mode() pagination.PaginateMode { return pagination.ModeOffset }

func (p *offsetPaginator) Page() int {
	lim := p.Limit()
	if lim <= 0 {
		return 1
	}
	return p.Offset()/lim + 1
}
func (p *offsetPaginator) Size() int { return p.Limit() }

func (p *offsetPaginator) Offset() int {
	if p.offset < 0 {
		return 0
	}
	return p.offset
}
func (p *offsetPaginator) Limit() int {
	if p.limit < 1 {
		return 1
	}
	return p.limit
}

func (p *offsetPaginator) Token() string       { return "" }
func (p *offsetPaginator) NextToken() string   { return "" }
func (p *offsetPaginator) PrevToken() string   { return "" }
func (p *offsetPaginator) SetToken(string)     {}
func (p *offsetPaginator) SetNextToken(string) {}
func (p *offsetPaginator) SetPrevToken(string) {}

func (p *offsetPaginator) Total() int64 {
	if p.total < 0 {
		return 0
	}
	return p.total
}
func (p *offsetPaginator) SetTotal(total int64) {
	if total < 0 {
		total = 0
	}
	p.total = total
}
func (p *offsetPaginator) TotalPages() int {
	lim := p.Limit()
	if lim == 0 {
		return 0
	}
	t := p.Total()
	pages := int((t + int64(lim) - 1) / int64(lim))
	if pages < 1 {
		return 1
	}
	return pages
}

func (p *offsetPaginator) HasNext() bool {
	return p.Offset()+p.Limit() < int(p.Total())
}
func (p *offsetPaginator) HasPrev() bool { return p.Offset() > 0 }

func (p *offsetPaginator) WithPage(page int) pagination.Paginator {
	if page < 1 {
		page = 1
	}
	p.offset = (page - 1) * p.Limit()
	return p
}
func (p *offsetPaginator) WithSize(size int) pagination.Paginator { return p.WithLimit(size) }
func (p *offsetPaginator) WithOffset(offset int) pagination.Paginator {
	if offset < 0 {
		offset = 0
	}
	p.offset = offset
	return p
}
func (p *offsetPaginator) WithLimit(limit int) pagination.Paginator {
	if limit < 1 {
		limit = 1
	}
	p.limit = limit
	return p
}
func (p *offsetPaginator) WithToken(string) pagination.Paginator { return p }
