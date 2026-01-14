package paginator

import "github.com/tx7do/go-crud/pagination"

type tokenPaginator struct {
	token     string
	nextToken string
	prevToken string
	limit     int
	total     int64
}

func NewTokenPaginator(token string, limit int) pagination.Paginator {
	if limit < 1 {
		limit = DefaultLimit
	}
	return &tokenPaginator{
		token: token,
		limit: limit,
	}
}

func NewTokenPaginatorWithDefault() pagination.Paginator {
	return &tokenPaginator{
		limit: DefaultLimit,
	}
}

func (p *tokenPaginator) Mode() pagination.PaginateMode { return pagination.ModeToken }

func (p *tokenPaginator) Page() int { return 1 }
func (p *tokenPaginator) Size() int {
	if p.limit < 1 {
		return 1
	}
	return p.limit
}

func (p *tokenPaginator) Offset() int { return 0 }
func (p *tokenPaginator) Limit() int  { return p.Size() }

func (p *tokenPaginator) Token() string         { return p.token }
func (p *tokenPaginator) NextToken() string     { return p.nextToken }
func (p *tokenPaginator) PrevToken() string     { return p.prevToken }
func (p *tokenPaginator) SetToken(token string) { p.token = token }
func (p *tokenPaginator) SetNextToken(t string) { p.nextToken = t }
func (p *tokenPaginator) SetPrevToken(t string) { p.prevToken = t }

func (p *tokenPaginator) Total() int64 {
	if p.total < 0 {
		return 0
	}
	return p.total
}
func (p *tokenPaginator) SetTotal(total int64) {
	if total < 0 {
		total = 0
	}
	p.total = total
}
func (p *tokenPaginator) TotalPages() int {
	lim := p.Limit()
	if lim == 0 {
		return 0
	}
	t := p.Total()
	if t == 0 {
		// total 未知时无法准确计算页数，返回 0 表示未知
		return 0
	}
	pages := int((t + int64(lim) - 1) / int64(lim))
	if pages < 1 {
		return 1
	}
	return pages
}

func (p *tokenPaginator) HasNext() bool { return p.nextToken != "" }
func (p *tokenPaginator) HasPrev() bool { return p.prevToken != "" }

func (p *tokenPaginator) WithPage(int) pagination.Paginator { return p } // token 模式下无效
func (p *tokenPaginator) WithSize(size int) pagination.Paginator {
	if size < 1 {
		size = 1
	}
	p.limit = size
	return p
}
func (p *tokenPaginator) WithOffset(int) pagination.Paginator      { return p } // token 模式下无效
func (p *tokenPaginator) WithLimit(limit int) pagination.Paginator { return p.WithSize(limit) }
func (p *tokenPaginator) WithToken(token string) pagination.Paginator {
	p.token = token
	return p
}
