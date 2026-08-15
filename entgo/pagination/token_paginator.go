package pagination

import (
	"entgo.io/ent/dialect/sql"
	"github.com/tx7do/go-crud/pagination"

	"github.com/tx7do/go-crud/pagination/paginator"
)

// TokenPaginator 基于 Token 的分页器
type TokenPaginator struct {
	impl pagination.Paginator
}

func NewTokenPaginator() *TokenPaginator {
	return &TokenPaginator{
		impl: paginator.NewTokenPaginatorWithDefault(),
	}
}

func (p *TokenPaginator) BuildSelector(token string, pageSize int) func(*sql.Selector) {
	p.impl.
		WithToken(token).
		WithPage(pageSize)

	// 无 token 或解码失败时只应用 pageSize
	if token == "" {
		return func(s *sql.Selector) {
			s.Limit(p.impl.Size())
		}
	}

	lastID, ok := pagination.VerifyAndDecode(token, nil)
	if !ok {
		return func(s *sql.Selector) {
			s.Limit(p.impl.Size())
		}
	}

	return func(s *sql.Selector) {
		s.Where(sql.GT("id", lastID))
		s.Limit(p.impl.Size())
	}
}
