package pagination

import (
	"encoding/base64"

	"entgo.io/ent/dialect/sql"
	"github.com/tx7do/go-crud/pagination"

	"github.com/go-kratos/kratos/v2/encoding"
	_ "github.com/go-kratos/kratos/v2/encoding/json"

	"github.com/tx7do/go-crud/pagination/paginator"
)

// TokenPaginator 基于 Token 的分页器
type TokenPaginator struct {
	impl  pagination.Paginator
	codec encoding.Codec
}

func NewTokenPaginator() *TokenPaginator {
	return &TokenPaginator{
		impl:  paginator.NewTokenPaginatorWithDefault(),
		codec: encoding.GetCodec("json"),
	}
}

func (p *TokenPaginator) BuildSelector(token string, pageSize int) func(*sql.Selector) {
	p.impl.
		WithToken(token).
		WithPage(pageSize)

	type cursor struct {
		LastID int64 `json:"last_id"`
	}

	// 无 token 或解码失败时只应用 pageSize
	if token == "" {
		return func(s *sql.Selector) {
			s.Limit(p.impl.Size())
		}
	}

	b, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return func(s *sql.Selector) {
			s.Limit(p.impl.Size())
		}
	}

	var c cursor
	if err = p.codec.Unmarshal(b, &c); err != nil {
		return func(s *sql.Selector) {
			s.Limit(p.impl.Size())
		}
	}

	lastID := c.LastID
	return func(s *sql.Selector) {
		s.Where(sql.GT("id", lastID))
		s.Limit(p.impl.Size())
	}
}
