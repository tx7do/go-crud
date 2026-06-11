package pagination

import (
	"encoding/base64"

	"github.com/tx7do/go-crud/pagination"
	"github.com/tx7do/go-wind-plugins/encoding"
	_ "github.com/tx7do/go-wind-plugins/encoding/json"

	"github.com/tx7do/go-crud/opensearch/query"
	"github.com/tx7do/go-crud/pagination/paginator"
)

// TokenPaginator 基于 Token 的分页器（MongoDB 版）
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

// BuildClause 根据传入 token/pageSize 更新状态并将 filter/limit 设置到 query.Builder。
// 若 pageSize <= 0 则返回原 builder。若 token 无法解析则仅设置 limit。
func (p *TokenPaginator) BuildClause(builder *query.Builder, token string, pageSize int) *query.Builder {
	p.impl.
		WithToken(token).
		WithPage(pageSize)

	size := p.impl.Size()
	if size <= 0 {
		return builder
	}

	// 无 token 或无法解码时仅设置 size
	if token == "" {
		builder.SetFromSize(0, size)
		return builder
	}

	b, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		builder.SetFromSize(0, size)
		return builder
	}

	var c struct {
		LastID int64 `json:"last_id"`
	}
	if err = p.codec.Unmarshal(b, &c); err != nil {
		builder.SetFromSize(0, size)
		return builder
	}

	// OpenSearch: id > last_id
	builder.SetRange("id", c.LastID, nil)
	builder.SetFromSize(0, size)
	return builder
}
