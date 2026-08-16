package pagination

import (
	"github.com/tx7do/go-crud/influxdb/query"
	"github.com/tx7do/go-crud/pagination"
	"github.com/tx7do/go-crud/pagination/paginator"
)

// TokenPaginator 基于 Token 的分页器（InfluxDB 版）
type TokenPaginator struct {
	impl pagination.Paginator
}

func NewTokenPaginator() *TokenPaginator {
	return &TokenPaginator{
		impl: paginator.NewTokenPaginatorWithDefault(),
	}
}

// BuildClause 根据传入 token/pageSize 更新内部状态并将 skip/limit 设置到 query.Builder。
// 若 limit <= 0（未设置或无效），返回原 builder。
func (p *TokenPaginator) BuildClause(builder *query.Builder, token string, pageSize int) *query.Builder {
	p.impl.
		WithToken(token).
		WithSize(pageSize)

	lim := p.impl.Limit()
	off := p.impl.Offset()

	if lim <= 0 {
		return builder
	}

	// builder 为具体类型，直接调用其 Offset/Limit。
	// 此前用运行时断言探测 SetSkip(int64)/SetLimit(int64)，而 Builder 只有
	// Offset(int)/Limit(int)，断言永不命中——LIMIT/OFFSET 从未生效。
	if builder != nil {
		if off > 0 {
			builder.Offset(off)
		}
		builder.Limit(lim)
	}

	return builder
}
