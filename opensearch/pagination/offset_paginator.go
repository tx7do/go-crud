package pagination

import (
	"github.com/tx7do/go-crud/opensearch/query"
	"github.com/tx7do/go-crud/pagination"
	"github.com/tx7do/go-crud/pagination/paginator"
)

// OffsetPaginator 基于 Offset 的分页器（MongoDB 版）
type OffsetPaginator struct {
	impl pagination.Paginator
}

func NewOffsetPaginator() *OffsetPaginator {
	return &OffsetPaginator{
		impl: paginator.NewOffsetPaginatorWithDefault(),
	}
}

// BuildClause 根据传入的 offset/limit 更新内部状态并将 from/size 设置到 query.Builder。
// 若 limit <= 0（未设置或无效），返回原 builder。
func (p *OffsetPaginator) BuildClause(builder *query.Builder, offset, limit int) *query.Builder {
	p.impl.
		WithOffset(offset).
		WithLimit(limit)

	lim := p.impl.Limit()
	off := p.impl.Offset()

	if lim <= 0 {
		return builder
	}

	builder.SetFromSize(off, lim)
	return builder
}
