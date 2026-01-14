package pagination

import (
	"github.com/tx7do/go-crud/mongodb/query"
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

// BuildClause 根据传入的 offset/limit 更新内部状态并将 skip/limit 设置到 query.Builder。
// 若 limit <= 0（未设置或无效），返回原 builder。
// 当 offset 为 0 时仅设置 limit，否则同时设置 skip 和 limit。
func (p *OffsetPaginator) BuildClause(builder *query.Builder, offset, limit int) *query.Builder {
	p.impl.
		WithOffset(offset).
		WithLimit(limit)

	lim := p.impl.Limit()
	off := p.impl.Offset()

	if lim <= 0 {
		return builder
	}

	// 将 offset/limit 转为 int64 并设置到 builder（query.Builder 应实现 SetSkip/SetLimit）
	if off > 0 {
		builder.SetSkip(int64(off))
	}
	builder.SetLimit(int64(lim))

	return builder
}
