package pagination

import (
	"github.com/tx7do/go-crud/opensearch/query"
	"github.com/tx7do/go-crud/pagination"
	"github.com/tx7do/go-crud/pagination/paginator"
)

// PagePaginator 基于页码的分页器（MongoDB 版）
// 使用示例： p.BuildClause(builder, page, size) 会在 builder 上设置 skip/limit
type PagePaginator struct {
	impl pagination.Paginator
}

func NewPagePaginator() *PagePaginator {
	return &PagePaginator{
		impl: paginator.NewPagePaginatorWithDefault(),
	}
}

// BuildClause 根据传入的 page/size 更新内部状态并将 page/size 设置到 query.Builder。
// size 超过 MaxLimit 时钳制到上限；size 无效（<= 0）时交给 builder.SetPage
// 归一化为默认页大小。
func (p *PagePaginator) BuildClause(builder *query.Builder, page, size int) *query.Builder {
	p.impl.
		WithPage(page).
		WithSize(size)

	lim := p.impl.Limit()
	if lim <= 0 {
		return builder
	}

	if size < 1 {
		// 未设置或无效：保留 SetPage 的默认页大小语义
		builder.SetPage(page, 0)
		return builder
	}

	builder.SetPage(p.impl.Page(), lim)
	return builder
}
