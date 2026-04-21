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
// 若 limit <= 0（未设置或无效），返回原 builder。
func (p *PagePaginator) BuildClause(builder *query.Builder, page, size int) *query.Builder {
	p.impl.
		WithPage(page).
		WithSize(size)

	lim := p.impl.Limit()
	if lim <= 0 {
		return builder
	}

	builder.SetPage(page, size)
	return builder
}
