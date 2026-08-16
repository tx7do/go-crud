package pagination

import (
	"github.com/tx7do/go-crud/influxdb/query"
	"github.com/tx7do/go-crud/pagination"
	"github.com/tx7do/go-crud/pagination/paginator"
)

// PagePaginator 基于页码的分页器（InfluxDB 版）
// 使用示例： p.BuildClause(builder, page, size) 会在 builder 上设置 skip/limit（若 builder 支持）
type PagePaginator struct {
	impl pagination.Paginator
}

func NewPagePaginator() *PagePaginator {
	return &PagePaginator{
		impl: paginator.NewPagePaginatorWithDefault(),
	}
}

// BuildClause 根据传入的 page/size 更新内部状态并将 skip/limit 设置到 query.Builder。
// 若 limit <= 0（未设置或无效），返回原 builder。
// 当 offset 为 0 时仅设置 limit，否则同时设置 skip 和 limit。
func (p *PagePaginator) BuildClause(builder *query.Builder, page, size int) *query.Builder {
	p.impl.
		WithPage(page).
		WithSize(size)

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
