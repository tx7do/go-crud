package pagination

import (
	"github.com/tx7do/go-crud/clickhouse/query"
	"github.com/tx7do/go-crud/paginator"
)

// PagePaginator 基于页码的分页器（ClickHouse 版）
// 不再返回 GORM scope，而是直接构造 ClickHouse 的 LIMIT/OFFSET 子句。
// 使用示例： sql := "SELECT ... " + p.BuildClause(page, size)
type PagePaginator struct {
	impl paginator.Paginator
}

func NewPagePaginator() *PagePaginator {
	return &PagePaginator{
		impl: paginator.NewPagePaginatorWithDefault(),
	}
}

// BuildClause 根据传入的 page/size 更新内部状态并返回 ClickHouse 的 LIMIT/OFFSET 子句。
// 若 limit <= 0（未设置或无效），返回空字符串。
// 当 offset 为 0 时仅返回 "LIMIT <n>"，否则返回 "LIMIT <n> OFFSET <m>"。
func (p *PagePaginator) BuildClause(builder *query.Builder, page, size int) *query.Builder {
	p.impl.
		WithPage(page).
		WithSize(size)

	lim := p.impl.Limit()
	off := p.impl.Offset()

	if lim <= 0 {
		return builder
	}
	if off > 0 {
		return builder.Offset(off).Limit(lim)
	}

	return builder.Limit(lim)
}
