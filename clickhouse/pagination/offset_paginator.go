package pagination

import (
	"github.com/tx7do/go-crud/clickhouse/query"
	"github.com/tx7do/go-crud/paginator"
)

// OffsetPaginator 基于 Offset 的分页器（ClickHouse 版）
// 不再返回 GORM scope，而是直接构造 ClickHouse 的 LIMIT/OFFSET 子句字符串。
// 使用示例： sql := "SELECT ... " + p.BuildClause(offset, limit)
type OffsetPaginator struct {
	impl paginator.Paginator
}

func NewOffsetPaginator() *OffsetPaginator {
	return &OffsetPaginator{
		impl: paginator.NewOffsetPaginatorWithDefault(),
	}
}

// BuildClause 根据传入的 offset/limit 更新内部状态并返回 ClickHouse 的 LIMIT/OFFSET 子句。
// 若 limit <= 0（未设置或无效），返回空字符串。
// 当 offset 为 0 时仅返回 "LIMIT <n>"，否则返回 "LIMIT <n> OFFSET <m>"。
func (p *OffsetPaginator) BuildClause(builder *query.Builder, offset, limit int) *query.Builder {
	p.impl.
		WithOffset(offset).
		WithLimit(limit)

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
