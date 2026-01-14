package pagination

import (
	"github.com/tx7do/go-crud/influxdb/query"
	"github.com/tx7do/go-crud/pagination"
	"github.com/tx7do/go-crud/pagination/paginator"
)

// OffsetPaginator 基于 Offset 的分页器（InfluxDB 版）
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
// 通过类型断言在运行时调用可选的 SetSkip/SetLimit 方法，避免在编译期依赖它们。
func (p *OffsetPaginator) BuildClause(builder *query.Builder, offset, limit int) *query.Builder {
	p.impl.
		WithOffset(offset).
		WithLimit(limit)

	lim := p.impl.Limit()
	off := p.impl.Offset()

	if lim <= 0 {
		return builder
	}

	// 如果 builder 实现了 SetSkip，则设置 skip
	if off > 0 && builder != nil {
		if s, ok := interface{}(builder).(interface{ SetSkip(int64) }); ok {
			s.SetSkip(int64(off))
		}
	}

	// 如果 builder 实现了 SetLimit，则设置 limit
	if builder != nil {
		if l, ok := interface{}(builder).(interface{ SetLimit(int64) }); ok {
			l.SetLimit(int64(lim))
		}
	}

	return builder
}
