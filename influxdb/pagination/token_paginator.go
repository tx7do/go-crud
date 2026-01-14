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
// 通过类型断言在运行时调用可选的 SetSkip/SetLimit 方法，避免在编译期依赖它们。
func (p *TokenPaginator) BuildClause(builder *query.Builder, token string, pageSize int) *query.Builder {
	p.impl.
		WithToken(token).
		WithPage(pageSize)

	lim := p.impl.Limit()
	off := p.impl.Offset()

	if lim <= 0 {
		return builder
	}

	if off > 0 && builder != nil {
		if s, ok := interface{}(builder).(interface{ SetSkip(int64) }); ok {
			s.SetSkip(int64(off))
		}
	}
	if builder != nil {
		if l, ok := interface{}(builder).(interface{ SetLimit(int64) }); ok {
			l.SetLimit(int64(lim))
		}
	}

	return builder
}
