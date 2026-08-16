package interceptor

import (
	"context"

	"entgo.io/ent"
)

// SharedLimiter 为未显式设置 Limit 的查询施加兜底行数上限。
// f 将 ent.Query 转换为具体类型的查询（通常是对原查询的类型断言，共享底层状态）。
//
// 此前实现的两个缺陷：
//  1. 在转换出的对象上调用 Limit 后，却用原始 query 执行——若 f 返回的不是
//     同一对象（如包装/复制），上限被丢弃；
//  2. 先无条件施加 Limit，再判断调用方是否已设置，顺序颠倒。
//
// 现在：调用方已设置 Limit 则完全尊重；否则在转换结果上施加兜底，
// 且若转换结果本身是 ent.Query 就用它执行，保证上限真正生效。
func SharedLimiter[Q interface{ Limit(int) }](f func(ent.Query) (Q, error), limit int) ent.Interceptor {
	return ent.InterceptFunc(func(next ent.Querier) ent.Querier {
		return ent.QuerierFunc(func(ctx context.Context, query ent.Query) (ent.Value, error) {
			if ent.QueryFromContext(ctx).Limit != nil {
				// 调用方显式设置过 Limit，尊重调用方
				return next.Query(ctx, query)
			}

			l, err := f(query)
			if err != nil {
				return nil, err
			}
			l.Limit(limit)

			if typed, ok := any(l).(ent.Query); ok {
				query = typed
			}
			return next.Query(ctx, query)
		})
	})
}
