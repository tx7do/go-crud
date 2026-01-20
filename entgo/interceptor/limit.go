package interceptor

import (
	"context"

	"entgo.io/ent"
)

func SharedLimiter[Q interface{ Limit(int) }](f func(ent.Query) (Q, error), limit int) ent.Interceptor {
	return ent.InterceptFunc(func(next ent.Querier) ent.Querier {
		return ent.QuerierFunc(func(ctx context.Context, query ent.Query) (ent.Value, error) {
			l, err := f(query)
			if err != nil {
				return nil, err
			}
			l.Limit(limit)
			// LimitInterceptor limits the number of records returned from the
			// database to the configured one, in case Limit was not explicitly set.
			if ent.QueryFromContext(ctx).Limit == nil {
				l.Limit(limit)
			}
			return next.Query(ctx, query)
		})
	})
}
