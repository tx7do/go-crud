package pagination

import (
	"github.com/tx7do/go-crud/pagination"
	bsonV2 "go.mongodb.org/mongo-driver/v2/bson"

	"github.com/tx7do/go-crud/mongodb/query"
	"github.com/tx7do/go-crud/pagination/paginator"
)

// TokenPaginator 基于 Token 的分页器（MongoDB 版）
type TokenPaginator struct {
	impl pagination.Paginator
}

func NewTokenPaginator() *TokenPaginator {
	return &TokenPaginator{
		impl: paginator.NewTokenPaginatorWithDefault(),
	}
}

// BuildClause 根据传入 token/pageSize 更新状态并将 filter/limit 设置到 query.Builder。
// 若 pageSize <= 0 则返回原 builder。若 token 无法解析则仅设置 limit。
func (p *TokenPaginator) BuildClause(builder *query.Builder, token string, pageSize int) *query.Builder {
	p.impl.
		WithToken(token).
		WithSize(pageSize)

	size := p.impl.Size()
	if size <= 0 {
		return builder
	}

	// 无 token 或无法解码时仅设置 limit
	if token == "" {
		builder.SetLimit(int64(size))
		return builder
	}

	lastID, ok := pagination.VerifyAndDecode(token, pagination.TokenSecret())
	if !ok {
		builder.SetLimit(int64(size))
		return builder
	}

	// 为 MongoDB 设置过滤条件 id > last_id，并设置 limit
	filter := bsonV2.M{"id": bsonV2.M{"$gt": lastID}}
	builder.SetFilter(filter)
	builder.SetLimit(int64(size))

	return builder
}
