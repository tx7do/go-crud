package pagination

import (
	"encoding/base64"

	"github.com/go-kratos/kratos/v2/encoding"
	_ "github.com/go-kratos/kratos/v2/encoding/json"
	bsonV2 "go.mongodb.org/mongo-driver/v2/bson"

	"github.com/tx7do/go-crud/mongodb/query"
	"github.com/tx7do/go-crud/paginator"
)

// TokenPaginator 基于 Token 的分页器（MongoDB 版）
type TokenPaginator struct {
	impl  paginator.Paginator
	codec encoding.Codec
}

func NewTokenPaginator() *TokenPaginator {
	return &TokenPaginator{
		impl:  paginator.NewTokenPaginatorWithDefault(),
		codec: encoding.GetCodec("json"),
	}
}

// BuildClause 根据传入 token/pageSize 更新状态并将 filter/limit 设置到 query.Builder。
// 若 pageSize <= 0 则返回原 builder。若 token 无法解析则仅设置 limit。
func (p *TokenPaginator) BuildClause(builder *query.Builder, token string, pageSize int) *query.Builder {
	p.impl.
		WithToken(token).
		WithPage(pageSize)

	size := p.impl.Size()
	if size <= 0 {
		return builder
	}

	// 无 token 或无法解码时仅设置 limit
	if token == "" {
		builder.SetLimit(int64(size))
		return builder
	}

	b, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		builder.SetLimit(int64(size))
		return builder
	}

	var c struct {
		LastID int64 `json:"last_id"`
	}
	if err = p.codec.Unmarshal(b, &c); err != nil {
		builder.SetLimit(int64(size))
		return builder
	}

	// 为 MongoDB 设置过滤条件 id > last_id，并设置 limit
	filter := bsonV2.M{"id": bsonV2.M{"$gt": c.LastID}}
	builder.SetFilter(filter)
	builder.SetLimit(int64(size))

	return builder
}
