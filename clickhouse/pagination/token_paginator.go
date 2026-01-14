package pagination

import (
	"encoding/base64"

	"github.com/go-kratos/kratos/v2/encoding"
	_ "github.com/go-kratos/kratos/v2/encoding/json"
	"github.com/tx7do/go-crud/clickhouse/query"
	"github.com/tx7do/go-crud/pagination"

	"github.com/tx7do/go-crud/pagination/paginator"
)

// TokenPaginator 基于 Token 的分页器（ClickHouse 版）
// BuildClause 返回可直接拼接到 ClickHouse 查询的子句，形式为:
// - 当 token 为空或无效时: "LIMIT <n>"
// - 当 token 有效且包含 last_id 时: "WHERE id > <last_id> LIMIT <n>"
type TokenPaginator struct {
	impl  pagination.Paginator
	codec encoding.Codec
}

func NewTokenPaginator() *TokenPaginator {
	return &TokenPaginator{
		impl:  paginator.NewTokenPaginatorWithDefault(),
		codec: encoding.GetCodec("json"),
	}
}

// BuildClause 根据传入 token/pageSize 更新状态并返回 ClickHouse 的 WHERE/LIMIT 子句。
// 若 pageSize <= 0 或解析失败则返回空字符串或仅 LIMIT。
func (p *TokenPaginator) BuildClause(builder *query.Builder, token string, pageSize int) *query.Builder {
	p.impl.
		WithToken(token).
		WithPage(pageSize)

	size := p.impl.Size()
	if size <= 0 {
		return builder
	}

	// 无 token 或无法解码时仅返回 LIMIT
	if token == "" {
		return builder.Limit(size)
	}

	b, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return builder.Limit(size)
	}

	var c struct {
		LastID int64 `json:"last_id"`
	}
	if err = p.codec.Unmarshal(b, &c); err != nil {
		return builder.Limit(size)
	}

	return builder.Limit(size).Where("id > ?", c.LastID)
}
