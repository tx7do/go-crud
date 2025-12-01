package pagination

import (
	"encoding/base64"

	"gorm.io/gorm"

	"github.com/go-kratos/kratos/v2/encoding"
	_ "github.com/go-kratos/kratos/v2/encoding/json"

	"github.com/tx7do/go-curd/paginator"
)

// TokenPaginator 基于 Token 的分页器
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

// BuildDB 根据传入 token/size 更新状态并返回应用到 *gorm.DB 的闭包
// 使用示例： db = paginator.BuildDB(token, size)(db)
func (p *TokenPaginator) BuildDB(token string, pageSize int) func(*gorm.DB) *gorm.DB {
	p.impl.
		WithToken(token).
		WithPage(pageSize)

	type cursor struct {
		LastID int64 `json:"last_id"`
	}

	return func(db *gorm.DB) *gorm.DB {
		if db == nil {
			return db
		}

		// 无 token 或解码失败时只应用 pageSize
		if token == "" {
			return db.Limit(p.impl.Size())
		}

		b, err := base64.StdEncoding.DecodeString(token)
		if err != nil {
			return db.Limit(p.impl.Size())
		}

		var c cursor
		if err = p.codec.Unmarshal(b, &c); err != nil {
			return db.Limit(p.impl.Size())
		}

		lastID := c.LastID
		db = db.Where("id > ?", lastID)

		return db.Limit(p.impl.Size())
	}
}
