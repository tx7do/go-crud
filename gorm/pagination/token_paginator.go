package pagination

import (
	"github.com/tx7do/go-crud/pagination"
	"gorm.io/gorm"

	"github.com/tx7do/go-crud/pagination/paginator"
)

// TokenPaginator 基于 Token 的分页器
type TokenPaginator struct {
	impl pagination.Paginator
}

func NewTokenPaginator() *TokenPaginator {
	return &TokenPaginator{
		impl: paginator.NewTokenPaginatorWithDefault(),
	}
}

// BuildDB 根据传入 token/size 更新状态并返回应用到 *gorm.DB 的闭包
// 使用示例： db = paginator.BuildDB(token, size)(db)
func (p *TokenPaginator) BuildDB(token string, pageSize int) func(*gorm.DB) *gorm.DB {
	p.impl.
		WithToken(token).
		WithPage(pageSize)

	return func(db *gorm.DB) *gorm.DB {
		if db == nil {
			return db
		}

		// 无 token 或解码失败时只应用 pageSize
		if token == "" {
			return db.Limit(p.impl.Size())
		}

		lastID, ok := pagination.VerifyAndDecode(token, nil)
		if !ok {
			return db.Limit(p.impl.Size())
		}

		db = db.Where("id > ?", lastID)

		return db.Limit(p.impl.Size())
	}
}
