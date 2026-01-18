package sorting

import (
	"entgo.io/ent/dialect/sql"
	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

type StructuredSorting struct {
}

func NewStructuredSorting() *StructuredSorting {
	return &StructuredSorting{}
}

func (ss StructuredSorting) BuildSelector(orders []*paginationV1.Sorting) (func(s *sql.Selector), error) {
	if len(orders) == 0 {
		return nil, nil
	}

	return func(s *sql.Selector) {
		for _, order := range orders {
			if order == nil || order.GetField() == "" {
				continue
			}

			buildOrderBySelector(s, order.Field, order.GetDirection() == paginationV1.Sorting_DESC)
		}
	}, nil
}

// BuildSelectorWithDefaultField 构建排序选择器
// - orderBys: 排序字段列表
// - defaultOrderField: 默认排序字段
// - defaultDesc: 默认是否降序
func (ss StructuredSorting) BuildSelectorWithDefaultField(orders []*paginationV1.Sorting, defaultOrderField string, defaultDesc bool) (func(s *sql.Selector), error) {
	if len(orders) == 0 && defaultOrderField != "" {
		return func(s *sql.Selector) {
			buildOrderBySelector(s, defaultOrderField, defaultDesc)
		}, nil
	} else {
		return ss.BuildSelector(orders)
	}
}
