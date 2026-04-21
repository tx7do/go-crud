package sorting

import (
	"strings"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/opensearch/query"
	"github.com/tx7do/go-utils/stringcase"
)

// StructuredSorting 将结构化排序指令转换为 OpenSearch 的 sort 数组
type StructuredSorting struct{}

// NewStructuredSorting 创建实例
func NewStructuredSorting() *StructuredSorting {
	return &StructuredSorting{}
}

// BuildOrderClause 根据传入的排序指令构造 OpenSearch sort 数组
func (ss StructuredSorting) BuildOrderClause(builder *query.Builder, orders []*paginationV1.Sorting) *query.Builder {
	if builder == nil || len(orders) == 0 {
		return builder
	}
	for _, o := range orders {
		if o == nil {
			continue
		}
		field := strings.TrimSpace(o.GetField())
		if field == "" {
			continue
		}
		// 字段名处理，允许点用于 JSON 或表别名
		var col string
		if strings.Contains(field, ".") {
			parts := strings.SplitN(field, ".", 2)
			col = stringcase.ToSnakeCase(parts[0]) + "." + parts[1]
		} else {
			col = stringcase.ToSnakeCase(field)
		}
		asc := true
		if o.GetDirection() == paginationV1.Sorting_DESC {
			asc = false
		}
		builder.SetSort(col, asc)
	}
	return builder
}

// BuildOrderClauseWithDefaultField 当 orders 为空时使用默认排序字段
func (ss StructuredSorting) BuildOrderClauseWithDefaultField(builder *query.Builder, orders []*paginationV1.Sorting, defaultOrderField string, defaultDesc bool) *query.Builder {
	if builder == nil {
		return builder
	}
	if len(orders) == 0 {
		if strings.TrimSpace(defaultOrderField) == "" {
			return builder
		}
		order := paginationV1.Sorting_ASC
		if defaultDesc {
			order = paginationV1.Sorting_DESC
		}
		orders = []*paginationV1.Sorting{
			{
				Field:     defaultOrderField,
				Direction: order,
			},
		}
	}
	return ss.BuildOrderClause(builder, orders)
}
