package sorting

import (
	"strings"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/clickhouse/query"
)

// StructuredSorting 将结构化排序指令转换为 ClickHouse 的 ORDER BY 子句
type StructuredSorting struct{}

// NewStructuredSorting 创建实例
func NewStructuredSorting() *StructuredSorting {
	return &StructuredSorting{}
}

// BuildOrderClause 根据传入的排序指令构造 ORDER BY 子句
func (ss StructuredSorting) BuildOrderClause(builder *query.Builder, orders []*paginationV1.Sorting) *query.Builder {
	if len(orders) == 0 {
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
		// 校验字段名，允许点用于 JSON 或表别名
		if !fieldNameRegexp.MatchString(field) {
			continue
		}

		builder.OrderBy(field, o.GetOrder() == paginationV1.Sorting_DESC)
	}

	return builder
}

// BuildOrderClauseWithDefaultField 当 orders 为空时使用默认排序字段
func (ss StructuredSorting) BuildOrderClauseWithDefaultField(builder *query.Builder, orders []*paginationV1.Sorting, defaultOrderField string, defaultDesc bool) *query.Builder {
	if len(orders) == 0 {
		order := paginationV1.Sorting_DESC
		if !defaultDesc {
			order = paginationV1.Sorting_ASC
		}
		return ss.BuildOrderClause(builder, []*paginationV1.Sorting{
			{
				Field: defaultOrderField,
				Order: order,
			},
		})
	}

	return ss.BuildOrderClause(builder, orders)
}
