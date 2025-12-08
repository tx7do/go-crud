package sorting

import (
	"strings"

	pagination "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/clickhouse/query"
)

// StructuredSorting 将结构化排序指令转换为 ClickHouse 的 ORDER BY 子句
type StructuredSorting struct{}

// NewStructuredSorting 创建实例
func NewStructuredSorting() *StructuredSorting {
	return &StructuredSorting{}
}

// BuildOrderClause 根据传入的排序指令构造 ORDER BY 子句
func (ss StructuredSorting) BuildOrderClause(builder *query.Builder, orders []*pagination.Sorting) *query.Builder {
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

		builder.OrderBy(field, o.GetOrder() == pagination.Sorting_DESC)
	}

	return builder
}

// BuildOrderClauseWithDefaultField 当 orders 为空时使用默认排序字段
func (ss StructuredSorting) BuildOrderClauseWithDefaultField(builder *query.Builder, orders []*pagination.Sorting, defaultOrderField string, defaultDesc bool) *query.Builder {
	if len(orders) == 0 {
		order := pagination.Sorting_DESC
		if !defaultDesc {
			order = pagination.Sorting_ASC
		}
		return ss.BuildOrderClause(builder, []*pagination.Sorting{
			{
				Field: defaultOrderField,
				Order: order,
			},
		})
	}

	return ss.BuildOrderClause(builder, orders)
}
