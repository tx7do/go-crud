package sorting

import (
	"strings"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/mongodb/query"
	"github.com/tx7do/go-utils/stringcase"
	bsonV2 "go.mongodb.org/mongo-driver/v2/bson"
)

// StructuredSorting 将结构化排序指令转换为 MongoDB 的 ORDER BY 子句
type StructuredSorting struct{}

// NewStructuredSorting 创建实例
func NewStructuredSorting() *StructuredSorting {
	return &StructuredSorting{}
}

// BuildOrderClause 根据传入的排序指令构造 ORDER BY 子句
func (ss StructuredSorting) BuildOrderClause(builder *query.Builder, orders []*paginationV1.Sorting) *query.Builder {
	if builder == nil || len(orders) == 0 {
		return builder
	}

	var sortFields []bsonV2.E
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

		var col string
		if strings.Contains(field, ".") {
			parts := strings.SplitN(field, ".", 2)
			col = stringcase.ToSnakeCase(parts[0]) + "." + parts[1]
		} else {
			col = stringcase.ToSnakeCase(field)
		}

		dir := int32(1)
		if o.GetDirection() == paginationV1.Sorting_DESC {
			dir = -1
		}
		sortFields = append(sortFields, bsonV2.E{Key: col, Value: dir})
	}

	if len(sortFields) > 0 {
		builder.SetSortWithPriority(sortFields)
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
