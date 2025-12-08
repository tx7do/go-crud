package field

import (
	"github.com/tx7do/go-crud/clickhouse/query"
)

// Selector 字段选择器，用于构建 ClickHouse 查询中的 SELECT 子句。
type Selector struct{}

// NewFieldSelector 返回一个新的 Selector。
func NewFieldSelector() *Selector { return &Selector{} }

// BuildSelector 返回一个用于将 SELECT 子句拼接到给定基础 SQL 的函数。
// 当 fields 为空时返回 (nil, nil)。
// 返回的函数接收一个 baseSQL（例如 "FROM table WHERE ..."）并返回完整 SQL。
func (fs Selector) BuildSelector(builder *query.Builder, fields []string) (*query.Builder, error) {
	if len(fields) == 0 {
		return builder, nil
	}

	fields = NormalizePaths(fields)
	if len(fields) == 0 {
		return builder, nil
	}

	for _, field := range fields {
		builder.Select(field)
	}

	return builder, nil
}
