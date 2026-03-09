package field

import (
	"strings"

	"entgo.io/ent/dialect/sql"
)

// Selector 字段选择器，用于构建SELECT语句中的字段列表。
type Selector struct{}

func NewFieldSelector() *Selector { return &Selector{} }

// BuildSelect 构建字段选择
func (fs Selector) BuildSelect(s *sql.Selector, fields []string) {
	if len(fields) > 0 {
		fields = NormalizePaths(fields)
		s.Select(fields...)
	}
}

// BuildSelectorWithTable 构建字段选择器并指定表名
func (fs Selector) BuildSelectorWithTable(table string, fields []string) (func(s *sql.Selector), error) {
	if len(fields) > 0 {
		return func(s *sql.Selector) {
			fs.BuildSelectWithTable(s, table, fields)
		}, nil
	}
	return nil, nil
}

// BuildSelectWithTable 构建字段选择，给未带点的字段前置 table 名称
func (fs Selector) BuildSelectWithTable(s *sql.Selector, table string, fields []string) {
	if len(fields) == 0 {
		return
	}
	fields = NormalizePaths(fields)
	if table != "" {
		for i, f := range fields {
			if !strings.Contains(f, ".") {
				fields[i] = quoteIdentPart(table) + "." + f
			}
		}
	}
	s.Select(fields...)
}

// BuildSelector 构建字段选择器
func (fs Selector) BuildSelector(fields []string) (func(s *sql.Selector), error) {
	if len(fields) > 0 {
		return func(s *sql.Selector) {
			fs.BuildSelect(s, fields)
		}, nil
	}

	return nil, nil
}
