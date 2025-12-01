package field

import (
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

// BuildSelector 构建字段选择器
func (fs Selector) BuildSelector(fields []string) (func(s *sql.Selector), error) {
	if len(fields) > 0 {
		return func(s *sql.Selector) {
			fs.BuildSelect(s, fields)
		}, nil
	} else {
		return nil, nil
	}
}
