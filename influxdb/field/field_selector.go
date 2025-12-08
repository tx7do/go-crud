package field

import (
	"regexp"
	"strings"

	"github.com/tx7do/go-crud/influxdb/query"
	"github.com/tx7do/go-utils/stringcase"
)

var fieldNameRegexp = regexp.MustCompile(`^[A-Za-z0-9_.]+$`)

// Selector 用于构建 InfluxDB 查询中的 SELECT 列表
type Selector struct{}

// NewFieldSelector 返回一个新的 Selector。
func NewFieldSelector() *Selector { return &Selector{} }

// BuildSelector 为给定的 builder 构建 SELECT 列表并设置到 builder 中。
// 当 fields 为空或无有效字段时返回原 builder 和 nil 错误。
// 支持 "*" 表示全选（会调用 builder.Select(nil)）。
func (fs Selector) BuildSelector(builder *query.Builder, fields []string) (*query.Builder, error) {
	if builder == nil {
		return nil, nil
	}
	if len(fields) == 0 {
		return builder, nil
	}

	fields = NormalizePaths(fields)
	if len(fields) == 0 {
		return builder, nil
	}

	cols := make([]string, 0, len(fields))
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		// 支持 '*' 表示全选
		if f == "*" {
			// InfluxDB Builder 的 Select(nil) 表示 SELECT *
			builder = builder.Select(nil)
			return builder, nil
		}
		// 简单校验字段名，允许字母数字下划线和点
		if !fieldNameRegexp.MatchString(f) {
			continue
		}

		var col string
		if strings.Contains(f, ".") {
			parts := strings.SplitN(f, ".", 2)
			col = stringcase.ToSnakeCase(parts[0]) + "." + parts[1]
		} else {
			col = stringcase.ToSnakeCase(f)
		}

		cols = append(cols, col)
	}

	if len(cols) == 0 {
		return builder, nil
	}

	builder = builder.Select(cols)
	return builder, nil
}
