package field

import (
	"regexp"
	"strings"

	"github.com/tx7do/go-crud/opensearch/query"
	"github.com/tx7do/go-utils/stringcase"
)

var fieldNameRegexp = regexp.MustCompile(`^[A-Za-z0-9_.]+$`)

// Selector 字段选择器，用于构建 OpenSearch 查询的 _source 字段
// 将传入的字段路径规范化、校验并转换为 OpenSearch _source 数组。
type Selector struct{}

// NewFieldSelector 返回一个新的 Selector。
func NewFieldSelector() *Selector { return &Selector{} }

// BuildSelector 为给定的 builder 构建 _source 并设置到 builder 中。
// 当 fields 为空或无有效字段时返回原 builder。
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

	var sourceFields []string
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		// 简单校验字段名，允许字母数字下划线和点
		if !fieldNameRegexp.MatchString(f) {
			continue
		}
		var key string
		if strings.Contains(f, ".") {
			parts := strings.SplitN(f, ".", 2)
			key = stringcase.ToSnakeCase(parts[0]) + "." + parts[1]
		} else {
			key = stringcase.ToSnakeCase(f)
		}
		sourceFields = append(sourceFields, key)
	}

	if len(sourceFields) == 0 {
		return builder, nil
	}

	builder.SetSource(sourceFields...)
	return builder, nil
}
