package field

import (
	"regexp"
	"strings"

	"github.com/tx7do/go-crud/mongodb/query"
	"github.com/tx7do/go-utils/stringcase"
	bsonV2 "go.mongodb.org/mongo-driver/v2/bson"
)

var fieldNameRegexp = regexp.MustCompile(`^[A-Za-z0-9_.]+$`)

// Selector 字段选择器，用于构建 MongoDB 查询中的 projection（投影）
// 将传入的字段路径规范化、校验并转换为 mongo projection 文档。
type Selector struct{}

// NewFieldSelector 返回一个新的 Selector。
func NewFieldSelector() *Selector { return &Selector{} }

// BuildSelector 为给定的 builder 构建 projection 并设置到 builder 中。
// 当 fields 为空或无有效字段时返回原 builder 和 nil 错误。
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

	proj := bsonV2.M{}
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

		proj[key] = int32(1)
	}

	if len(proj) == 0 {
		return builder, nil
	}

	// 将 projection 设置到 builder（需在 query.Builder 中实现 SetProjection）
	builder.SetProjection(proj)

	return builder, nil
}
