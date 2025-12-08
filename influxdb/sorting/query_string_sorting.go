package sorting

import (
	"strings"

	"github.com/tx7do/go-crud/influxdb/query"
	"github.com/tx7do/go-utils/stringcase"
)

// QueryStringSorting 用于把查询字符串转换为 InfluxDB 的 ORDER BY 子句
type QueryStringSorting struct{}

// NewQueryStringSorting 创建实例
func NewQueryStringSorting() *QueryStringSorting {
	return &QueryStringSorting{}
}

// parseOrder 将单个 order 表达式解析为列名和是否降序
// 支持 "-field", "field", "field:desc", "field.desc"（如果字段本身含 '.'，只在最后一段为 desc 时视为方向）
func parseOrder(expr string) (col string, desc bool, ok bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", false, false
	}

	// 前缀 '-'
	if strings.HasPrefix(expr, "-") {
		desc = true
		expr = strings.TrimPrefix(expr, "-")
		expr = strings.TrimSpace(expr)
		if expr == "" {
			return "", false, false
		}
	}

	// 支持 "field:desc"
	if parts := strings.SplitN(expr, ":", 2); len(parts) == 2 {
		expr = strings.TrimSpace(parts[0])
		if strings.EqualFold(strings.TrimSpace(parts[1]), "desc") {
			desc = true
		}
	}

	// 支持尾部 ".desc"（只判断最后一段）
	if idx := strings.LastIndex(expr, "."); idx != -1 {
		tail := expr[idx+1:]
		if strings.EqualFold(tail, "desc") || strings.EqualFold(tail, "asc") {
			if strings.EqualFold(tail, "desc") {
				desc = true
			}
			expr = expr[:idx]
		}
	}

	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", false, false
	}

	// 简单校验字段名，允许字母数字下划线和点（fieldNameRegexp 需在包内定义）
	if !fieldNameRegexp.MatchString(expr) {
		return "", false, false
	}

	// 对含 '.' 的字段，将第一段转为 snake_case（保持后续路径）
	if strings.Contains(expr, ".") {
		parts := strings.SplitN(expr, ".", 2)
		col = stringcase.ToSnakeCase(parts[0]) + "." + parts[1]
	} else {
		col = stringcase.ToSnakeCase(expr)
	}

	return col, desc, true
}

// BuildOrderClause 根据 orderBys 构建排序并设置到 InfluxDB 的 builder（使用 Builder.OrderBy）
func (qss QueryStringSorting) BuildOrderClause(builder *query.Builder, orderBys []string) *query.Builder {
	if builder == nil || len(orderBys) == 0 {
		return builder
	}

	for _, ob := range orderBys {
		if ob == "" {
			continue
		}
		col, desc, ok := parseOrder(ob)
		if !ok {
			continue
		}
		// 将每个排序项应用到 InfluxDB Builder
		builder = builder.OrderBy(col, desc)
	}

	return builder
}

// BuildOrderClauseWithDefaultField 当 orderBys 为空时使用默认排序字段
// defaultOrderField 为空则不应用默认排序
func (qss QueryStringSorting) BuildOrderClauseWithDefaultField(builder *query.Builder, orderBys []string, defaultOrderField string, defaultDesc bool) *query.Builder {
	if len(orderBys) == 0 && defaultOrderField != "" {
		order := defaultOrderField
		if defaultDesc {
			order = "-" + defaultOrderField
		}
		return qss.BuildOrderClause(builder, []string{order})
	}
	return qss.BuildOrderClause(builder, orderBys)
}
