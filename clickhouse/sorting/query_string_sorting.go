package sorting

import (
	"fmt"
	"strings"

	"github.com/tx7do/go-crud/clickhouse/query"
	"github.com/tx7do/go-utils/stringcase"
)

// QueryStringSorting 用于把查询字符串转换为 ClickHouse 的 ORDER BY 子句
type QueryStringSorting struct{}

// NewQueryStringSorting 创建实例
func NewQueryStringSorting() *QueryStringSorting {
	return &QueryStringSorting{}
}

// parseOrderCH 将单个 order 表达式解析为 column expression 和 direction
// 支持格式:
//   - "-field"         -> field DESC
//   - "field"          -> field ASC
//   - "field:desc"     -> field DESC
//   - "field.desc"     -> field DESC
//
// 对含 '.' 的字段会生成 JSONExtractString(column, 'key')
func parseOrderCH(expr string) (string, string, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", "", false
	}

	desc := false
	if strings.HasPrefix(expr, "-") {
		desc = true
		expr = strings.TrimPrefix(expr, "-")
	}

	// 支持 'field:desc' 或 'field.desc'
	if parts := strings.SplitN(expr, ":", 2); len(parts) == 2 {
		expr = parts[0]
		if strings.EqualFold(parts[1], "desc") {
			desc = true
		}
	} else if parts = strings.SplitN(expr, ".", 2); len(parts) == 2 {
		// 注意：这里也支持 field.desc 风格（最后一段为 desc）
		// 但若 field 本身含多段（如 preferences.daily_email）则会在后续处理为 JSONExtract
		// 因此需要判断后段是否为 desc
		// 如果是 desc，则使用前半部分作为字段；否则保留原 expr（后续会判定为 JSON）
		if strings.EqualFold(parts[1], "desc") {
			expr = parts[0]
			desc = true
		}
	}

	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", "", false
	}

	// 简单校验字段名，避免注入（允许点）
	if !fieldNameRegexp.MatchString(expr) {
		return "", "", false
	}

	// 构造列表达式：针对含 '.' 的字段使用 JSONExtractString(column, 'key')
	colExpr := ""
	if strings.Contains(expr, ".") {
		parts := strings.SplitN(expr, ".", 2)
		col := stringcase.ToSnakeCase(parts[0])
		colExpr = fmt.Sprintf("%s.%s", col, parts[1])
	} else {
		colExpr = stringcase.ToSnakeCase(expr)
	}

	dir := toDirection(desc)
	return colExpr, dir, true
}

// BuildOrderClause 根据 orderBys 构建 ClickHouse 的 ORDER BY 子句（不带末尾空格）
// 返回空字符串表示无排序
func (qss QueryStringSorting) BuildOrderClause(builder *query.Builder, orderBys []string) *query.Builder {
	if len(orderBys) == 0 {
		return builder
	}

	for _, ob := range orderBys {
		if ob == "" {
			continue
		}

		col, dir, ok := parseOrderCH(ob)
		if !ok {
			continue
		}

		builder.OrderBy(col, dir == "DESC")
	}

	return builder
}

// BuildOrderClauseWithDefaultField 当 orderBys 为空时使用默认排序字段
// defaultOrderField 为空则不应用默认排序
func (qss QueryStringSorting) BuildOrderClauseWithDefaultField(builder *query.Builder, orderBys []string, defaultOrderField string, defaultDesc bool) *query.Builder {
	if len(orderBys) == 0 {
		var order string
		if defaultDesc {
			order = "-" + defaultOrderField
		} else {
			order = defaultOrderField
		}
		return qss.BuildOrderClause(builder, []string{order})
	}

	return qss.BuildOrderClause(builder, orderBys)
}
