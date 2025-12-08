package query

import (
	"fmt"
	"strings"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/go-utils/stringcase"
)

// Builder 用于构建 ClickHouse SQL 查询
type Builder struct {
	table       string
	columns     []string
	distinct    bool
	conditions  []string
	orderBy     []string
	groupBy     []string
	having      []string
	joins       []string
	with        []string
	union       []string
	offset      int
	limit       int
	limitBy     string
	params      []interface{} // 用于存储参数
	useIndex    string        // 索引提示
	cacheResult bool          // 是否缓存查询结果
	debug       bool          // 是否启用调试
	log         *log.Helper
}

// NewQueryBuilder 创建一个新的 Builder 实例
func NewQueryBuilder(table string, log *log.Helper) *Builder {
	return &Builder{
		log:    log,
		table:  table,
		params: []interface{}{},
	}
}

// EnableDebug 启用调试模式
func (qb *Builder) EnableDebug() *Builder {
	qb.debug = true
	return qb
}

// logDebug 打印调试信息
func (qb *Builder) logDebug(message string) {
	if qb.debug {
		qb.log.Debug("[Builder Debug]:", message)
	}
}

// TableName 返回查询的表名
func (qb *Builder) TableName() string {
	return qb.table
}

func (qb *Builder) Logger() *log.Helper {
	return qb.log
}

// Select 设置查询的列
func (qb *Builder) Select(columns ...string) *Builder {
	if qb.columns == nil {
		qb.columns = []string{}
	}

	for _, column := range columns {
		if !isValidIdentifier(column) {
			panic("Invalid column name")
		}
		qb.columns = append(qb.columns, column)
	}

	return qb
}

// Distinct 设置 DISTINCT 查询
func (qb *Builder) Distinct() *Builder {
	qb.distinct = true
	return qb
}

// Where 添加查询条件并支持参数化
func (qb *Builder) Where(condition string, args ...interface{}) *Builder {
	if qb.conditions == nil {
		qb.conditions = []string{}
	}
	if qb.params == nil {
		qb.params = []interface{}{}
	}

	if !isValidCondition(condition) {
		panic("Invalid condition")
	}

	qb.conditions = append(qb.conditions, condition)
	qb.params = append(qb.params, args...)
	return qb
}

// OrderBy 设置排序条件
func (qb *Builder) OrderBy(order string, desc bool) *Builder {
	order = strings.TrimSpace(order)
	if order == "" {
		return qb
	}

	// 简单安全校验，避免注入或注释
	if strings.Contains(order, ";") || strings.Contains(order, "--") {
		panic("Invalid order expression")
	}

	var colExpr string
	if strings.Contains(order, ".") {
		parts := strings.SplitN(order, ".", 2)
		if !isValidIdentifier(parts[0]) {
			panic("Invalid order expression")
		}
		col := stringcase.ToSnakeCase(parts[0])
		jsonKey := parts[1] // 保留原样（可能包含点）
		colExpr = fmt.Sprintf("JSONExtractString(%s, '%s')", col, jsonKey)
	} else {
		//if !isValidIdentifier(order) {
		//	panic("Invalid order expression")
		//}
		colExpr = stringcase.ToSnakeCase(order)
	}

	dir := "ASC"
	if desc {
		dir = "DESC"
	}

	qb.orderBy = append(qb.orderBy, fmt.Sprintf("%s %s", colExpr, dir))
	return qb
}

// GroupBy 设置分组条件
func (qb *Builder) GroupBy(columns ...string) *Builder {
	qb.groupBy = append(qb.groupBy, columns...)
	return qb
}

// Having 添加分组后的过滤条件并支持参数化
func (qb *Builder) Having(condition string, args ...interface{}) *Builder {
	qb.having = append(qb.having, condition)
	qb.params = append(qb.params, args...)
	return qb
}

// Join 添加 JOIN 操作
func (qb *Builder) Join(joinType, table, onCondition string) *Builder {
	join := fmt.Sprintf("%s JOIN %s ON %s", joinType, table, onCondition)
	qb.joins = append(qb.joins, join)
	return qb
}

// With 添加 WITH 子句
func (qb *Builder) With(expression string) *Builder {
	qb.with = append(qb.with, expression)
	return qb
}

// Union 添加 UNION 操作
func (qb *Builder) Union(query string) *Builder {
	qb.union = append(qb.union, query)
	return qb
}

// Limit 设置查询结果的限制数量
func (qb *Builder) Limit(limit int) *Builder {
	qb.limit = limit
	return qb
}

// Offset 设置查询结果的偏移量
func (qb *Builder) Offset(offset int) *Builder {
	qb.offset = offset
	return qb
}

// UseIndex 设置索引提示
func (qb *Builder) UseIndex(index string) *Builder {
	qb.useIndex = index
	return qb
}

// CacheResult 启用查询结果缓存
func (qb *Builder) CacheResult() *Builder {
	qb.cacheResult = true
	return qb
}

// ArrayJoin 添加 ARRAY JOIN 子句
func (qb *Builder) ArrayJoin(expression string) *Builder {
	qb.joins = append(qb.joins, fmt.Sprintf("ARRAY JOIN %s", expression))
	return qb
}

// Final 添加 FINAL 修饰符
func (qb *Builder) Final() *Builder {
	qb.table = fmt.Sprintf("%s FINAL", qb.table)
	return qb
}

// Sample 添加 SAMPLE 子句
func (qb *Builder) Sample(sampleRate float64) *Builder {
	qb.table = fmt.Sprintf("%s SAMPLE %f", qb.table, sampleRate)
	return qb
}

// LimitBy 添加 LIMIT BY 子句
func (qb *Builder) LimitBy(limit int, columns ...string) *Builder {
	if limit <= 0 || len(columns) == 0 {
		return qb
	}

	qb.limit = limit

	for _, c := range columns {
		c = strings.TrimSpace(c)
		if c == "" {
			panic("Invalid limit by column")
		}
		if strings.Contains(c, ".") {
			parts := strings.SplitN(c, ".", 2)
			if !isValidIdentifier(parts[0]) {
				panic("Invalid limit by column")
			}
		} else {
			if !isValidIdentifier(c) {
				panic("Invalid limit by column")
			}
		}
	}

	qb.limitBy = fmt.Sprintf("LIMIT %d BY (%s)", limit, strings.Join(columns, ", "))
	qb.limit = 0
	return qb
}

// PreWhere 添加 PREWHERE 子句
func (qb *Builder) PreWhere(condition string, args ...interface{}) *Builder {
	qb.conditions = append([]string{condition}, qb.conditions...)
	qb.params = append(args, qb.params...)
	return qb
}

// Format 添加 FORMAT 子句
func (qb *Builder) Format(format string) *Builder {
	qb.union = append(qb.union, fmt.Sprintf("FORMAT %s", format))
	return qb
}

// Build 构建最终的 SQL 查询
func (qb *Builder) Build() (string, []interface{}) {
	query := ""

	if qb.cacheResult {
		query += "/* CACHE */ "
	}

	query += "SELECT "
	if qb.distinct {
		query += "DISTINCT "
	}
	query += qb.buildColumns()
	query += fmt.Sprintf(" FROM %s", qb.table)

	if qb.useIndex != "" {
		query += fmt.Sprintf(" USE INDEX (%s)", qb.useIndex)
	}

	if len(qb.conditions) > 0 {
		query += fmt.Sprintf(" WHERE %s", strings.Join(qb.conditions, " AND "))
	}

	if len(qb.groupBy) > 0 {
		query += fmt.Sprintf(" GROUP BY %s", strings.Join(qb.groupBy, ", "))
	}

	if len(qb.having) > 0 {
		query += fmt.Sprintf(" HAVING %s", strings.Join(qb.having, " AND "))
	}

	if len(qb.orderBy) > 0 {
		query += fmt.Sprintf(" ORDER BY %s", strings.Join(qb.orderBy, ", "))
	}

	if qb.limitBy != "" {
		query += " " + qb.limitBy
	} else if qb.limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", qb.limit)
	}

	if qb.offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", qb.offset)
	}

	return query, qb.params
}

func (qb *Builder) buildColumns() string {
	if len(qb.columns) == 0 {
		return "*"
	}
	return strings.Join(qb.columns, ", ")
}

// BuildWhereParam 构建 WHERE 子句和参数列表
func (qb *Builder) BuildWhereParam() (string, []interface{}) {
	return strings.Join(qb.conditions, " AND "), qb.params
}
