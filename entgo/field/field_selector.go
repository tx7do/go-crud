package field

import (
	"strings"

	"entgo.io/ent/dialect/sql"

	"github.com/tx7do/go-crud/entgo/ent"
)

// filterColumns 仅保留属于当前表（s.TableName()）真实列的字段，丢弃未知列名
// 以防止跨列访问。返回的切片长度可能为 0。
// 表未在白名单映射中时 fail-open（保留全部字段），保持旧行为；但无论表是否注册，
// 路径的每一段都必须通过硬性标识符校验（ent.IsValidFieldPath）——ent 的 Builder.Ident
// 会把含括号/引号的字符串原样写入 SELECT 列表，此前仅校验第一个点之后的部分，
// 前缀可携带注入载荷。
func filterColumns(s *sql.Selector, fields []string) []string {
	allowed := make([]string, 0, len(fields))
	for _, f := range fields {
		if !ent.IsValidFieldPath(f) {
			continue
		}
		// 仅校验不带表前缀的列部分（形如 "table.col" 时取 col）。
		col := f
		if idx := strings.Index(col, "."); idx >= 0 {
			col = col[idx+1:]
		}
		if col == "" {
			continue
		}
		if !columnAllowed(s.TableName(), col) {
			continue
		}
		allowed = append(allowed, f)
	}
	return allowed
}

// columnAllowed 判断字段是否应被允许用于当前表。
// 仅当表已被白名单映射（实体表）且列不属于该表时拒绝；表未映射时 fail-open。
func columnAllowed(table, col string) bool {
	err := ent.CheckColumn(table, col)
	if err == nil {
		return true
	}
	if strings.Contains(err.Error(), "unknown table") {
		return true
	}
	return false
}

// Selector 字段选择器，用于构建SELECT语句中的字段列表。
type Selector struct{}

func NewFieldSelector() *Selector { return &Selector{} }

// BuildSelect 构建字段选择
func (fs Selector) BuildSelect(s *sql.Selector, fields []string) {
	if len(fields) > 0 {
		fields = filterColumns(s, fields)
		if len(fields) == 0 {
			return
		}
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
	fields = filterColumns(s, fields)
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
