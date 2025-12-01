package sorting

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
)

// QueryStringSorting 用于把查询字符串转换为 GORM 的 order scope
type QueryStringSorting struct{}

// NewQueryStringSorting 创建实例
func NewQueryStringSorting() *QueryStringSorting {
	return &QueryStringSorting{}
}

// parseOrder 将单个 order 表达式解析为 field 和 direction
// 支持格式:
//   - "-field"         -> field DESC
//   - "field"          -> field ASC
//   - "field:desc"     -> field DESC
//   - "field.desc"     -> field DESC
func parseOrder(expr string) (string, string, bool) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", "", false
	}

	desc := false
	// 前缀 '-' 表示 DESC
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
		expr = parts[0]
		if strings.EqualFold(parts[1], "desc") {
			desc = true
		}
	}

	expr = strings.TrimSpace(expr)
	if expr == "" {
		return "", "", false
	}

	// 简单校验字段名，避免注入（允许表别名如 "t.field"）
	if !fieldNameRegexp.MatchString(expr) {
		return "", "", false
	}

	dir := toDirection(desc)

	return expr, dir, true
}

// BuildScope 根据 orderBys 构建 GORM scope（可与 db.Scopes 一起使用）
// orderBys 示例: []string{"-created_at", "name:asc", "user.id.desc"}
func (qss QueryStringSorting) BuildScope(orderBys []string) func(*gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if len(orderBys) == 0 {
			return db
		}
		for _, ob := range orderBys {
			field, dir, ok := parseOrder(ob)
			if !ok {
				// 跳过不合法的字段表达式
				continue
			}
			db = db.Order(fmt.Sprintf("%s %s", field, dir))
		}
		return db
	}
}

// BuildScopeWithDefaultField 当 orderBys 为空时使用默认排序字段
// defaultOrderField 为空则不应用默认排序
func (qss QueryStringSorting) BuildScopeWithDefaultField(orderBys []string, defaultOrderField string, defaultDesc bool) func(*gorm.DB) *gorm.DB {
	if len(orderBys) == 0 && strings.TrimSpace(defaultOrderField) != "" {
		def := strings.TrimSpace(defaultOrderField)
		// 校验默认字段
		if fieldNameRegexp.MatchString(def) {
			dir := toDirection(defaultDesc)
			return func(db *gorm.DB) *gorm.DB {
				return db.Order(fmt.Sprintf("%s %s", def, dir))
			}
		}
		// 默认字段不合法时返回空 scope
		return func(db *gorm.DB) *gorm.DB { return db }
	}
	return qss.BuildScope(orderBys)
}
