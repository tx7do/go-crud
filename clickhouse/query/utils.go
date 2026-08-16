package query

import (
	"regexp"
	"strings"
)

// isValidIdentifier 验证表名或列名是否合法
func isValidIdentifier(identifier string) bool {
	identifier = strings.TrimSpace(identifier)
	if identifier == "" {
		return false
	}

	// 支持用 ` 包裹的标识符：`name`
	if strings.HasPrefix(identifier, "`") && strings.HasSuffix(identifier, "`") && len(identifier) >= 2 {
		inner := identifier[1 : len(identifier)-1]
		matched, _ := regexp.MatchString(`^[a-zA-Z_][a-zA-Z0-9_]*$`, inner)
		return matched
	}

	// 普通未包裹的标识符
	matched, _ := regexp.MatchString(`^[a-zA-Z_][a-zA-Z0-9_]*$`, identifier)
	return matched
}

// isValidCondition 验证条件语句是否合法
func isValidCondition(condition string) bool {
	// 简单验证条件中是否包含危险字符
	return !strings.Contains(condition, ";") && !strings.Contains(condition, "--")
}

// LimitKeywordPattern 匹配作为 SQL 关键字的 LIMIT（大小写不敏感，词边界
// 锚定，后随空白/数字/结尾）。用于 Get 强制 LIMIT 1 时识别查询是否已含
// LIMIT——此前用 strings.Contains(upper,"LIMIT") 会把 rate_limit 这类
// 列名误判成已含 LIMIT 而跳过，导致返回多行。
var LimitKeywordPattern = regexp.MustCompile(`(?i)(^|[^A-Za-z0-9_])LIMIT\s`)
