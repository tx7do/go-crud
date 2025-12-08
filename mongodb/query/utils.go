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
