package query

import (
	"regexp"
	"strings"
)

// isValidIdentifier 验证表名或列名是否合法
func isValidIdentifier(identifier string) bool {
	// 仅允许字母、数字、下划线，且不能以数字开头
	matched, _ := regexp.MatchString(`^[a-zA-Z_][a-zA-Z0-9_]*$`, identifier)
	return matched
}

// isValidCondition 验证条件语句是否合法
func isValidCondition(condition string) bool {
	// 简单验证条件中是否包含危险字符
	return !strings.Contains(condition, ";") && !strings.Contains(condition, "--")
}
