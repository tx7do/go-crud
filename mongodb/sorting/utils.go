package sorting

import "regexp"

// fieldNameRegexp 允许的字段名：以字母或下划线开头，后续允许字母数字下划线和点（点用于 JSON key 或表别名）
var fieldNameRegexp = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_\.]*$`)

func toDirection(desc bool) string {
	if desc {
		return "DESC"
	}
	return "ASC"
}
