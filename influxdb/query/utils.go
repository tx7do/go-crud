package query

import (
	"fmt"
	"reflect"
	"strings"
)

// formatValue 根据类型格式化值；slice 会被格式化为 "(v1,v2,...)"
func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}

	switch t := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", escapeString(t))
	case bool:
		if t {
			return "true"
		}
		return "false"
	case fmt.Stringer:
		return fmt.Sprintf("'%s'", escapeString(t.String()))
	default:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			parts := make([]string, 0, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				parts = append(parts, formatValue(rv.Index(i).Interface()))
			}
			return "(" + strings.Join(parts, ",") + ")"
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64:
			return fmt.Sprintf("%v", v)
		default:
			// fallback to quoted string
			return fmt.Sprintf("'%s'", escapeString(fmt.Sprintf("%v", v)))
		}
	}
}

// formatRegex 将值包装为 /.../ 形式，用于 =~ 操作
func formatRegex(v interface{}) string {
	s := ""
	switch t := v.(type) {
	case string:
		s = t
	default:
		s = fmt.Sprintf("%v", v)
	}
	// 简单转义斜线
	s = strings.ReplaceAll(s, "/", "\\/")
	return fmt.Sprintf("/%s/", s)
}

// escapeString 转义单引号等
func escapeString(s string) string {
	// 转义单引号和反斜杠
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "'", "\\'")
	return s
}
