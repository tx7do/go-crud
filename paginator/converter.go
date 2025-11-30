package paginator

import (
	"strings"

	"github.com/tx7do/go-utils/stringcase"

	pagination "github.com/tx7do/go-curd/api/gen/go/pagination/v1"
)

// ConverterStringToOperator 将字符串转换为 pagination.Operator 枚举值
func ConverterStringToOperator(str string) pagination.Operator {
	str = strings.ToLower(stringcase.ToSnakeCase(str))

	switch str {
	case "eq", "equal", "equals":
		return pagination.Operator_EQ
	case "ne", "neq", "not", "not_equal", "not_equals", "not-equal":
		return pagination.Operator_NEQ
	case "gt", "greater_than", "greater-than":
		return pagination.Operator_GT
	case "gte", "greater_than_or_equal", "greater_equals", "greater-or-equal":
		return pagination.Operator_GTE
	case "lt", "less_than", "less-than":
		return pagination.Operator_LT
	case "lte", "less_than_or_equal", "less_equals", "less-or-equal":
		return pagination.Operator_LTE
	case "like":
		return pagination.Operator_LIKE
	case "ilike", "i_like":
		return pagination.Operator_ILIKE
	case "not_like", "notlike":
		return pagination.Operator_NOT_LIKE
	case "in":
		return pagination.Operator_IN
	case "nin", "not_in", "notin":
		return pagination.Operator_NIN
	case "is_null", "isnull":
		return pagination.Operator_IS_NULL
	case "is_not_null", "isnot_null", "isnotnull", "not_isnull":
		return pagination.Operator_IS_NOT_NULL
	case "between", "range":
		return pagination.Operator_BETWEEN
	case "regexp", "regex":
		return pagination.Operator_REGEXP
	case "iregexp", "i_regexp", "iregex":
		return pagination.Operator_IREGEXP
	case "contains":
		return pagination.Operator_CONTAINS
	case "icontains", "i_contains":
		return pagination.Operator_ICONTAINS
	case "starts_with", "startswith":
		return pagination.Operator_STARTS_WITH
	case "istarts_with", "i_starts_with", "istartswith":
		return pagination.Operator_ISTARTS_WITH
	case "ends_with", "endswith":
		return pagination.Operator_ENDS_WITH
	case "iends_with", "i_ends_with", "iendswith":
		return pagination.Operator_IENDS_WITH
	case "json_contains":
		return pagination.Operator_JSON_CONTAINS
	case "array_contains":
		return pagination.Operator_ARRAY_CONTAINS
	case "exists":
		return pagination.Operator_EXISTS
	case "search":
		return pagination.Operator_SEARCH
	case "exact":
		return pagination.Operator_EXACT
	case "iexact", "i_exact":
		return pagination.Operator_IEXACT
	default:
		return pagination.Operator_OPERATOR_UNSPECIFIED
	}
}

// IsValidOperatorString 检查字符串是否为有效的 pagination.Operator 枚举值
func IsValidOperatorString(str string) bool {
	op := ConverterStringToOperator(str)
	return op != pagination.Operator_OPERATOR_UNSPECIFIED
}

// ConverterStringToDatePart 将字符串转换为 pagination.DatePart 枚举
func ConverterStringToDatePart(s string) pagination.DatePart {
	s = strings.ToLower(stringcase.ToSnakeCase(s))

	switch s {
	case "date":
		return pagination.DatePart_DATE
	case "year", "yr":
		return pagination.DatePart_YEAR
	case "iso_year", "iso-year":
		return pagination.DatePart_ISO_YEAR
	case "quarter":
		return pagination.DatePart_QUARTER
	case "month":
		return pagination.DatePart_MONTH
	case "week":
		return pagination.DatePart_WEEK
	case "week_day", "week-day", "weekday":
		return pagination.DatePart_WEEK_DAY
	case "iso_week_day", "iso-week-day":
		return pagination.DatePart_ISO_WEEK_DAY
	case "day":
		return pagination.DatePart_DAY
	case "time":
		return pagination.DatePart_TIME
	case "hour":
		return pagination.DatePart_HOUR
	case "minute", "min":
		return pagination.DatePart_MINUTE
	case "second", "sec":
		return pagination.DatePart_SECOND
	case "microsecond":
		return pagination.DatePart_MICROSECOND
	default:
		return pagination.DatePart_DATE_PART_UNSPECIFIED
	}
}

// IsValidDatePartString 检查字符串是否为有效的 pagination.DatePart 枚举值
func IsValidDatePartString(str string) bool {
	dp := ConverterStringToDatePart(str)
	return dp != pagination.DatePart_DATE_PART_UNSPECIFIED
}
