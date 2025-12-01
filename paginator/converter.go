package paginator

import (
	"strings"

	"github.com/tx7do/go-utils/stringcase"

	pagination "github.com/tx7do/go-curd/api/gen/go/pagination/v1"
)

var operatorMap = map[string]pagination.Operator{
	"eq":     pagination.Operator_EQ,
	"equal":  pagination.Operator_EQ,
	"equals": pagination.Operator_EQ,

	"ne":         pagination.Operator_NEQ,
	"neq":        pagination.Operator_NEQ,
	"not":        pagination.Operator_NEQ,
	"not_equal":  pagination.Operator_NEQ,
	"not_equals": pagination.Operator_NEQ,
	"not-equal":  pagination.Operator_NEQ,

	"gt":           pagination.Operator_GT,
	"greater_than": pagination.Operator_GT,
	"greater-than": pagination.Operator_GT,

	"gte":                   pagination.Operator_GTE,
	"greater_than_or_equal": pagination.Operator_GTE,
	"greater_equals":        pagination.Operator_GTE,
	"greater_or_equal":      pagination.Operator_GTE,
	"greater-or-equal":      pagination.Operator_GTE,

	"lt":        pagination.Operator_LT,
	"less_than": pagination.Operator_LT,
	"less-than": pagination.Operator_LT,

	"lte":                pagination.Operator_LTE,
	"less_than_or_equal": pagination.Operator_LTE,
	"less_equals":        pagination.Operator_LTE,
	"less_or_equal":      pagination.Operator_LTE,
	"less-or-equal":      pagination.Operator_LTE,

	"like": pagination.Operator_LIKE,

	"ilike":  pagination.Operator_ILIKE,
	"i_like": pagination.Operator_ILIKE,

	"not_like": pagination.Operator_NOT_LIKE,
	"notlike":  pagination.Operator_NOT_LIKE,

	"in": pagination.Operator_IN,

	"nin":    pagination.Operator_NIN,
	"not_in": pagination.Operator_NIN,
	"notin":  pagination.Operator_NIN,

	"is_null": pagination.Operator_IS_NULL,
	"isnull":  pagination.Operator_IS_NULL,

	"is_not_null": pagination.Operator_IS_NOT_NULL,
	"isnot_null":  pagination.Operator_IS_NOT_NULL,
	"isnotnull":   pagination.Operator_IS_NOT_NULL,
	"not_isnull":  pagination.Operator_IS_NOT_NULL,

	"between": pagination.Operator_BETWEEN,
	"range":   pagination.Operator_BETWEEN,

	"regexp": pagination.Operator_REGEXP,
	"regex":  pagination.Operator_REGEXP,

	"iregexp":  pagination.Operator_IREGEXP,
	"i_regexp": pagination.Operator_IREGEXP,
	"iregex":   pagination.Operator_IREGEXP,

	"contains": pagination.Operator_CONTAINS,

	"icontains":  pagination.Operator_ICONTAINS,
	"i_contains": pagination.Operator_ICONTAINS,

	"starts_with": pagination.Operator_STARTS_WITH,
	"startswith":  pagination.Operator_STARTS_WITH,

	"istarts_with":  pagination.Operator_ISTARTS_WITH,
	"i_starts_with": pagination.Operator_ISTARTS_WITH,
	"istartswith":   pagination.Operator_ISTARTS_WITH,

	"ends_with": pagination.Operator_ENDS_WITH,
	"endswith":  pagination.Operator_ENDS_WITH,

	"iends_with":  pagination.Operator_IENDS_WITH,
	"i_ends_with": pagination.Operator_IENDS_WITH,
	"iendswith":   pagination.Operator_IENDS_WITH,

	"json_contains":  pagination.Operator_JSON_CONTAINS,
	"array_contains": pagination.Operator_ARRAY_CONTAINS,
	"exists":         pagination.Operator_EXISTS,
	"search":         pagination.Operator_SEARCH,
	"exact":          pagination.Operator_EXACT,

	"iexact":  pagination.Operator_IEXACT,
	"i_exact": pagination.Operator_IEXACT,
}

// ConverterStringToOperator 将字符串转换为 pagination.Operator 枚举值
func ConverterStringToOperator(str string) pagination.Operator {
	key := strings.ToLower(stringcase.ToSnakeCase(str))
	if v, ok := operatorMap[key]; ok {
		return v
	}
	return pagination.Operator_OPERATOR_UNSPECIFIED
}

// IsValidOperatorString 检查字符串是否为有效的 pagination.Operator 枚举值
func IsValidOperatorString(str string) bool {
	op := ConverterStringToOperator(str)
	return op != pagination.Operator_OPERATOR_UNSPECIFIED
}

var datePartMap = map[string]pagination.DatePart{
	"date": pagination.DatePart_DATE,

	"year": pagination.DatePart_YEAR,
	"yr":   pagination.DatePart_YEAR,

	"iso_year": pagination.DatePart_ISO_YEAR,
	"iso-year": pagination.DatePart_ISO_YEAR,

	"quarter": pagination.DatePart_QUARTER,
	"month":   pagination.DatePart_MONTH,
	"week":    pagination.DatePart_WEEK,

	"week_day": pagination.DatePart_WEEK_DAY,
	"week-day": pagination.DatePart_WEEK_DAY,
	"weekday":  pagination.DatePart_WEEK_DAY,

	"iso_week_day": pagination.DatePart_ISO_WEEK_DAY,
	"iso-week-day": pagination.DatePart_ISO_WEEK_DAY,

	"day":  pagination.DatePart_DAY,
	"time": pagination.DatePart_TIME,
	"hour": pagination.DatePart_HOUR,

	"minute": pagination.DatePart_MINUTE,
	"min":    pagination.DatePart_MINUTE,

	"second": pagination.DatePart_SECOND,
	"sec":    pagination.DatePart_SECOND,

	"microsecond": pagination.DatePart_MICROSECOND,
}

// ConverterStringToDatePart 将字符串转换为 pagination.DatePart 枚举
func ConverterStringToDatePart(s string) pagination.DatePart {
	key := strings.ToLower(stringcase.ToSnakeCase(s))
	if v, ok := datePartMap[key]; ok {
		return v
	}
	return pagination.DatePart_DATE_PART_UNSPECIFIED
}

// IsValidDatePartString 检查字符串是否为有效的 pagination.DatePart 枚举值
func IsValidDatePartString(str string) bool {
	dp := ConverterStringToDatePart(str)
	return dp != pagination.DatePart_DATE_PART_UNSPECIFIED
}
