package pagination

import (
	"strings"

	"github.com/tx7do/go-utils/stringcase"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

var operatorMap = map[string]paginationV1.Operator{
	"eq":     paginationV1.Operator_EQ,
	"equal":  paginationV1.Operator_EQ,
	"equals": paginationV1.Operator_EQ,

	"ne":         paginationV1.Operator_NEQ,
	"neq":        paginationV1.Operator_NEQ,
	"not":        paginationV1.Operator_NEQ,
	"not_equal":  paginationV1.Operator_NEQ,
	"not_equals": paginationV1.Operator_NEQ,
	"not-equal":  paginationV1.Operator_NEQ,

	"gt":           paginationV1.Operator_GT,
	"greater_than": paginationV1.Operator_GT,
	"greater-than": paginationV1.Operator_GT,

	"gte":                   paginationV1.Operator_GTE,
	"greater_than_or_equal": paginationV1.Operator_GTE,
	"greater_equals":        paginationV1.Operator_GTE,
	"greater_or_equal":      paginationV1.Operator_GTE,
	"greater-or-equal":      paginationV1.Operator_GTE,

	"lt":        paginationV1.Operator_LT,
	"less_than": paginationV1.Operator_LT,
	"less-than": paginationV1.Operator_LT,

	"lte":                paginationV1.Operator_LTE,
	"less_than_or_equal": paginationV1.Operator_LTE,
	"less_equals":        paginationV1.Operator_LTE,
	"less_or_equal":      paginationV1.Operator_LTE,
	"less-or-equal":      paginationV1.Operator_LTE,

	"like": paginationV1.Operator_LIKE,

	"ilike":  paginationV1.Operator_ILIKE,
	"i_like": paginationV1.Operator_ILIKE,

	"not_like": paginationV1.Operator_NOT_LIKE,
	"notlike":  paginationV1.Operator_NOT_LIKE,

	"in": paginationV1.Operator_IN,

	"nin":    paginationV1.Operator_NIN,
	"not_in": paginationV1.Operator_NIN,
	"notin":  paginationV1.Operator_NIN,

	"is_null": paginationV1.Operator_IS_NULL,
	"isnull":  paginationV1.Operator_IS_NULL,

	"is_not_null": paginationV1.Operator_IS_NOT_NULL,
	"isnot_null":  paginationV1.Operator_IS_NOT_NULL,
	"isnotnull":   paginationV1.Operator_IS_NOT_NULL,
	"not_isnull":  paginationV1.Operator_IS_NOT_NULL,

	"between": paginationV1.Operator_BETWEEN,
	"range":   paginationV1.Operator_BETWEEN,

	"regexp": paginationV1.Operator_REGEXP,
	"regex":  paginationV1.Operator_REGEXP,

	"iregexp":  paginationV1.Operator_IREGEXP,
	"i_regexp": paginationV1.Operator_IREGEXP,
	"iregex":   paginationV1.Operator_IREGEXP,

	"contains": paginationV1.Operator_CONTAINS,

	"icontains":  paginationV1.Operator_ICONTAINS,
	"i_contains": paginationV1.Operator_ICONTAINS,

	"starts_with": paginationV1.Operator_STARTS_WITH,
	"startswith":  paginationV1.Operator_STARTS_WITH,

	"istarts_with":  paginationV1.Operator_ISTARTS_WITH,
	"i_starts_with": paginationV1.Operator_ISTARTS_WITH,
	"istartswith":   paginationV1.Operator_ISTARTS_WITH,

	"ends_with": paginationV1.Operator_ENDS_WITH,
	"endswith":  paginationV1.Operator_ENDS_WITH,

	"iends_with":  paginationV1.Operator_IENDS_WITH,
	"i_ends_with": paginationV1.Operator_IENDS_WITH,
	"iendswith":   paginationV1.Operator_IENDS_WITH,

	"json_contains":  paginationV1.Operator_JSON_CONTAINS,
	"array_contains": paginationV1.Operator_ARRAY_CONTAINS,
	"exists":         paginationV1.Operator_EXISTS,
	"search":         paginationV1.Operator_SEARCH,
	"exact":          paginationV1.Operator_EXACT,

	"iexact":  paginationV1.Operator_IEXACT,
	"i_exact": paginationV1.Operator_IEXACT,
}

// ConverterStringToOperator 将字符串转换为 paginationV1.Operator 枚举值
func ConverterStringToOperator(str string) paginationV1.Operator {
	key := strings.ToLower(stringcase.ToSnakeCase(str))
	if v, ok := operatorMap[key]; ok {
		return v
	}
	return paginationV1.Operator_OPERATOR_UNSPECIFIED
}

// IsValidOperatorString 检查字符串是否为有效的 paginationV1.Operator 枚举值
func IsValidOperatorString(str string) bool {
	op := ConverterStringToOperator(str)
	return op != paginationV1.Operator_OPERATOR_UNSPECIFIED
}

var datePartMap = map[string]paginationV1.DatePart{
	"date": paginationV1.DatePart_DATE,

	"year": paginationV1.DatePart_YEAR,
	"yr":   paginationV1.DatePart_YEAR,

	"iso_year": paginationV1.DatePart_ISO_YEAR,
	"iso-year": paginationV1.DatePart_ISO_YEAR,

	"quarter": paginationV1.DatePart_QUARTER,
	"month":   paginationV1.DatePart_MONTH,
	"week":    paginationV1.DatePart_WEEK,

	"week_day": paginationV1.DatePart_WEEK_DAY,
	"week-day": paginationV1.DatePart_WEEK_DAY,
	"weekday":  paginationV1.DatePart_WEEK_DAY,

	"iso_week_day": paginationV1.DatePart_ISO_WEEK_DAY,
	"iso-week-day": paginationV1.DatePart_ISO_WEEK_DAY,

	"day":  paginationV1.DatePart_DAY,
	"time": paginationV1.DatePart_TIME,
	"hour": paginationV1.DatePart_HOUR,

	"minute": paginationV1.DatePart_MINUTE,
	"min":    paginationV1.DatePart_MINUTE,

	"second": paginationV1.DatePart_SECOND,
	"sec":    paginationV1.DatePart_SECOND,

	"microsecond": paginationV1.DatePart_MICROSECOND,
}

// ConverterStringToDatePart 将字符串转换为 paginationV1.DatePart 枚举
func ConverterStringToDatePart(s string) *paginationV1.DatePart {
	key := strings.ToLower(stringcase.ToSnakeCase(s))
	if v, ok := datePartMap[key]; ok {
		return &v
	}
	return nil
}

// IsValidDatePartString 检查字符串是否为有效的 paginationV1.DatePart 枚举值
func IsValidDatePartString(str string) bool {
	dp := ConverterStringToDatePart(str)
	return dp != nil
}
