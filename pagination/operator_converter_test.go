package pagination

import (
	"testing"

	pagination "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-utils/trans"
)

func TestConverterStringToOperator(t *testing.T) {
	cases := map[string]pagination.Operator{
		"eq":             pagination.Operator_EQ,
		"EQ":             pagination.Operator_EQ,
		"equal":          pagination.Operator_EQ,
		"equals":         pagination.Operator_EQ,
		"ne":             pagination.Operator_NEQ,
		"not-equal":      pagination.Operator_NEQ,
		"not_equal":      pagination.Operator_NEQ,
		"gt":             pagination.Operator_GT,
		"greater-than":   pagination.Operator_GT,
		"gte":            pagination.Operator_GTE,
		"less_than":      pagination.Operator_LT,
		"like":           pagination.Operator_LIKE,
		"iLike":          pagination.Operator_ILIKE,
		"i_like":         pagination.Operator_ILIKE,
		"in":             pagination.Operator_IN,
		"notin":          pagination.Operator_NIN,
		"isNotNull":      pagination.Operator_IS_NOT_NULL,
		"isnull":         pagination.Operator_IS_NULL,
		"between":        pagination.Operator_BETWEEN,
		"regexp":         pagination.Operator_REGEXP,
		"iregex":         pagination.Operator_IREGEXP,
		"contains":       pagination.Operator_CONTAINS,
		"icontains":      pagination.Operator_ICONTAINS,
		"startsWith":     pagination.Operator_STARTS_WITH,
		"ends_with":      pagination.Operator_ENDS_WITH,
		"json_contains":  pagination.Operator_JSON_CONTAINS,
		"array_contains": pagination.Operator_ARRAY_CONTAINS,
		"exists":         pagination.Operator_EXISTS,
		"search":         pagination.Operator_SEARCH,
		"exact":          pagination.Operator_EXACT,
		"iexact":         pagination.Operator_IEXACT,

		// unknown / empty -> unspecified
		"":       pagination.Operator_OPERATOR_UNSPECIFIED,
		"foobar": pagination.Operator_OPERATOR_UNSPECIFIED,
	}

	for input, want := range cases {
		t.Run(input, func(t *testing.T) {
			got := ConverterStringToOperator(input)
			if got != want {
				t.Fatalf("ConverterStringToOperator(%q) = %v, want %v", input, got, want)
			}
		})
	}
}

func TestIsValidOperatorString(t *testing.T) {
	valid := []string{"eq", "not_equal", "i_like", "search"}
	for _, s := range valid {
		if !IsValidOperatorString(s) {
			t.Fatalf("IsValidOperatorString(%q) = false, want true", s)
		}
	}

	invalid := []string{"", "unknown_op", "blah"}
	for _, s := range invalid {
		if IsValidOperatorString(s) {
			t.Fatalf("IsValidOperatorString(%q) = true, want false", s)
		}
	}
}

func TestConverterStringToDatePart(t *testing.T) {
	cases := map[string]*pagination.DatePart{
		"date":         trans.Ptr(pagination.DatePart_DATE),
		"Date":         trans.Ptr(pagination.DatePart_DATE),
		"DATE":         trans.Ptr(pagination.DatePart_DATE),
		"year":         trans.Ptr(pagination.DatePart_YEAR),
		"yr":           trans.Ptr(pagination.DatePart_YEAR),
		"iso_year":     trans.Ptr(pagination.DatePart_ISO_YEAR),
		"iso-year":     trans.Ptr(pagination.DatePart_ISO_YEAR),
		"quarter":      trans.Ptr(pagination.DatePart_QUARTER),
		"month":        trans.Ptr(pagination.DatePart_MONTH),
		"week":         trans.Ptr(pagination.DatePart_WEEK),
		"week_day":     trans.Ptr(pagination.DatePart_WEEK_DAY),
		"week-day":     trans.Ptr(pagination.DatePart_WEEK_DAY),
		"weekday":      trans.Ptr(pagination.DatePart_WEEK_DAY),
		"iso_week_day": trans.Ptr(pagination.DatePart_ISO_WEEK_DAY),
		"iso-week-day": trans.Ptr(pagination.DatePart_ISO_WEEK_DAY),
		"day":          trans.Ptr(pagination.DatePart_DAY),
		"time":         trans.Ptr(pagination.DatePart_TIME),
		"hour":         trans.Ptr(pagination.DatePart_HOUR),
		"minute":       trans.Ptr(pagination.DatePart_MINUTE),
		"min":          trans.Ptr(pagination.DatePart_MINUTE),
		"second":       trans.Ptr(pagination.DatePart_SECOND),
		"sec":          trans.Ptr(pagination.DatePart_SECOND),
		"microsecond":  trans.Ptr(pagination.DatePart_MICROSECOND),

		// unknown / empty -> unspecified
		"":       nil,
		"foobar": nil,
	}

	for input, want := range cases {
		t.Run(input, func(t *testing.T) {
			got := ConverterStringToDatePart(input)
			if want == nil && got != nil {
				t.Fatalf("ConverterStringToDatePart(%q) = %v, want %v", input, got, want)
			} else if want != nil && *got != *want {
				t.Fatalf("ConverterStringToDatePart(%q) = %v, want %v", input, got, want)
			}
		})
	}
}

func TestIsValidDatePartString(t *testing.T) {
	valid := []string{"date", "year", "iso_year", "minute", "microsecond"}
	for _, s := range valid {
		if !IsValidDatePartString(s) {
			t.Fatalf("IsValidDatePartString(%q) = false, want true", s)
		}
	}

	invalid := []string{"", "not_a_part", "blah"}
	for _, s := range invalid {
		if IsValidDatePartString(s) {
			t.Fatalf("IsValidDatePartString(%q) = true, want false", s)
		}
	}
}
