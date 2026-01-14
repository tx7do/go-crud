package pagination

import (
	"testing"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-utils/trans"
)

func TestConverterStringToOperator(t *testing.T) {
	cases := map[string]paginationV1.Operator{
		"eq":             paginationV1.Operator_EQ,
		"EQ":             paginationV1.Operator_EQ,
		"equal":          paginationV1.Operator_EQ,
		"equals":         paginationV1.Operator_EQ,
		"ne":             paginationV1.Operator_NEQ,
		"not-equal":      paginationV1.Operator_NEQ,
		"not_equal":      paginationV1.Operator_NEQ,
		"gt":             paginationV1.Operator_GT,
		"greater-than":   paginationV1.Operator_GT,
		"gte":            paginationV1.Operator_GTE,
		"less_than":      paginationV1.Operator_LT,
		"like":           paginationV1.Operator_LIKE,
		"iLike":          paginationV1.Operator_ILIKE,
		"i_like":         paginationV1.Operator_ILIKE,
		"in":             paginationV1.Operator_IN,
		"notin":          paginationV1.Operator_NIN,
		"isNotNull":      paginationV1.Operator_IS_NOT_NULL,
		"isnull":         paginationV1.Operator_IS_NULL,
		"between":        paginationV1.Operator_BETWEEN,
		"regexp":         paginationV1.Operator_REGEXP,
		"iregex":         paginationV1.Operator_IREGEXP,
		"contains":       paginationV1.Operator_CONTAINS,
		"icontains":      paginationV1.Operator_ICONTAINS,
		"startsWith":     paginationV1.Operator_STARTS_WITH,
		"ends_with":      paginationV1.Operator_ENDS_WITH,
		"json_contains":  paginationV1.Operator_JSON_CONTAINS,
		"array_contains": paginationV1.Operator_ARRAY_CONTAINS,
		"exists":         paginationV1.Operator_EXISTS,
		"search":         paginationV1.Operator_SEARCH,
		"exact":          paginationV1.Operator_EXACT,
		"iexact":         paginationV1.Operator_IEXACT,

		// unknown / empty -> unspecified
		"":       paginationV1.Operator_OPERATOR_UNSPECIFIED,
		"foobar": paginationV1.Operator_OPERATOR_UNSPECIFIED,
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
	cases := map[string]*paginationV1.DatePart{
		"date":         trans.Ptr(paginationV1.DatePart_DATE),
		"Date":         trans.Ptr(paginationV1.DatePart_DATE),
		"DATE":         trans.Ptr(paginationV1.DatePart_DATE),
		"year":         trans.Ptr(paginationV1.DatePart_YEAR),
		"yr":           trans.Ptr(paginationV1.DatePart_YEAR),
		"iso_year":     trans.Ptr(paginationV1.DatePart_ISO_YEAR),
		"iso-year":     trans.Ptr(paginationV1.DatePart_ISO_YEAR),
		"quarter":      trans.Ptr(paginationV1.DatePart_QUARTER),
		"month":        trans.Ptr(paginationV1.DatePart_MONTH),
		"week":         trans.Ptr(paginationV1.DatePart_WEEK),
		"week_day":     trans.Ptr(paginationV1.DatePart_WEEK_DAY),
		"week-day":     trans.Ptr(paginationV1.DatePart_WEEK_DAY),
		"weekday":      trans.Ptr(paginationV1.DatePart_WEEK_DAY),
		"iso_week_day": trans.Ptr(paginationV1.DatePart_ISO_WEEK_DAY),
		"iso-week-day": trans.Ptr(paginationV1.DatePart_ISO_WEEK_DAY),
		"day":          trans.Ptr(paginationV1.DatePart_DAY),
		"time":         trans.Ptr(paginationV1.DatePart_TIME),
		"hour":         trans.Ptr(paginationV1.DatePart_HOUR),
		"minute":       trans.Ptr(paginationV1.DatePart_MINUTE),
		"min":          trans.Ptr(paginationV1.DatePart_MINUTE),
		"second":       trans.Ptr(paginationV1.DatePart_SECOND),
		"sec":          trans.Ptr(paginationV1.DatePart_SECOND),
		"microsecond":  trans.Ptr(paginationV1.DatePart_MICROSECOND),

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
