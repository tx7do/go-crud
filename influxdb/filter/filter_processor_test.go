package filter

import (
	"testing"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/influxdb/query"
)

func TestProcessor_Process_ReturnsBuilder_NoPanic(t *testing.T) {
	proc := NewProcessor()

	ops := []paginationV1.Operator{
		paginationV1.Operator_EQ,
		paginationV1.Operator_NEQ,
		paginationV1.Operator_IN,
		paginationV1.Operator_NIN,
		paginationV1.Operator_GTE,
		paginationV1.Operator_GT,
		paginationV1.Operator_LTE,
		paginationV1.Operator_LT,
		paginationV1.Operator_BETWEEN,
		paginationV1.Operator_IS_NULL,
		paginationV1.Operator_IS_NOT_NULL,
		paginationV1.Operator_CONTAINS,
		paginationV1.Operator_ICONTAINS,
		paginationV1.Operator_STARTS_WITH,
		paginationV1.Operator_ISTARTS_WITH,
		paginationV1.Operator_ENDS_WITH,
		paginationV1.Operator_IENDS_WITH,
		paginationV1.Operator_EXACT,
		paginationV1.Operator_IEXACT,
		paginationV1.Operator_REGEXP,
		paginationV1.Operator_IREGEXP,
		paginationV1.Operator_SEARCH,
	}

	for _, op := range ops {
		qb := query.NewQueryBuilder("m")
		got := proc.Process(qb, op, "name", "val", []string{"a", "b"})
		if got == nil {
			t.Fatalf("Process returned nil for op %v", op)
		}
		if got != qb {
			t.Fatalf("Process should return the same builder pointer for op %v", op)
		}
	}
}

func TestProcessor_SpecificCases_ReturnsBuilder(t *testing.T) {
	proc := NewProcessor()

	t.Run("Equal_ReturnsBuilder", func(t *testing.T) {
		qb := query.NewQueryBuilder("m")
		got := proc.Equal(qb, "name", "tom")
		if got == nil || got != qb {
			t.Fatalf("Equal should return the same non-nil builder")
		}
	})

	t.Run("In_JSONArray_ReturnsBuilder", func(t *testing.T) {
		qb := query.NewQueryBuilder("m")
		got := proc.In(qb, "name", `["a","b"]`, nil)
		if got == nil || got != qb {
			t.Fatalf("In should return the same non-nil builder")
		}
	})

	t.Run("NotIn_WithValues_ReturnsBuilder", func(t *testing.T) {
		qb := query.NewQueryBuilder("m")
		got := proc.NotIn(qb, "status", "", []string{"x", "y"})
		if got == nil || got != qb {
			t.Fatalf("NotIn should return the same non-nil builder")
		}
	})

	t.Run("Range_Between_ReturnsBuilder", func(t *testing.T) {
		qb := query.NewQueryBuilder("m")
		got := proc.Range(qb, "created_at", `["2020-01-01","2021-01-01"]`, nil)
		if got == nil || got != qb {
			t.Fatalf("Range should return the same non-nil builder")
		}
	})

	t.Run("IsNull_ReturnsBuilder", func(t *testing.T) {
		qb := query.NewQueryBuilder("m")
		// Processor 没有单独的 IsNull 方法，使用 Process + Operator_IS_NULL
		got := proc.Process(qb, paginationV1.Operator_IS_NULL, "deleted_at", "", nil)
		if got == nil || got != qb {
			t.Fatalf("Process(IS_NULL) should return the same non-nil builder")
		}
	})

	t.Run("Contains_ReturnsBuilder", func(t *testing.T) {
		qb := query.NewQueryBuilder("m")
		got := proc.Contains(qb, "title", "go")
		if got == nil || got != qb {
			t.Fatalf("Contains should return the same non-nil builder")
		}
	})

	t.Run("JsonField_DotPath_ReturnsBuilder", func(t *testing.T) {
		qb := query.NewQueryBuilder("m")
		got := proc.Process(qb, paginationV1.Operator_EQ, "preferences.daily_email", "1", nil)
		if got == nil || got != qb {
			t.Fatalf("Processing JSON dot path should return the same non-nil builder")
		}
	})
}
