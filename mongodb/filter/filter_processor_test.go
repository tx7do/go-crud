package filter

import (
	"testing"

	pagination "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/mongodb/query"
)

func TestProcessor_Process_ReturnsBuilder_NoPanic(t *testing.T) {
	proc := NewProcessor()

	ops := []pagination.Operator{
		pagination.Operator_EQ,
		pagination.Operator_NEQ,
		pagination.Operator_IN,
		pagination.Operator_NIN,
		pagination.Operator_GTE,
		pagination.Operator_GT,
		pagination.Operator_LTE,
		pagination.Operator_LT,
		pagination.Operator_BETWEEN,
		pagination.Operator_IS_NULL,
		pagination.Operator_IS_NOT_NULL,
		pagination.Operator_CONTAINS,
		pagination.Operator_ICONTAINS,
		pagination.Operator_STARTS_WITH,
		pagination.Operator_ISTARTS_WITH,
		pagination.Operator_ENDS_WITH,
		pagination.Operator_IENDS_WITH,
		pagination.Operator_EXACT,
		pagination.Operator_IEXACT,
		pagination.Operator_REGEXP,
		pagination.Operator_IREGEXP,
		pagination.Operator_SEARCH,
	}

	for _, op := range ops {
		qb := &query.Builder{}
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
		qb := &query.Builder{}
		got := proc.Equal(qb, "name", "tom")
		if got == nil || got != qb {
			t.Fatalf("Equal should return the same non-nil builder")
		}
	})

	t.Run("In_JSONArray_ReturnsBuilder", func(t *testing.T) {
		qb := &query.Builder{}
		got := proc.In(qb, "name", `["a","b"]`, nil)
		if got == nil || got != qb {
			t.Fatalf("In should return the same non-nil builder")
		}
	})

	t.Run("NotIn_WithValues_ReturnsBuilder", func(t *testing.T) {
		qb := &query.Builder{}
		got := proc.NotIn(qb, "status", "", []string{"x", "y"})
		if got == nil || got != qb {
			t.Fatalf("NotIn should return the same non-nil builder")
		}
	})

	t.Run("Range_Between_ReturnsBuilder", func(t *testing.T) {
		qb := &query.Builder{}
		got := proc.Range(qb, "created_at", `["2020-01-01","2021-01-01"]`, nil)
		if got == nil || got != qb {
			t.Fatalf("Range should return the same non-nil builder")
		}
	})

	t.Run("IsNull_ReturnsBuilder", func(t *testing.T) {
		qb := &query.Builder{}
		got := proc.IsNull(qb, "deleted_at")
		if got == nil || got != qb {
			t.Fatalf("IsNull should return the same non-nil builder")
		}
	})

	t.Run("Contains_ReturnsBuilder", func(t *testing.T) {
		qb := &query.Builder{}
		got := proc.Contains(qb, "title", "go")
		if got == nil || got != qb {
			t.Fatalf("Contains should return the same non-nil builder")
		}
	})

	t.Run("JsonField_DotPath_ReturnsBuilder", func(t *testing.T) {
		qb := &query.Builder{}
		got := proc.Process(qb, pagination.Operator_EQ, "preferences.daily_email", "1", nil)
		if got == nil || got != qb {
			t.Fatalf("Processing JSON dot path should return the same non-nil builder")
		}
	})
}
