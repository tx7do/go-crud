package filter

import (
	"strings"
	"testing"

	pagination "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/clickhouse/query"
)

func TestProcessor_BuilderSQLFragments(t *testing.T) {
	t.Run("Equal_IncludesWhereAndArg", func(t *testing.T) {
		qb := query.NewQueryBuilder("users", nil)
		proc := NewProcessor()
		proc.Process(qb, pagination.Operator_EQ, "name", "tom", nil)
		sql, args := qb.Build()
		up := strings.ToUpper(sql)
		if !strings.Contains(up, "WHERE") {
			t.Fatalf("expected WHERE in SQL, got: %s", sql)
		}
		if !strings.Contains(up, "NAME = ?") {
			t.Fatalf("expected NAME = ? in SQL, got: %s", sql)
		}
		if len(args) != 1 || args[0] != "tom" {
			t.Fatalf("unexpected args: %+v", args)
		}
	})

	t.Run("In_JSONArray_AddsPlaceholdersAndArgs", func(t *testing.T) {
		qb := query.NewQueryBuilder("users", nil)
		proc := NewProcessor()
		proc.Process(qb, pagination.Operator_IN, "name", `["a","b"]`, nil)
		sql, args := qb.Build()
		up := strings.ToUpper(sql)
		if !strings.Contains(up, "IN (") {
			t.Fatalf("expected IN clause, got: %s", sql)
		}
		if len(args) != 2 {
			t.Fatalf("expected 2 args, got: %d (%+v)", len(args), args)
		}
	})

	t.Run("NotIn_WithValues_AddsNotIn", func(t *testing.T) {
		qb := query.NewQueryBuilder("users", nil)
		proc := NewProcessor()
		proc.Process(qb, pagination.Operator_NIN, "status", "", []string{"x", "y"})
		sql, args := qb.Build()
		up := strings.ToUpper(sql)
		if !strings.Contains(up, "NOT IN (") {
			t.Fatalf("expected NOT IN clause, got: %s", sql)
		}
		if len(args) != 2 {
			t.Fatalf("expected 2 args, got: %d", len(args))
		}
	})

	t.Run("Range_Between_AddsBetweenAndArgs", func(t *testing.T) {
		qb := query.NewQueryBuilder("users", nil)
		proc := NewProcessor()
		proc.Process(qb, pagination.Operator_BETWEEN, "created_at", `["2020-01-01","2021-01-01"]`, nil)
		sql, args := qb.Build()
		up := strings.ToUpper(sql)
		if !strings.Contains(up, "BETWEEN") {
			t.Fatalf("expected BETWEEN in SQL, got: %s", sql)
		}
		if len(args) != 2 {
			t.Fatalf("expected 2 args, got: %d", len(args))
		}
	})

	t.Run("IsNull_AddsIsNull", func(t *testing.T) {
		qb := query.NewQueryBuilder("users", nil)
		proc := NewProcessor()
		proc.Process(qb, pagination.Operator_IS_NULL, "deleted_at", "", nil)
		sql, _ := qb.Build()
		if !strings.Contains(strings.ToUpper(sql), "IS NULL") {
			t.Fatalf("expected IS NULL, got: %s", sql)
		}
	})

	t.Run("Contains_LikeAndArg", func(t *testing.T) {
		qb := query.NewQueryBuilder("users", nil)
		proc := NewProcessor()
		proc.Process(qb, pagination.Operator_CONTAINS, "title", "go", nil)
		sql, args := qb.Build()
		if !strings.Contains(strings.ToUpper(sql), "LIKE") {
			t.Fatalf("expected LIKE, got: %s", sql)
		}
		if len(args) != 1 {
			t.Fatalf("expected 1 arg, got: %d", len(args))
		}
		if !strings.Contains(args[0].(string), "%go%") {
			t.Fatalf("expected wildcard arg contains %%go%%, got: %v", args[0])
		}
	})

	t.Run("JsonField_UsesJSONExtractString", func(t *testing.T) {
		qb := query.NewQueryBuilder("users", nil)
		proc := NewProcessor()
		proc.Process(qb, pagination.Operator_EQ, "preferences.daily_email", "1", nil)
		sql, _ := qb.Build()
		up := strings.ToUpper(sql)
		if !strings.Contains(up, "JSONEXTRACTSTRING") || !strings.Contains(up, "DAILY_EMAIL") {
			t.Fatalf("expected JSONExtractString with key daily_email, got: %s", sql)
		}
	})
}

func TestProcessor_ProcessDispatcher_NoPanicAndReturnsBuilder(t *testing.T) {
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
		qb := query.NewQueryBuilder("users", nil)
		proc := NewProcessor()
		got := proc.Process(qb, op, "name", "val", []string{"a", "b"})
		if got == nil {
			t.Fatalf("Process returned nil for op %v", op)
		}
	}
}
