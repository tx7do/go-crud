package sorting

import (
	"strings"
	"testing"

	pagination "github.com/tx7do/go-curd/api/gen/go/pagination/v1"
)

func TestStructuredSorting_BuildScope_Empty(t *testing.T) {
	ss := NewStructuredSorting()

	scope := ss.BuildScope(nil)
	sql := sqlOfScope(t, scope)
	if strings.Contains(strings.ToUpper(sql), "ORDER BY") {
		t.Fatalf("did not expect ORDER BY for empty orders, got SQL: %s", sql)
	}
}

func TestStructuredSorting_BuildScope_Orderings(t *testing.T) {
	ss := NewStructuredSorting()

	orders := []*pagination.Sorting{
		{Field: "name", Order: pagination.Sorting_ASC},
		{Field: "age", Order: pagination.Sorting_DESC},
		nil,
		{Field: "", Order: pagination.Sorting_ASC},
		{Field: "created_at", Order: pagination.Sorting_ASC},
	}

	scope := ss.BuildScope(orders)
	sql := sqlOfScope(t, scope)
	up := strings.ToUpper(sql)

	if !strings.Contains(up, "ORDER BY") {
		t.Fatalf("expected ORDER BY in SQL, got: %s", sql)
	}
	if !strings.Contains(up, "NAME") {
		t.Fatalf("expected ordering by name, got: %s", sql)
	}
	if !strings.Contains(up, "AGE") || !strings.Contains(up, "DESC") {
		t.Fatalf("expected ordering by age DESC, got: %s", sql)
	}
	if !strings.Contains(up, "CREATED_AT") {
		t.Fatalf("expected ordering by created_at, got: %s", sql)
	}
}

func TestStructuredSorting_BuildScopeWithDefaultField(t *testing.T) {
	ss := NewStructuredSorting()

	// 当 orders 为空时应使用默认字段和方向
	scope := ss.BuildScopeWithDefaultField(nil, "created_at", true)
	sql := sqlOfScope(t, scope)
	up := strings.ToUpper(sql)
	if !strings.Contains(up, "ORDER BY") || !strings.Contains(up, "CREATED_AT") || !strings.Contains(up, "DESC") {
		t.Fatalf("expected ORDER BY created_at DESC, got: %s", sql)
	}

	// 提供 orders 时应优先使用 orders 而非默认字段
	scope2 := ss.BuildScopeWithDefaultField([]*pagination.Sorting{{Field: "score", Order: pagination.Sorting_DESC}}, "created_at", true)
	sql2 := sqlOfScope(t, scope2)
	up2 := strings.ToUpper(sql2)
	if strings.Contains(up2, "CREATED_AT") {
		t.Fatalf("did not expect default field to be used when orders provided, got: %s", sql2)
	}
	if !strings.Contains(up2, "SCORE") || !strings.Contains(up2, "DESC") {
		t.Fatalf("expected ORDER BY score DESC, got: %s", sql2)
	}
}
