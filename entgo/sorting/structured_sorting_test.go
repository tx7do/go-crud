package sorting

import (
	"strings"
	"testing"

	"entgo.io/ent/dialect/sql"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

func TestStructuredSorting_BuildSelector_Empty(t *testing.T) {
	ss := NewStructuredSorting()

	selFunc, err := ss.BuildSelector(nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if selFunc != nil {
		t.Fatal("expected nil selector for empty orders")
	}
}

func TestStructuredSorting_BuildSelector_Orderings(t *testing.T) {
	ss := NewStructuredSorting()

	orders := []*paginationV1.Sorting{
		{Field: "name", Direction: paginationV1.Sorting_ASC},
		{Field: "age", Direction: paginationV1.Sorting_DESC},
		nil,
		{Field: "", Direction: paginationV1.Sorting_ASC},
		{Field: "created_at", Direction: paginationV1.Sorting_ASC},
	}

	selFunc, err := ss.BuildSelector(orders)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if selFunc == nil {
		t.Fatal("expected non-nil selector function")
	}

	s := sql.Select("t.*").From(sql.Table("t"))
	selFunc(s)
	sqlStr, _ := s.Query()

	if !strings.Contains(strings.ToUpper(sqlStr), "ORDER BY") {
		t.Fatalf("expected ORDER BY in SQL, got: %s", sqlStr)
	}
	if !strings.Contains(sqlStr, "name") {
		t.Fatalf("expected ordering by name, got: %s", sqlStr)
	}
	if !strings.Contains(strings.ToUpper(sqlStr), "AGE") || !strings.Contains(strings.ToUpper(sqlStr), "DESC") {
		t.Fatalf("expected ordering by age DESC, got: %s", sqlStr)
	}
	if !strings.Contains(sqlStr, "created_at") {
		t.Fatalf("expected ordering by created_at, got: %s", sqlStr)
	}
}

func TestStructuredSorting_BuildSelectorWithDefaultField(t *testing.T) {
	ss := NewStructuredSorting()

	// 无 orders 且提供 defaultOrderField 时，应返回默认排序
	selFunc, err := ss.BuildSelectorWithDefaultField(nil, "created_at", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if selFunc == nil {
		t.Fatal("expected non-nil selector when default field provided")
	}
	s := sql.Select("t.*").From(sql.Table("t"))
	selFunc(s)
	sqlStr, _ := s.Query()
	if !strings.Contains(strings.ToUpper(sqlStr), "ORDER BY") || !strings.Contains(strings.ToUpper(sqlStr), "CREATED_AT") || !strings.Contains(strings.ToUpper(sqlStr), "DESC") {
		t.Fatalf("expected ORDER BY created_at DESC, got: %s", sqlStr)
	}

	// 提供 orders 时，应优先使用 orders 而非默认字段
	selFunc2, err := ss.BuildSelectorWithDefaultField([]*paginationV1.Sorting{{Field: "score", Direction: paginationV1.Sorting_DESC}}, "created_at", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if selFunc2 == nil {
		t.Fatal("expected non-nil selector when orders provided")
	}
	s2 := sql.Select("t.*").From(sql.Table("t"))
	selFunc2(s2)
	sqlStr2, _ := s2.Query()
	if strings.Contains(strings.ToUpper(sqlStr2), "CREATED_AT") {
		t.Fatalf("did not expect default field to be used when orders provided, got: %s", sqlStr2)
	}
	if !strings.Contains(strings.ToUpper(sqlStr2), "SCORE") || !strings.Contains(strings.ToUpper(sqlStr2), "DESC") {
		t.Fatalf("expected ORDER BY score DESC, got: %s", sqlStr2)
	}
}
