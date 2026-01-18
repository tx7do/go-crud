package sorting

import (
	"strings"
	"testing"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/influxdb/query"
)

func extractOrderClause(q string) string {
	idx := strings.Index(q, "ORDER BY ")
	if idx == -1 {
		return ""
	}
	s := q[idx+len("ORDER BY "):]
	// 截断到可能的后续子句（LIMIT/GROUP BY/WHERE/OFFSET）
	for _, delim := range []string{" LIMIT", " GROUP BY", " WHERE", " OFFSET"} {
		if p := strings.Index(s, delim); p != -1 {
			s = s[:p]
		}
	}
	return strings.TrimSpace(s)
}

func TestStructuredSorting_BuildOrderClause_NoOrders_NoSort(t *testing.T) {
	ss := NewStructuredSorting()
	qb := query.NewQueryBuilder("m")

	gotBuilder := ss.BuildOrderClause(qb, nil)
	if gotBuilder == nil {
		t.Fatalf("expected builder returned, got nil")
	}
	out := gotBuilder.Build()
	if strings.Contains(out, "ORDER BY") {
		t.Fatalf("did not expect ORDER BY for nil orders, got: %s", out)
	}
}

func TestStructuredSorting_BuildOrderClause_Orderings(t *testing.T) {
	ss := NewStructuredSorting()
	qb := query.NewQueryBuilder("m")

	orders := []*paginationV1.Sorting{
		{Field: "name", Direction: paginationV1.Sorting_ASC},
		{Field: "age", Direction: paginationV1.Sorting_DESC},
		nil,
		{Field: "", Direction: paginationV1.Sorting_ASC},
		{Field: "UserProfile.name", Direction: paginationV1.Sorting_ASC}, // first segment -> snake_case
		{Field: "created_at", Direction: paginationV1.Sorting_ASC},
	}

	gotBuilder := ss.BuildOrderClause(qb, orders)
	if gotBuilder == nil {
		t.Fatalf("expected builder returned, got nil")
	}
	out := gotBuilder.Build()
	clause := extractOrderClause(out)
	if clause == "" {
		t.Fatalf("expected ORDER BY clause applied, got: %s", out)
	}

	// 期望的排序片段（按顺序出现）
	expected := []string{
		"name",              // ASC may be emitted without "ASC"
		"age DESC",          // DESC explicit
		"user_profile.name", // snake_case first segment preserved dot path
		"created_at",        // as-is
	}

	cur := 0
	for _, tok := range expected {
		pos := strings.Index(clause[cur:], tok)
		if pos == -1 {
			t.Fatalf("expected token %q in ORDER BY clause, got clause: %q", tok, clause)
		}
		cur += pos + len(tok)
	}

	// 确保没有危险字符或注入字样
	if strings.Contains(clause, ";") || strings.Contains(strings.ToUpper(clause), "DROP") {
		t.Fatalf("unexpected dangerous content in ORDER BY clause: %q", clause)
	}
}

func TestStructuredSorting_BuildOrderClauseWithDefaultField(t *testing.T) {
	ss := NewStructuredSorting()

	// 未提供 orders -> 应使用默认字段和方向（默认 DESC）
	qb1 := query.NewQueryBuilder("m")
	gotBuilder := ss.BuildOrderClauseWithDefaultField(qb1, nil, "created_at", true)
	if gotBuilder == nil {
		t.Fatalf("expected builder returned, got nil")
	}
	out := gotBuilder.Build()
	clause := extractOrderClause(out)
	if clause == "" {
		t.Fatalf("expected ORDER BY clause for default field, got: %s", out)
	}
	if !strings.Contains(clause, "created_at") || !strings.Contains(clause, "DESC") {
		t.Fatalf("expected ORDER BY created_at DESC, got clause: %q", clause)
	}

	// 提供 orders 时应优先使用 orders 而非默认字段
	qb2 := query.NewQueryBuilder("m")
	provided := []*paginationV1.Sorting{{Field: "score", Direction: paginationV1.Sorting_DESC}}
	gotBuilder2 := ss.BuildOrderClauseWithDefaultField(qb2, provided, "created_at", true)
	if gotBuilder2 == nil {
		t.Fatalf("expected builder returned, got nil")
	}
	out2 := gotBuilder2.Build()
	clause2 := extractOrderClause(out2)
	if clause2 == "" {
		t.Fatalf("expected ORDER BY clause applied, got: %s", out2)
	}
	if !strings.Contains(clause2, "score") || !strings.Contains(clause2, "DESC") {
		t.Fatalf("expected ORDER BY score DESC, got clause: %q", clause2)
	}
}
