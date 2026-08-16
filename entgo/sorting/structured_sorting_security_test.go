package sorting

import (
	"strings"
	"testing"

	"entgo.io/ent/dialect/sql"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

func buildSortingSQL(t *testing.T, table, field string) string {
	t.Helper()
	sel, err := NewStructuredSorting().BuildSelector([]*paginationV1.Sorting{
		{Field: field, Direction: paginationV1.Sorting_DESC},
	})
	if err != nil {
		t.Fatalf("BuildSelector error: %v", err)
	}
	s := sql.Select("*").From(sql.Table(table))
	sel(s)
	q, _ := s.Query()
	return q
}

// TestSorting_HostileFieldDropped 验证含 SQL 元字符的排序字段被硬性校验拒绝，
// 不会进入 ORDER BY（列白名单对未注册表 fail-open，故此校验必须独立生效）。
func TestSorting_HostileFieldDropped(t *testing.T) {
	for _, field := range []string{
		"id) OR (1=1 --",
		"(select version())",
		`name" --`,
		"id\xfff' OR '1'='1",
	} {
		for _, table := range []string{"users", "custom_table"} {
			q := buildSortingSQL(t, table, field)
			for _, d := range []string{"(", "'", `"`, "--", "version()", ";"} {
				if strings.Contains(q, d) {
					t.Errorf("hostile sort field %q leaked %q into SQL on %s: %q", field, d, table, q)
				}
			}
			if strings.Contains(q, "ORDER BY") {
				t.Errorf("hostile sort field %q must be dropped on %s, got ORDER BY: %q", field, table, q)
			}
		}
	}
}

// TestSorting_ValidFieldStillWorks 验证已注册表的合法列排序不受影响。
func TestSorting_ValidFieldStillWorks(t *testing.T) {
	q := buildSortingSQL(t, "users", "name")
	if !strings.Contains(q, "ORDER BY") {
		t.Fatalf("expected ORDER BY for valid field, got %q", q)
	}
	if !strings.Contains(q, "name") {
		t.Fatalf("expected column name in ORDER BY, got %q", q)
	}
}

// TestSorting_DefaultFieldValidated 验证默认排序字段同样过硬性校验。
func TestSorting_DefaultFieldValidated(t *testing.T) {
	sel, err := NewStructuredSorting().BuildSelectorWithDefaultField(nil, "(select version())", true)
	if err != nil {
		t.Fatalf("BuildSelectorWithDefaultField error: %v", err)
	}
	s := sql.Select("*").From(sql.Table("custom_table"))
	sel(s)
	q, _ := s.Query()
	if strings.Contains(q, "ORDER BY") || strings.Contains(q, "version()") {
		t.Errorf("hostile default sort field must be dropped, got %q", q)
	}
}
