package filter

import (
	"strings"
	"testing"

	"entgo.io/ent/dialect/sql"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

func buildFilterSQL(t *testing.T, table string, field string) (string, []any) {
	t.Helper()
	expr := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
		Conditions: []*paginationV1.FilterCondition{
			{Field: field, Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "1"}},
		},
	}
	sels, err := NewStructuredFilter().BuildSelectors(expr)
	if err != nil {
		t.Fatalf("BuildSelectors error: %v", err)
	}
	s := sql.Select("*").From(sql.Table(table))
	for _, sel := range sels {
		sel(s)
	}
	q, args := s.Query()
	return q, args
}

// TestFilter_HostileFieldNeutralizedOnKnownTable 验证在已注册白名单的表上，
// 携带 SQL 元字符的过滤字段不会进入 WHERE（硬性校验 + 列白名单双重拦截）。
func TestFilter_HostileFieldNeutralizedOnKnownTable(t *testing.T) {
	hostile := []string{
		"id) OR (1=1 --",
		`id" = "x`,
		"id; DROP TABLE users",
		"id AS (select version())",
		"name = 'a' OR 1=1",
		"id\xffd' OR '1'='1", // 非法 UTF-8：ToSnakeCase 归一化对它原样透传
	}
	for _, field := range hostile {
		q, _ := buildFilterSQL(t, "users", field)
		for _, d := range []string{"(", ")", "'", `"`, ";", "--", " AS ", "version()"} {
			if strings.Contains(q, d) {
				t.Errorf("hostile field %q leaked %q into SQL: %q", field, d, q)
			}
		}
	}
}

// TestFilter_HostileFieldNeutralizedOnUnknownTable 验证在未注册白名单的表上
// （列白名单 fail-open 场景），硬性标识符校验仍然拦截注入载荷。
func TestFilter_HostileFieldNeutralizedOnUnknownTable(t *testing.T) {
	for _, field := range []string{"id) OR (1=1 --", "(select version())", "a\xffb' OR '1'='1"} {
		q, _ := buildFilterSQL(t, "custom_table", field)
		for _, d := range []string{"(", "'", "--", "version()", ";"} {
			if strings.Contains(q, d) {
				t.Errorf("hostile field %q leaked %q into SQL on unknown table: %q", field, d, q)
			}
		}
		if strings.Contains(q, "WHERE") {
			t.Errorf("hostile field %q must be dropped entirely, got WHERE: %q", field, q)
		}
	}
}

// TestFilter_ValidFieldStillWorks 验证已注册表的合法列过滤不受影响。
func TestFilter_ValidFieldStillWorks(t *testing.T) {
	q, args := buildFilterSQL(t, "users", "name")
	if !strings.Contains(q, "WHERE") {
		t.Fatalf("expected WHERE clause for valid field, got %q", q)
	}
	if !strings.Contains(q, "name") {
		t.Fatalf("expected column name in SQL, got %q", q)
	}
	if len(args) == 0 || args[0] != "1" {
		t.Fatalf("expected bound value 1, got %v", args)
	}
}
