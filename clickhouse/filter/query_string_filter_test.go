package filter

import (
	"strings"
	"testing"

	"github.com/tx7do/go-crud/clickhouse/query"
)

func TestBuildSelectors_BasicANDOR(t *testing.T) {
	sf := NewQueryStringFilter()
	builder := query.NewQueryBuilder("", nil)

	andJson := `{"name":"tom","title__contains":"Go"}`
	orJson := `{"status__in": "[\"active\",\"pending\"]", "title__contains":"Go"}`

	b, err := sf.BuildSelectors(builder, andJson, orJson)
	if err != nil {
		t.Fatalf("BuildSelectors error: %v", err)
	}
	if b == nil {
		t.Fatalf("expected builder, got nil")
	}

	sql, _ := b.Build()
	lower := strings.ToLower(sql)

	if !strings.Contains(lower, "where") {
		t.Fatalf("expected WHERE clause, got: %q", lower)
	}
	if !strings.Contains(lower, "name") || !strings.Contains(lower, "=") {
		t.Fatalf("unexpected sql for EQ: %q", lower)
	}
	if !strings.Contains(lower, "title") || !strings.Contains(lower, "like") {
		t.Fatalf("unexpected sql for CONTAINS: %q", lower)
	}
	// OR 部分应包含 status 和 in
	if !strings.Contains(lower, "status") || !strings.Contains(lower, "in") {
		t.Fatalf("unexpected sql for OR selector: %q", lower)
	}
}

func TestBuildSelectors_InBetweenAndJsonb(t *testing.T) {
	sf := NewQueryStringFilter()

	// IN 操作
	builder1 := query.NewQueryBuilder("", nil)
	inJson := `{"name__in":"a,b"}`
	b1, err := sf.BuildSelectors(builder1, inJson, "")
	if err != nil {
		t.Fatalf("BuildSelectors error: %v", err)
	}
	sql1, args1 := b1.Build()
	l1 := strings.ToLower(sql1)
	if !strings.Contains(l1, " in ") && !strings.Contains(l1, " in(") {
		t.Fatalf("unexpected sql for IN: %q", l1)
	}
	if len(args1) == 0 {
		t.Fatalf("expected args for IN, got none")
	}

	// BETWEEN 操作
	builder2 := query.NewQueryBuilder("", nil)
	betweenJson := `{"created_at__between":"2020-01-01,2021-01-01"}`
	b2, err := sf.BuildSelectors(builder2, betweenJson, "")
	if err != nil {
		t.Fatalf("BuildSelectors error: %v", err)
	}
	sql2, args2 := b2.Build()
	l2 := strings.ToLower(sql2)
	if !strings.Contains(l2, "between") {
		t.Fatalf("unexpected sql for BETWEEN: %q", l2)
	}
	if len(args2) != 2 {
		t.Fatalf("expected 2 args for BETWEEN, got %d", len(args2))
	}

	// JSON 字段 equality
	builder3 := query.NewQueryBuilder("", nil)
	jsonb := `{"preferences.daily_email":"true"}`
	b3, err := sf.BuildSelectors(builder3, jsonb, "")
	if err != nil {
		t.Fatalf("BuildSelectors error: %v", err)
	}
	sql3, _ := b3.Build()
	l3 := strings.ToLower(sql3)
	if !(strings.Contains(l3, "preferences") && strings.Contains(l3, "daily_email")) {
		t.Fatalf("unexpected sql for JSON field: %q", l3)
	}
}

func TestBuildSelectors_UnsupportedDatePartAndOperators(t *testing.T) {
	sf := NewQueryStringFilter()

	// 不支持的 date-part 模式，不应产生条件
	builder := query.NewQueryBuilder("", nil)
	dateJson := `{"created_at__year__gt":"2020"}`
	b, err := sf.BuildSelectors(builder, dateJson, "")
	if err != nil {
		t.Fatalf("BuildSelectors error: %v", err)
	}
	sql, _ := b.Build()
	lower := strings.ToLower(sql)
	if strings.Contains(lower, "created_at") || strings.Contains(lower, "year") {
		t.Fatalf("expected no where for unsupported date-part pattern, got: %q", lower)
	}

	// combined eq / neq
	builder2 := query.NewQueryBuilder("", nil)
	eqJson := `{"status":"active","name__neq":"bob"}`
	b2, err := sf.BuildSelectors(builder2, eqJson, "")
	if err != nil {
		t.Fatalf("BuildSelectors error: %v", err)
	}
	sql2, _ := b2.Build()
	l2 := strings.ToLower(sql2)
	if !strings.Contains(l2, "status") || !strings.Contains(l2, "=") {
		t.Fatalf("unexpected sql for EQ in combined: %q", l2)
	}
	if !strings.Contains(l2, "name") || !strings.Contains(l2, "!=") {
		t.Fatalf("unexpected sql for NEQ in combined: %q", l2)
	}
}

func TestBuildSelectors_EmptyAndInvalid(t *testing.T) {
	sf := NewQueryStringFilter()

	// 空字符串不会报错且不添加条件
	builder := query.NewQueryBuilder("", nil)
	b, err := sf.BuildSelectors(builder, "", "")
	if err != nil {
		t.Fatalf("expected nil error for empty, got %v", err)
	}
	sql, _ := b.Build()
	if strings.Contains(strings.ToLower(sql), "where") {
		t.Fatalf("expected no WHERE for empty input, got: %q", sql)
	}

	// 无效 JSON 返回错误
	_, err = sf.BuildSelectors(query.NewQueryBuilder("", nil), "not a json", "")
	if err == nil {
		t.Fatalf("expected error for invalid json")
	}
}
