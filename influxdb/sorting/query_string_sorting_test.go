package sorting

import (
	"strings"
	"testing"

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

func TestParseOrder_BasicCases(t *testing.T) {
	cases := []struct {
		expr     string
		wantCol  string
		wantDesc bool
		wantOk   bool
	}{
		{"-field", "field", true, true},
		{"field", "field", false, true},
		{"field:desc", "field", true, true},
		{"field.desc", "field", true, true},
		{"preferences.daily_email", "preferences.daily_email", false, true},
		{"name;drop", "", false, false},
		{"", "", false, false},
	}

	for _, c := range cases {
		col, desc, ok := parseOrder(c.expr)
		if ok != c.wantOk {
			t.Fatalf("expr=%q: expected ok=%v, got %v", c.expr, c.wantOk, ok)
		}
		if !ok {
			continue
		}
		if col != c.wantCol || desc != c.wantDesc {
			t.Fatalf("expr=%q: expected (%q, %v), got (%q, %v)", c.expr, c.wantCol, c.wantDesc, col, desc)
		}
	}
}

func TestBuildOrderClause_EmptyOrders(t *testing.T) {
	qss := NewQueryStringSorting()
	qb := query.NewQueryBuilder("m")
	got := qss.BuildOrderClause(qb, nil)
	if got == nil {
		t.Fatalf("expected builder returned, got nil")
	}
	out := got.Build()
	if strings.Contains(out, "ORDER BY") {
		t.Fatalf("expected no ORDER BY for nil orders, got: %s", out)
	}
}

func TestBuildOrderClause_VariousOrders(t *testing.T) {
	qss := NewQueryStringSorting()

	orders := []string{
		"-field",     // DESC
		"field",      // ASC
		"field:desc", // DESC
		"name;drop",  // invalid -> ignored
	}

	// 构建期望项（使用 parseOrder 过滤无效项）
	type item struct {
		col  string
		desc bool
	}
	expected := make([]item, 0, len(orders))
	for _, o := range orders {
		c, d, ok := parseOrder(o)
		if ok {
			expected = append(expected, item{c, d})
		}
	}

	qb := query.NewQueryBuilder("m")
	got := qss.BuildOrderClause(qb, orders)
	if got == nil {
		t.Fatalf("expected builder returned, got nil")
	}
	out := got.Build()
	clause := extractOrderClause(out)
	if clause == "" {
		t.Fatalf("expected ORDER BY clause applied, got: %s", out)
	}

	// 解析实际 ORDER BY 子句为项列表并归一化
	parts := strings.Split(clause, ",")
	if len(parts) != len(expected) {
		t.Fatalf("expected %d order items, got %d: %q", len(expected), len(parts), clause)
	}

	for i, raw := range parts {
		p := strings.TrimSpace(raw)
		gotDesc := false

		up := strings.ToUpper(p)
		if strings.HasSuffix(up, " DESC") {
			gotDesc = true
			p = strings.TrimSpace(p[:len(p)-5])
		} else if strings.HasSuffix(up, " ASC") {
			gotDesc = false
			p = strings.TrimSpace(p[:len(p)-4])
		}

		// 去掉可能的表别名和引号（如 m.field 或 "field"）
		if strings.HasPrefix(p, "m.") {
			p = strings.TrimPrefix(p, "m.")
		}
		p = strings.Trim(p, `"`)

		exp := expected[i]
		if p != exp.col || gotDesc != exp.desc {
			t.Fatalf("order item %d: expected %q desc=%v, got %q desc=%v, clause=%q", i, exp.col, exp.desc, p, gotDesc, clause)
		}
	}

	// 确保没有危险字符或 SQL 注入字样
	if strings.Contains(clause, ";") || strings.Contains(strings.ToUpper(clause), "DROP") {
		t.Fatalf("unexpected dangerous content in ORDER BY clause: %q", clause)
	}
}
