package sorting

import (
	"strings"
	"testing"

	"github.com/tx7do/go-crud/clickhouse/query"
)

func TestParseOrderCH_BasicCases(t *testing.T) {
	cases := []struct {
		expr    string
		wantCol string
		wantDir string
		wantOk  bool
	}{
		{"-field", "field", "DESC", true},
		{"field", "field", "ASC", true},
		{"field:desc", "field", "DESC", true},
		{"field.desc", "field", "DESC", true},
		{"preferences.daily_email", "preferences.daily_email", "ASC", true},
		{"name;drop", "", "", false},
		{"", "", "", false},
	}

	for _, c := range cases {
		col, dir, ok := parseOrderCH(c.expr)
		if ok != c.wantOk {
			t.Fatalf("expr=%q: expected ok=%v, got %v", c.expr, c.wantOk, ok)
		}
		if !ok {
			continue
		}
		if col != c.wantCol || dir != c.wantDir {
			t.Fatalf("expr=%q: expected (%q, %q), got (%q, %q)", c.expr, c.wantCol, c.wantDir, col, dir)
		}
	}
}

func TestBuildOrderClause(t *testing.T) {
	qss := NewQueryStringSorting()

	t.Run("empty orders -> no ORDER BY", func(t *testing.T) {
		qb := query.NewQueryBuilder("test_table", nil)
		got := qss.BuildOrderClause(qb, nil)
		sql, _ := got.Build()
		up := strings.ToUpper(sql)
		if strings.Contains(up, "ORDER BY") {
			t.Fatalf("did not expect ORDER BY for nil orders, got: %s", sql)
		}
	})

	t.Run("various orders -> ORDER BY contains expected fragments", func(t *testing.T) {
		orders := []string{
			"-field",                  // DESC
			"field",                   // ASC
			"field:desc",              // DESC
			"preferences.daily_email", // JSONExtractString(...)
			"name;drop",               // invalid -> ignored
		}
		qb := query.NewQueryBuilder("test_table", nil)
		got := qss.BuildOrderClause(qb, orders)
		sql, _ := got.Build()
		up := strings.ToUpper(sql)

		if !strings.Contains(up, "ORDER BY") {
			t.Fatalf("expected ORDER BY in result, got: %s", sql)
		}

		// basic field checks (FIELD should appear from "field" / "-field")
		if !strings.Contains(up, "FIELD") {
			t.Fatalf("expected ordering by field, got: %s", sql)
		}

		// expect at least one DESC (from -field or field:desc)
		if !strings.Contains(up, "DESC") {
			t.Fatalf("expected DESC present for some fields, got: %s", sql)
		}

		// JSONExtractString presence and json key
		if !strings.Contains(up, "JSONEXTRACTSTRING") || !strings.Contains(up, "DAILY_EMAIL") {
			t.Fatalf("expected JSONExtractString(..., 'daily_email') in ORDER BY, got: %s", sql)
		}

		// invalid token should be ignored (ensure no DROP)
		if strings.Contains(up, "DROP") {
			t.Fatalf("did not expect dangerous token in SQL, got: %s", sql)
		}
	})
}
