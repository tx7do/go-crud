package update

import "testing"

// TestEscapeSQLLiteral 验证单引号转义（堵 jsonb_build_object 值注入）。
func TestEscapeSQLLiteral(t *testing.T) {
	cases := map[string]string{
		"normal":     "normal",
		"with'quote": "with''quote",
		"''; DROP--": "''''; DROP--",
		"":           "",
	}
	for in, want := range cases {
		if got := escapeSQLLiteral(in); got != want {
			t.Errorf("escapeSQLLiteral(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestEscapeSQLIdentifier 验证双引号标识符转义（堵 JSON 列名注入）。
func TestEscapeSQLIdentifier(t *testing.T) {
	cases := map[string]string{
		"col":                 "col",
		`a"b`:                 `a""b`,
		`"; DROP TABLE t; --`: `""; DROP TABLE t; --`,
	}
	for in, want := range cases {
		if got := escapeSQLIdentifier(in); got != want {
			t.Errorf("escapeSQLIdentifier(%q) = %q, want %q", in, got, want)
		}
	}
}
