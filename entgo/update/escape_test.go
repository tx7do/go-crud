package update

import "testing"

// TestEscapeSQLLiteral 验证单引号与反斜杠转义（堵 jsonb_build_object 值注入）。
// 反斜杠转义与 elasticsearch/utils.go escapeQueryValue 对齐（D-2）。
func TestEscapeSQLLiteral(t *testing.T) {
	cases := map[string]string{
		"normal":      "normal",
		"with'quote":  "with''quote",
		"''; DROP--":  "''''; DROP--",
		"":            "",
		`a\b`:         `a\\b`,
		`'; DROP --\`: `''; DROP --\\`,
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

// TestValidateIdentifier 验证标识符白名单：合法列名通过，含元字符的列名
// 被拒（D-1：堵 ent Ident 对含 " 的标识符原样 WriteString 的绕过路径）。
func TestValidateIdentifier(t *testing.T) {
	accept := []string{"col", "user_id", "_t", "a1", "field2_name"}
	for _, s := range accept {
		if !validateIdentifier(s) {
			t.Errorf("validateIdentifier(%q) must accept", s)
		}
	}
	reject := []string{`a"b`, `"; DROP`, "a;b", "a b", "a'b", "", "1abc", "a-b", "a.b"}
	for _, s := range reject {
		if validateIdentifier(s) {
			t.Errorf("validateIdentifier(%q) must reject", s)
		}
	}
}

// TestSetJson_NullRejectsBadFieldName 验证 SetJson* 对非白名单列名返回 nil
// 修饰器（D-1：不在 u.Set 第一参数位置喂入敌意列名）。
func TestSetJson_NullRejectsBadFieldName(t *testing.T) {
	if f := SetJsonNullFieldUpdateBuilder(`a"; DROP TABLE t; --`, nil, nil); f != nil {
		t.Errorf("bad fieldName must return nil modifier")
	}
	if f := SetJsonFieldValueUpdateBuilder(`a"; DROP TABLE t; --`, nil, nil, false); f != nil {
		t.Errorf("bad fieldName must return nil modifier")
	}
	// 合法列名在无 nilPaths/keyValues 时也应返回 nil
	if f := SetJsonNullFieldUpdateBuilder("col", nil, nil); f != nil {
		t.Errorf("empty nilPaths must return nil modifier")
	}
}
