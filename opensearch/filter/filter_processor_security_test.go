package filter

import (
	"testing"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

func buildAndQuery(p *Processor, field string, op paginationV1.Operator, val string) map[string]any {
	expr := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
		Conditions: []*paginationV1.FilterCondition{
			{Field: field, Op: op, ValueOneof: &paginationV1.FilterCondition_Value{Value: val}},
		},
	}
	return p.BuildOpenSearchQuery(expr)
}

// TestProcessor_SearchValueQuoted 验证 SEARCH 操作符的值被引号字面量化，
// OR/AND/字段名/通配符无法改变 query_string 查询结构（DSL 注入）。
func TestProcessor_SearchValueQuoted(t *testing.T) {
	p := NewProcessor()
	m := buildAndQuery(p, "name", paginationV1.Operator_SEARCH, "* OR admin_field:*")
	inner := extractMust(t, m)
	q, ok := inner["query_string"].(map[string]any)
	if !ok {
		t.Fatalf("expected query_string, got %#v", inner)
	}
	qs, _ := q["query"].(string)
	if qs != `"* OR admin_field:*"` {
		t.Errorf("SEARCH value must be quoted literal, got %q", qs)
	}
}

// TestProcessor_EndsWithWildcardEscaped 验证 ENDS_WITH 的值内通配符被转义，
// 仅保留前缀 * 的"以…结尾"语义。
func TestProcessor_EndsWithWildcardEscaped(t *testing.T) {
	p := NewProcessor()
	m := buildAndQuery(p, "name", paginationV1.Operator_ENDS_WITH, "x*?y")
	inner := extractMust(t, m)
	w, ok := inner["wildcard"].(map[string]any)
	if !ok {
		t.Fatalf("expected wildcard, got %#v", inner)
	}
	pat, _ := w["name"].(string)
	if pat != `*x\*\?y` {
		t.Errorf("wildcard pattern must escape inner * ?, got %q", pat)
	}
}

// TestProcessor_EscapeHelpers escapeQueryValue/escapeWildcard 单测
func TestProcessor_EscapeHelpers(t *testing.T) {
	if got := escapeQueryValue(`a"b\c`); got != `"a\"b\\c"` {
		t.Errorf("escapeQueryValue: got %q", got)
	}
	if got := escapeWildcard(`a*b?c`); got != `a\*b\?c` {
		t.Errorf("escapeWildcard: got %q", got)
	}
}

// extractMust 提取 AND 组输出的 bool.must[0]（AND 组被包在 bool 查询里）
func extractMust(t *testing.T, m map[string]any) map[string]any {
	t.Helper()
	b, ok := m["bool"].(map[string]any)
	if !ok {
		t.Fatalf("expected bool wrapper, got %#v", m)
	}
	must, ok := b["must"].([]map[string]any)
	if !ok || len(must) == 0 {
		t.Fatalf("expected non-empty must, got %#v", b)
	}
	return must[0]
}
