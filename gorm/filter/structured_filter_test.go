package filter

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

func mustMarshal(fe *paginationV1.FilterExpr) string {
	b, _ := protojson.MarshalOptions{Multiline: false, EmitUnpopulated: false}.Marshal(fe)
	return string(b)
}

func TestFilterExprExamples_Marshal(t *testing.T) {
	fe := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
		Conditions: []*paginationV1.FilterCondition{
			{Field: "A", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "1"}},
			{Field: "B", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "2"}},
		},
	}
	js := mustMarshal(fe)
	if js == "" {
		t.Fatal("protojson marshal returned empty string")
	}
}

func TestNewStructuredFilter_Basics(t *testing.T) {
	sf := NewStructuredFilter()
	if sf == nil {
		t.Fatal("NewStructuredFilter returned nil")
	}
	if sf.processor == nil {
		t.Fatal("expected processor to be initialized")
	}
}

func TestBuildFilterSelectors_NilAndUnspecified(t *testing.T) {
	sf := NewStructuredFilter()

	// nil expr -> empty slice
	sels, err := sf.BuildSelectors(nil)
	if err != nil {
		t.Fatalf("unexpected error for nil expr: %v", err)
	}
	if sels == nil {
		t.Log("BuildSelectors(nil) returned nil (acceptable)")
	} else if len(sels) != 0 {
		t.Fatalf("expected 0 selectors for nil expr, got %d", len(sels))
	}

	// unspecified -> nil, nil per implementation
	expr := &paginationV1.FilterExpr{Type: paginationV1.ExprType_EXPR_TYPE_UNSPECIFIED}
	sels2, err := sf.BuildSelectors(expr)
	if err != nil {
		t.Fatalf("unexpected error for unspecified expr: %v", err)
	}
	if sels2 != nil {
		t.Fatalf("expected nil selectors for unspecified expr, got %v", sels2)
	}
}

func TestBuildFilterSelectors_SimpleAnd(t *testing.T) {
	sf := NewStructuredFilter()
	expr := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
		Conditions: []*paginationV1.FilterCondition{
			{Field: "A", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "1"}},
		},
	}
	sels, err := sf.BuildSelectors(expr)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sels) != 1 {
		t.Fatalf("expected 1 selector for simple AND expr, got %d", len(sels))
	}
	if sels[0] == nil {
		t.Fatal("expected non-nil selector function")
	}
}

func Test_buildFilterSelector_NilAndUnspecified(t *testing.T) {
	sf := NewStructuredFilter()

	sel, err := sf.buildFilterSelector(nil)
	if err != nil {
		t.Fatalf("unexpected error for nil expr: %v", err)
	}
	if sel != nil {
		t.Fatal("expected nil selector for nil expr, got non-nil")
	}

	expr := &paginationV1.FilterExpr{Type: paginationV1.ExprType_EXPR_TYPE_UNSPECIFIED}
	sel2, err := sf.buildFilterSelector(expr)
	if err != nil {
		t.Fatalf("unexpected error for unspecified expr: %v", err)
	}
	if sel2 != nil {
		t.Fatal("expected nil selector for unspecified expr, got non-nil")
	}
}

func TestStructuredFilter_SupportedOperators_CreateSelectors(t *testing.T) {
	sf := NewStructuredFilter()

	// 仅测试实现中明确支持的操作集合
	ops := []struct {
		name   string
		op     paginationV1.Operator
		value  string
		values []string
	}{
		{"EQ", paginationV1.Operator_EQ, "v1", nil},
		{"NEQ", paginationV1.Operator_NEQ, "v1", nil},
		{"GT", paginationV1.Operator_GT, "10", nil},
		{"GTE", paginationV1.Operator_GTE, "10", nil},
		{"LT", paginationV1.Operator_LT, "10", nil},
		{"LTE", paginationV1.Operator_LTE, "10", nil},
		{"IN", paginationV1.Operator_IN, `["a","b"]`, nil},
		{"NIN", paginationV1.Operator_NIN, `["a","b"]`, nil},
		{"BETWEEN", paginationV1.Operator_BETWEEN, `["1","5"]`, nil},
		{"IS_NULL", paginationV1.Operator_IS_NULL, "", nil},
		{"IS_NOT_NULL", paginationV1.Operator_IS_NOT_NULL, "", nil},
		{"CONTAINS", paginationV1.Operator_CONTAINS, "sub", nil},
		{"ICONTAINS", paginationV1.Operator_ICONTAINS, "sub", nil},
		{"STARTS_WITH", paginationV1.Operator_STARTS_WITH, "pre", nil},
		{"ISTARTS_WITH", paginationV1.Operator_ISTARTS_WITH, "pre", nil},
		{"ENDS_WITH", paginationV1.Operator_ENDS_WITH, "suf", nil},
		{"IENDS_WITH", paginationV1.Operator_IENDS_WITH, "suf", nil},
		{"EXACT", paginationV1.Operator_EXACT, "exact", nil},
		{"IEXACT", paginationV1.Operator_IEXACT, "iexact", nil},
		{"REGEXP", paginationV1.Operator_REGEXP, `^a`, nil},
		{"IREGEXP", paginationV1.Operator_IREGEXP, `(?i)^a`, nil},
		{"SEARCH", paginationV1.Operator_SEARCH, "q", nil},
	}

	for _, tc := range ops {
		t.Run(tc.name, func(t *testing.T) {
			cond := &paginationV1.FilterCondition{
				Field:      "test_field",
				Op:         tc.op,
				ValueOneof: &paginationV1.FilterCondition_Value{Value: tc.value},
				Values:     tc.values,
			}
			expr := &paginationV1.FilterExpr{
				Type:       paginationV1.ExprType_AND,
				Conditions: []*paginationV1.FilterCondition{cond},
			}
			sels, err := sf.BuildSelectors(expr)
			if err != nil {
				t.Fatalf("operator %s: unexpected error: %v", tc.name, err)
			}
			if sels == nil {
				t.Fatalf("operator %s: expected selectors slice, got nil", tc.name)
			}
			if len(sels) != 1 {
				t.Fatalf("operator %s: expected 1 selector, got %d", tc.name, len(sels))
			}
			if sels[0] == nil {
				t.Fatalf("operator %s: expected non-nil selector function", tc.name)
			}
		})
	}
}

func TestStructuredFilter_JSONField_SQL(t *testing.T) {
	sf := NewStructuredFilter()
	db := openTestDB(t)

	// JSON 字段条件： preferences.daily_email = "true"
	cond := &paginationV1.FilterCondition{
		Field:      "preferences.daily_email",
		Op:         paginationV1.Operator_EQ,
		ValueOneof: &paginationV1.FilterCondition_Value{Value: "true"},
	}
	expr := &paginationV1.FilterExpr{
		Type:       paginationV1.ExprType_AND,
		Conditions: []*paginationV1.FilterCondition{cond},
	}

	sels, err := sf.BuildSelectors(expr)
	if err != nil {
		t.Fatalf("BuildSelectors error: %v", err)
	}
	if len(sels) != 1 {
		t.Fatalf("expected 1 selector, got %d", len(sels))
	}
	sql := sqlFor(t, db, sels[0])
	lsql := strings.ToLower(sql)
	if sql == "" {
		t.Fatalf("expected non-empty sql for jsonb condition")
	}
	if !strings.Contains(lsql, "preferences") {
		t.Fatalf("expected sql to reference preferences, got: %q", sql)
	}
	// json key may appear as literal or as JSON_EXTRACT etc., check key presence
	if !strings.Contains(lsql, "daily_email") && !strings.Contains(lsql, "->>") && !strings.Contains(lsql, "json_extract") {
		t.Fatalf("expected json key or json extract operator in sql, got: %q", sql)
	}
}
