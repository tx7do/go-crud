package filter

import (
	"testing"

	"github.com/tx7do/go-crud/mongodb/query"
	"github.com/tx7do/go-utils/trans"
	"google.golang.org/protobuf/encoding/protojson"

	pagination "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

func mustMarshal(fe *pagination.FilterExpr) string {
	b, _ := protojson.MarshalOptions{Multiline: false, EmitUnpopulated: false}.Marshal(fe)
	return string(b)
}

func TestFilterExprExamples_Marshal(t *testing.T) {
	fe := &pagination.FilterExpr{
		Type: pagination.ExprType_AND,
		Conditions: []*pagination.Condition{
			{Field: "A", Op: pagination.Operator_EQ, Value: trans.Ptr("1")},
			{Field: "B", Op: pagination.Operator_EQ, Value: trans.Ptr("2")},
		},
	}
	js := mustMarshal(fe)
	if js == "" {
		t.Fatal("protojson marshal returned empty string")
	}
}

func TestBuildFilterSelectors_NilAndUnspecified(t *testing.T) {
	sf := NewStructuredFilter()

	// nil expr -> no error, return builder (may be newly created or provided)
	b := &query.Builder{}
	got, err := sf.BuildSelectors(b, nil)
	if err != nil {
		t.Fatalf("unexpected error for nil expr: %v", err)
	}
	if got == nil {
		t.Fatalf("expected non-nil builder for nil expr")
	}
	if got != b {
		t.Fatalf("expected same builder pointer returned for nil expr")
	}

	// unspecified -> should not produce error and should return provided builder
	expr := &pagination.FilterExpr{Type: pagination.ExprType_EXPR_TYPE_UNSPECIFIED}
	builder2 := &query.Builder{}
	got2, err := sf.BuildSelectors(builder2, expr)
	if err != nil {
		t.Fatalf("unexpected error for unspecified expr: %v", err)
	}
	if got2 == nil {
		t.Fatalf("expected non-nil builder for unspecified expr")
	}
	if got2 != builder2 {
		t.Fatalf("expected same builder pointer returned for unspecified expr")
	}
}

func TestBuildFilterSelectors_SimpleAnd(t *testing.T) {
	sf := NewStructuredFilter()
	builder := &query.Builder{}

	expr := &pagination.FilterExpr{
		Type: pagination.ExprType_AND,
		Conditions: []*pagination.Condition{
			{Field: "Name", Op: pagination.Operator_EQ, Value: trans.Ptr("alice")},
			{Field: "Age", Op: pagination.Operator_GT, Value: trans.Ptr("18")},
		},
	}

	b, err := sf.BuildSelectors(builder, expr)
	if err != nil {
		t.Fatalf("BuildSelectors error: %v", err)
	}
	if b == nil {
		t.Fatal("expected non-nil builder")
	}
	if b != builder {
		t.Fatalf("expected same builder pointer returned")
	}
}

func TestStructuredFilter_SupportedOperators_CreateSelectors(t *testing.T) {
	sf := NewStructuredFilter()

	ops := []struct {
		name   string
		op     pagination.Operator
		value  string
		values []string
	}{
		{"EQ", pagination.Operator_EQ, "v1", nil},
		{"NEQ", pagination.Operator_NEQ, "v1", nil},
		{"GT", pagination.Operator_GT, "10", nil},
		{"GTE", pagination.Operator_GTE, "10", nil},
		{"LT", pagination.Operator_LT, "10", nil},
		{"LTE", pagination.Operator_LTE, "10", nil},
		{"IN", pagination.Operator_IN, `["a","b"]`, nil},
		{"NIN", pagination.Operator_NIN, `["a","b"]`, nil},
		{"BETWEEN", pagination.Operator_BETWEEN, `["1","5"]`, nil},
		{"IS_NULL", pagination.Operator_IS_NULL, "", nil},
		{"IS_NOT_NULL", pagination.Operator_IS_NOT_NULL, "", nil},
		{"CONTAINS", pagination.Operator_CONTAINS, "sub", nil},
		{"ICONTAINS", pagination.Operator_ICONTAINS, "sub", nil},
		{"STARTS_WITH", pagination.Operator_STARTS_WITH, "pre", nil},
		{"ISTARTS_WITH", pagination.Operator_ISTARTS_WITH, "pre", nil},
		{"ENDS_WITH", pagination.Operator_ENDS_WITH, "suf", nil},
		{"IENDS_WITH", pagination.Operator_IENDS_WITH, "suf", nil},
		{"EXACT", pagination.Operator_EXACT, "exact", nil},
		{"IEXACT", pagination.Operator_IEXACT, "iexact", nil},
		{"REGEXP", pagination.Operator_REGEXP, `^a`, nil},
		{"IREGEXP", pagination.Operator_IREGEXP, `(?i)^a`, nil},
		{"SEARCH", pagination.Operator_SEARCH, "q", nil},
	}

	for _, tc := range ops {
		t.Run(tc.name, func(t *testing.T) {
			builder := &query.Builder{}
			cond := &pagination.Condition{
				Field:  "test_field",
				Op:     tc.op,
				Value:  trans.Ptr(tc.value),
				Values: tc.values,
			}
			expr := &pagination.FilterExpr{
				Type:       pagination.ExprType_AND,
				Conditions: []*pagination.Condition{cond},
			}
			b, err := sf.BuildSelectors(builder, expr)
			if err != nil {
				t.Fatalf("operator %s: unexpected error: %v", tc.name, err)
			}
			if b == nil {
				t.Fatalf("operator %s: expected builder, got nil", tc.name)
			}
			if b != builder {
				t.Fatalf("operator %s: expected same builder pointer returned", tc.name)
			}
		})
	}
}

func TestBuildSelectors_OrWithInAndContains(t *testing.T) {
	sf := NewStructuredFilter()
	builder := &query.Builder{}

	expr := &pagination.FilterExpr{
		Type: pagination.ExprType_OR,
		Conditions: []*pagination.Condition{
			{Field: "status", Op: pagination.Operator_IN, Values: []string{"active", "pending"}},
			{Field: "title", Op: pagination.Operator_CONTAINS, Value: trans.Ptr("Go")},
		},
	}

	b, err := sf.BuildSelectors(builder, expr)
	if err != nil {
		t.Fatalf("BuildSelectors error: %v", err)
	}
	if b == nil {
		t.Fatal("expected non-nil builder")
	}
	if b != builder {
		t.Fatalf("expected same builder pointer returned")
	}
}

func TestBuildSelectors_JSONField(t *testing.T) {
	sf := NewStructuredFilter()
	builder := &query.Builder{}

	cond := &pagination.Condition{
		Field: "preferences.daily_email",
		Op:    pagination.Operator_EQ,
		Value: trans.Ptr("true"),
	}
	expr := &pagination.FilterExpr{
		Type:       pagination.ExprType_AND,
		Conditions: []*pagination.Condition{cond},
	}

	b, err := sf.BuildSelectors(builder, expr)
	if err != nil {
		t.Fatalf("BuildSelectors error: %v", err)
	}
	if b == nil {
		t.Fatal("expected non-nil builder")
	}
	if b != builder {
		t.Fatalf("expected same builder pointer returned")
	}
}
