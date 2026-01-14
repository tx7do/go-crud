package filter

import (
	"testing"

	"github.com/tx7do/go-crud/influxdb/query"
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

func TestBuildFilterSelectors_NilAndUnspecified(t *testing.T) {
	sf := NewStructuredFilter()

	// nil expr -> no error, return builder (may be newly created or provided)
	b := query.NewQueryBuilder("m")
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
	expr := &paginationV1.FilterExpr{Type: paginationV1.ExprType_EXPR_TYPE_UNSPECIFIED}
	builder2 := query.NewQueryBuilder("m")
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
	builder := query.NewQueryBuilder("m")

	expr := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
		Conditions: []*paginationV1.FilterCondition{
			{Field: "Name", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "alice"}},
			{Field: "Age", Op: paginationV1.Operator_GT, ValueOneof: &paginationV1.FilterCondition_Value{Value: "18"}},
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
			builder := query.NewQueryBuilder("m")
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
	builder := query.NewQueryBuilder("m")

	expr := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_OR,
		Conditions: []*paginationV1.FilterCondition{
			{Field: "status", Op: paginationV1.Operator_IN, Values: []string{"active", "pending"}},
			{Field: "title", Op: paginationV1.Operator_CONTAINS, ValueOneof: &paginationV1.FilterCondition_Value{Value: "Go"}},
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
	builder := query.NewQueryBuilder("m")

	cond := &paginationV1.FilterCondition{
		Field:      "preferences.daily_email",
		Op:         paginationV1.Operator_EQ,
		ValueOneof: &paginationV1.FilterCondition_Value{Value: "true"},
	}
	expr := &paginationV1.FilterExpr{
		Type:       paginationV1.ExprType_AND,
		Conditions: []*paginationV1.FilterCondition{cond},
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
