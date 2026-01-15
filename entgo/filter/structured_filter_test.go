package filter

import (
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

func mustMarshal(fe *paginationV1.FilterExpr) string {
	b, _ := protojson.MarshalOptions{Multiline: false, EmitUnpopulated: false}.Marshal(fe)
	return string(b)
}

func TestFilterExprExamples(t *testing.T) {
	t.Run("SimpleAND", func(t *testing.T) {
		// SQL: WHERE A = '1' AND B = '2'
		fe := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{
				{Field: "A", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "1"}},
				{Field: "B", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "2"}},
			},
		}

		if fe.GetType() != paginationV1.ExprType_AND {
			t.Fatalf("expected AND, got %v", fe.GetType())
		}
		if len(fe.GetConditions()) != 2 {
			t.Fatalf("expected 2 conditions, got %d", len(fe.GetConditions()))
		}
		// ensure json marshal works and contains type name
		js := mustMarshal(fe)
		if js == "" {
			t.Fatal("protojson marshal returned empty string")
		}
	})

	t.Run("SimpleOR", func(t *testing.T) {
		// SQL: WHERE A = '1' OR B = '2'
		fe := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_OR,
			Conditions: []*paginationV1.FilterCondition{
				{Field: "A", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "1"}},
				{Field: "B", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "2"}},
			},
		}

		if fe.GetType() != paginationV1.ExprType_OR {
			t.Fatalf("expected OR, got %v", fe.GetType())
		}
		if len(fe.GetConditions()) != 2 {
			t.Fatalf("expected 2 conditions, got %d", len(fe.GetConditions()))
		}
	})

	t.Run("Mixed_A_AND_BorC", func(t *testing.T) {
		// Logical: A AND (B OR C)
		// SQL: WHERE A = '1' AND (B = '2' OR C = '3')
		orGroup := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_OR,
			Conditions: []*paginationV1.FilterCondition{
				{Field: "B", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "2"}},
				{Field: "C", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "3"}},
			},
		}
		fe := &paginationV1.FilterExpr{
			Type:       paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{{Field: "A", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "1"}}},
			Groups:     []*paginationV1.FilterExpr{orGroup},
		}

		if fe.GetType() != paginationV1.ExprType_AND {
			t.Fatalf("expected top-level AND, got %v", fe.GetType())
		}
		if len(fe.GetConditions()) != 1 {
			t.Fatalf("expected 1 top-level condition, got %d", len(fe.GetConditions()))
		}
		if len(fe.GetGroups()) != 1 {
			t.Fatalf("expected 1 group, got %d", len(fe.GetGroups()))
		}
		if fe.GetGroups()[0].GetType() != paginationV1.ExprType_OR {
			t.Fatalf("expected inner group OR, got %v", fe.GetGroups()[0].GetType())
		}
	})

	t.Run("ComplexNested", func(t *testing.T) {
		// Logical: (A OR B) AND (C OR (D AND E))
		// SQL: WHERE (A = 'a' OR B = 'b') AND (C = 'c' OR (D = 'd' AND E = 'e'))
		left := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_OR,
			Conditions: []*paginationV1.FilterCondition{
				{Field: "A", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "a"}},
				{Field: "B", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "b"}},
			},
		}
		rightInner := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{
				{Field: "D", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "d"}},
				{Field: "E", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "e"}},
			},
		}
		right := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_OR,
			Conditions: []*paginationV1.FilterCondition{
				{Field: "C", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "c"}},
			},
			Groups: []*paginationV1.FilterExpr{rightInner},
		}
		fe := &paginationV1.FilterExpr{
			Type:   paginationV1.ExprType_AND,
			Groups: []*paginationV1.FilterExpr{left, right},
		}

		if fe.GetType() != paginationV1.ExprType_AND {
			t.Fatalf("expected top-level AND, got %v", fe.GetType())
		}
		if len(fe.GetGroups()) != 2 {
			t.Fatalf("expected 2 groups, got %d", len(fe.GetGroups()))
		}
		// marshal to ensure protobuf JSON representation is valid
		js := mustMarshal(fe)
		if js == "" {
			t.Fatal("protojson marshal returned empty string")
		}
	})
}

func TestNewStructuredFilter(t *testing.T) {
	sf := NewStructuredFilter()
	if sf == nil {
		t.Fatal("NewStructuredFilter returned nil")
	}
}

func TestBuildFilterSelectors_NilExpr(t *testing.T) {
	sf := NewStructuredFilter()

	sels, err := sf.BuildSelectors(nil)
	if err != nil {
		t.Fatalf("unexpected error for nil expr: %v", err)
	}
	if sels == nil {
		// code returns an empty slice; allow either empty or nil but prefer empty
		t.Log(" BuildSelectors(nil) returned nil slice (acceptable)")
	} else if len(sels) != 0 {
		t.Fatalf("expected 0 selectors for nil expr, got %d", len(sels))
	}
}

func TestBuildFilterSelectors_UnspecifiedExpr(t *testing.T) {
	sf := NewStructuredFilter()

	expr := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_EXPR_TYPE_UNSPECIFIED,
	}
	sels, err := sf.BuildSelectors(expr)
	if err != nil {
		t.Fatalf("unexpected error for unspecified expr: %v", err)
	}
	// implementation returns nil, nil for unspecified
	if sels != nil {
		t.Fatalf("expected nil selectors for unspecified expr, got %v", sels)
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

	// nil expr
	sel, err := sf.buildFilterSelector(nil)
	if err != nil {
		t.Fatalf("unexpected error for nil expr: %v", err)
	}
	if sel != nil {
		t.Fatal("expected nil selector for nil expr, got non-nil")
	}

	// unspecified expr
	expr := &paginationV1.FilterExpr{Type: paginationV1.ExprType_EXPR_TYPE_UNSPECIFIED}
	sel2, err := sf.buildFilterSelector(expr)
	if err != nil {
		t.Fatalf("unexpected error for unspecified expr: %v", err)
	}
	if sel2 != nil {
		t.Fatal("expected nil selector for unspecified expr, got non-nil")
	}
}

func TestStructuredFilter_VariousConditions(t *testing.T) {
	sf := NewStructuredFilter()
	if sf == nil {
		t.Fatal("NewStructuredFilter returned nil")
	}

	cases := []struct {
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
		{"LIKE", paginationV1.Operator_LIKE, "pattern%", nil},
		{"ILIKE", paginationV1.Operator_ILIKE, "pattern%", nil},
		{"NOT_LIKE", paginationV1.Operator_NOT_LIKE, "pattern%", nil},
		{"IN", paginationV1.Operator_IN, "", []string{"a", "b"}},
		{"NIN", paginationV1.Operator_NIN, "", []string{"a", "b"}},
		{"IS_NULL", paginationV1.Operator_IS_NULL, "", nil},
		{"IS_NOT_NULL", paginationV1.Operator_IS_NOT_NULL, "", nil},
		{"BETWEEN", paginationV1.Operator_BETWEEN, "", []string{"1", "5"}},
		{"REGEXP", paginationV1.Operator_REGEXP, "regex", nil},
		{"IREGEXP", paginationV1.Operator_IREGEXP, "regex", nil},
		{"CONTAINS", paginationV1.Operator_CONTAINS, "sub", nil},
		{"STARTS_WITH", paginationV1.Operator_STARTS_WITH, "pre", nil},
		{"ENDS_WITH", paginationV1.Operator_ENDS_WITH, "suf", nil},
		{"ICONTAINS", paginationV1.Operator_ICONTAINS, "sub", nil},
		{"ISTARTS_WITH", paginationV1.Operator_ISTARTS_WITH, "pre", nil},
		{"IENDS_WITH", paginationV1.Operator_IENDS_WITH, "suf", nil},
		{"JSON_CONTAINS", paginationV1.Operator_JSON_CONTAINS, `{"k":"v"}`, nil},
		{"ARRAY_CONTAINS", paginationV1.Operator_ARRAY_CONTAINS, "elem", nil},
		{"EXISTS", paginationV1.Operator_EXISTS, "subquery", nil},
		{"SEARCH", paginationV1.Operator_SEARCH, "q", nil},
		{"EXACT", paginationV1.Operator_EXACT, "exact", nil},
		{"IEXACT", paginationV1.Operator_IEXACT, "iexact", nil},
	}

	for _, tc := range cases {
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
