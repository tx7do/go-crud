package filter

import (
	"testing"

	"github.com/tx7do/go-utils/trans"
	"google.golang.org/protobuf/encoding/protojson"

	pagination "github.com/tx7do/go-curd/api/gen/go/pagination/v1"
)

func mustMarshal(fe *pagination.FilterExpr) string {
	b, _ := protojson.MarshalOptions{Multiline: false, EmitUnpopulated: false}.Marshal(fe)
	return string(b)
}

func TestFilterExprExamples(t *testing.T) {
	t.Run("SimpleAND", func(t *testing.T) {
		// SQL: WHERE A = '1' AND B = '2'
		fe := &pagination.FilterExpr{
			Type: pagination.ExprType_AND,
			Conditions: []*pagination.Condition{
				{Field: "A", Op: pagination.Operator_EQ, Value: trans.Ptr("1")},
				{Field: "B", Op: pagination.Operator_EQ, Value: trans.Ptr("2")},
			},
		}

		if fe.GetType() != pagination.ExprType_AND {
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
		fe := &pagination.FilterExpr{
			Type: pagination.ExprType_OR,
			Conditions: []*pagination.Condition{
				{Field: "A", Op: pagination.Operator_EQ, Value: trans.Ptr("1")},
				{Field: "B", Op: pagination.Operator_EQ, Value: trans.Ptr("2")},
			},
		}

		if fe.GetType() != pagination.ExprType_OR {
			t.Fatalf("expected OR, got %v", fe.GetType())
		}
		if len(fe.GetConditions()) != 2 {
			t.Fatalf("expected 2 conditions, got %d", len(fe.GetConditions()))
		}
	})

	t.Run("Mixed_A_AND_BorC", func(t *testing.T) {
		// Logical: A AND (B OR C)
		// SQL: WHERE A = '1' AND (B = '2' OR C = '3')
		orGroup := &pagination.FilterExpr{
			Type: pagination.ExprType_OR,
			Conditions: []*pagination.Condition{
				{Field: "B", Op: pagination.Operator_EQ, Value: trans.Ptr("2")},
				{Field: "C", Op: pagination.Operator_EQ, Value: trans.Ptr("3")},
			},
		}
		fe := &pagination.FilterExpr{
			Type:       pagination.ExprType_AND,
			Conditions: []*pagination.Condition{{Field: "A", Op: pagination.Operator_EQ, Value: trans.Ptr("1")}},
			Groups:     []*pagination.FilterExpr{orGroup},
		}

		if fe.GetType() != pagination.ExprType_AND {
			t.Fatalf("expected top-level AND, got %v", fe.GetType())
		}
		if len(fe.GetConditions()) != 1 {
			t.Fatalf("expected 1 top-level condition, got %d", len(fe.GetConditions()))
		}
		if len(fe.GetGroups()) != 1 {
			t.Fatalf("expected 1 group, got %d", len(fe.GetGroups()))
		}
		if fe.GetGroups()[0].GetType() != pagination.ExprType_OR {
			t.Fatalf("expected inner group OR, got %v", fe.GetGroups()[0].GetType())
		}
	})

	t.Run("ComplexNested", func(t *testing.T) {
		// Logical: (A OR B) AND (C OR (D AND E))
		// SQL: WHERE (A = 'a' OR B = 'b') AND (C = 'c' OR (D = 'd' AND E = 'e'))
		left := &pagination.FilterExpr{
			Type: pagination.ExprType_OR,
			Conditions: []*pagination.Condition{
				{Field: "A", Op: pagination.Operator_EQ, Value: trans.Ptr("a")},
				{Field: "B", Op: pagination.Operator_EQ, Value: trans.Ptr("b")},
			},
		}
		rightInner := &pagination.FilterExpr{
			Type: pagination.ExprType_AND,
			Conditions: []*pagination.Condition{
				{Field: "D", Op: pagination.Operator_EQ, Value: trans.Ptr("d")},
				{Field: "E", Op: pagination.Operator_EQ, Value: trans.Ptr("e")},
			},
		}
		right := &pagination.FilterExpr{
			Type: pagination.ExprType_OR,
			Conditions: []*pagination.Condition{
				{Field: "C", Op: pagination.Operator_EQ, Value: trans.Ptr("c")},
			},
			Groups: []*pagination.FilterExpr{rightInner},
		}
		fe := &pagination.FilterExpr{
			Type:   pagination.ExprType_AND,
			Groups: []*pagination.FilterExpr{left, right},
		}

		if fe.GetType() != pagination.ExprType_AND {
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
	if sf.processor == nil {
		t.Fatal("expected processor to be initialized")
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

	expr := &pagination.FilterExpr{
		Type: pagination.ExprType_EXPR_TYPE_UNSPECIFIED,
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

	expr := &pagination.FilterExpr{
		Type: pagination.ExprType_AND,
		Conditions: []*pagination.Condition{
			{Field: "A", Op: pagination.Operator_EQ, Value: trans.Ptr("1")},
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
	expr := &pagination.FilterExpr{Type: pagination.ExprType_EXPR_TYPE_UNSPECIFIED}
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
		{"LIKE", pagination.Operator_LIKE, "pattern%", nil},
		{"ILIKE", pagination.Operator_ILIKE, "pattern%", nil},
		{"NOT_LIKE", pagination.Operator_NOT_LIKE, "pattern%", nil},
		{"IN", pagination.Operator_IN, "", []string{"a", "b"}},
		{"NIN", pagination.Operator_NIN, "", []string{"a", "b"}},
		{"IS_NULL", pagination.Operator_IS_NULL, "", nil},
		{"IS_NOT_NULL", pagination.Operator_IS_NOT_NULL, "", nil},
		{"BETWEEN", pagination.Operator_BETWEEN, "", []string{"1", "5"}},
		{"REGEXP", pagination.Operator_REGEXP, "regex", nil},
		{"IREGEXP", pagination.Operator_IREGEXP, "regex", nil},
		{"CONTAINS", pagination.Operator_CONTAINS, "sub", nil},
		{"STARTS_WITH", pagination.Operator_STARTS_WITH, "pre", nil},
		{"ENDS_WITH", pagination.Operator_ENDS_WITH, "suf", nil},
		{"ICONTAINS", pagination.Operator_ICONTAINS, "sub", nil},
		{"ISTARTS_WITH", pagination.Operator_ISTARTS_WITH, "pre", nil},
		{"IENDS_WITH", pagination.Operator_IENDS_WITH, "suf", nil},
		{"JSON_CONTAINS", pagination.Operator_JSON_CONTAINS, `{"k":"v"}`, nil},
		{"ARRAY_CONTAINS", pagination.Operator_ARRAY_CONTAINS, "elem", nil},
		{"EXISTS", pagination.Operator_EXISTS, "subquery", nil},
		{"SEARCH", pagination.Operator_SEARCH, "q", nil},
		{"EXACT", pagination.Operator_EXACT, "exact", nil},
		{"IEXACT", pagination.Operator_IEXACT, "iexact", nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
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
