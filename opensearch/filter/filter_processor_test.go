package filter

import (
	"testing"

	"github.com/stretchr/testify/assert"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

func TestProcessor_BuildOpenSearchQuery_AllOperators(t *testing.T) {
	proc := NewProcessor()
	ops := []struct {
		op   paginationV1.Operator
		val  string
		vals []any
		want string // 断言结构类型
	}{
		{paginationV1.Operator_EQ, "v", nil, "term"},
		{paginationV1.Operator_NEQ, "v", nil, "bool"},
		{paginationV1.Operator_IN, "", []any{"a", "b"}, "terms"},
		{paginationV1.Operator_NIN, "", []any{"a", "b"}, "bool"},
		{paginationV1.Operator_GTE, "1", nil, "range"},
		{paginationV1.Operator_GT, "1", nil, "range"},
		{paginationV1.Operator_LTE, "1", nil, "range"},
		{paginationV1.Operator_LT, "1", nil, "range"},
		{paginationV1.Operator_BETWEEN, "", []any{"1", "2"}, "range"},
		{paginationV1.Operator_IS_NULL, "", nil, "bool"},
		{paginationV1.Operator_IS_NOT_NULL, "", nil, "exists"},
		{paginationV1.Operator_CONTAINS, "v", nil, "match_phrase"},
		{paginationV1.Operator_ICONTAINS, "v", nil, "match_phrase"},
		{paginationV1.Operator_STARTS_WITH, "v", nil, "prefix"},
		{paginationV1.Operator_ISTARTS_WITH, "v", nil, "prefix"},
		{paginationV1.Operator_ENDS_WITH, "v", nil, "wildcard"},
		{paginationV1.Operator_IENDS_WITH, "v", nil, "wildcard"},
		{paginationV1.Operator_EXACT, "v", nil, "term"},
		{paginationV1.Operator_IEXACT, "v", nil, "term"},
		{paginationV1.Operator_REGEXP, "v", nil, "regexp"},
		{paginationV1.Operator_IREGEXP, "v", nil, "regexp"},
		{paginationV1.Operator_SEARCH, "v", nil, "query_string"},
	}
	for _, tc := range ops {
		cond := &paginationV1.FilterCondition{
			Field:      "f",
			Op:         tc.op,
			ValueOneof: &paginationV1.FilterCondition_Value{Value: tc.val},
			Values:     toStringSlice(tc.vals),
		}
		got := proc.buildCond(cond)
		if tc.want != "" {
			if assert.NotNil(t, got, "op %v", tc.op) {
				found := false
				for k := range got {
					if k == tc.want {
						found = true
						break
					}
				}
				assert.True(t, found, "expect key %s for op %v, got %v", tc.want, tc.op, got)
			}
		}
	}
}

func TestProcessor_BuildOpenSearchQuery_AND_OR(t *testing.T) {
	proc := NewProcessor()
	// AND
	andExpr := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
		Conditions: []*paginationV1.FilterCondition{
			{Field: "f1", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "v1"}},
			{Field: "f2", Op: paginationV1.Operator_GT, ValueOneof: &paginationV1.FilterCondition_Value{Value: "2"}},
		},
	}
	q := proc.BuildOpenSearchQuery(andExpr)
	assert.NotNil(t, q)
	boolQ := q["bool"].(map[string]any)
	assert.Len(t, boolQ["must"], 2)
	// OR
	orExpr := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_OR,
		Conditions: []*paginationV1.FilterCondition{
			{Field: "f1", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "v1"}},
			{Field: "f2", Op: paginationV1.Operator_GT, ValueOneof: &paginationV1.FilterCondition_Value{Value: "2"}},
		},
	}
	q2 := proc.BuildOpenSearchQuery(orExpr)
	assert.NotNil(t, q2)
	boolQ2 := q2["bool"].(map[string]any)
	assert.Len(t, boolQ2["should"], 2)
}

func toStringSlice(vals []any) []string {
	if vals == nil {
		return nil
	}
	out := make([]string, len(vals))
	for i, v := range vals {
		out[i] = v.(string)
	}
	return out
}
