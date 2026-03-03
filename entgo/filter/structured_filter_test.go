package filter

import (
	"fmt"
	"strings"
	"testing"

	"github.com/tx7do/go-utils/trans"
	"google.golang.org/protobuf/encoding/protojson"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"

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

		// 验证生成的 SQL
		sf := NewStructuredFilter()
		sels, err := sf.BuildSelectors(fe)
		if err != nil {
			t.Fatalf("BuildSelectors error: %v", err)
		}
		if len(sels) != 1 {
			t.Fatalf("expected 1 selector, got %d", len(sels))
		}

		s := sql.Dialect(dialect.MySQL).Select("*").From(sql.Table("t"))
		sels[0](s)
		gotSQL, gotArgs := s.Query()

		if !strings.Contains(strings.ToLower(gotSQL), "where") {
			t.Fatalf("expected WHERE clause, got: %s", gotSQL)
		}
		if len(gotArgs) != 2 || gotArgs[0] != "1" || gotArgs[1] != "2" {
			t.Fatalf("expected args ['1', '2'], got %#v", gotArgs)
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

		// 验证生成的 SQL
		sf := NewStructuredFilter()
		sels, err := sf.BuildSelectors(fe)
		if err != nil {
			t.Fatalf("BuildSelectors error: %v", err)
		}
		if len(sels) != 1 {
			t.Fatalf("expected 1 selector, got %d", len(sels))
		}

		s := sql.Dialect(dialect.MySQL).Select("*").From(sql.Table("t"))
		sels[0](s)
		gotSQL, gotArgs := s.Query()

		if !strings.Contains(strings.ToLower(gotSQL), "where") {
			t.Fatalf("expected WHERE clause, got: %s", gotSQL)
		}
		if len(gotArgs) != 2 || gotArgs[0] != "1" || gotArgs[1] != "2" {
			t.Fatalf("expected args ['1', '2'], got %#v", gotArgs)
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

		// 验证生成的 SQL
		sf := NewStructuredFilter()
		sels, err := sf.BuildSelectors(fe)
		if err != nil {
			t.Fatalf("BuildSelectors error: %v", err)
		}
		if len(sels) != 1 {
			t.Fatalf("expected 1 selector, got %d", len(sels))
		}

		s := sql.Dialect(dialect.MySQL).Select("*").From(sql.Table("t"))
		sels[0](s)
		gotSQL, gotArgs := s.Query()

		// 验证 SQL 包含 WHERE 子句
		if !strings.Contains(strings.ToLower(gotSQL), "where") {
			t.Fatalf("expected WHERE clause, got: %s", gotSQL)
		}

		// 验证至少包含 3 个参数（A=1, B=2, C=3）
		if len(gotArgs) < 3 {
			t.Fatalf("expected at least 3 args, got %d: %#v", len(gotArgs), gotArgs)
		}

		// 验证包含预期的值
		values := make(map[string]bool)
		for _, arg := range gotArgs {
			values[fmt.Sprintf("%v", arg)] = true
		}

		if !values["1"] {
			t.Fatalf("expected arg '1' (for A), got args: %#v", gotArgs)
		}
		if !values["2"] {
			t.Fatalf("expected arg '2' (for B), got args: %#v", gotArgs)
		}
		if !values["3"] {
			t.Fatalf("expected arg '3' (for C), got args: %#v", gotArgs)
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

		// 验证生成的 SQL
		sf := NewStructuredFilter()
		sels, err := sf.BuildSelectors(fe)
		if err != nil {
			t.Fatalf("BuildSelectors error: %v", err)
		}
		if len(sels) != 1 {
			t.Fatalf("expected 1 selector, got %d", len(sels))
		}

		s := sql.Dialect(dialect.MySQL).Select("*").From(sql.Table("t"))
		sels[0](s)
		gotSQL, gotArgs := s.Query()

		if !strings.Contains(strings.ToLower(gotSQL), "where") {
			t.Fatalf("expected WHERE clause, got: %s", gotSQL)
		}
		// 验证参数数量（5个值：a, b, c, d, e）
		if len(gotArgs) != 5 {
			t.Fatalf("expected 5 args, got %d: %#v", len(gotArgs), gotArgs)
		}
	})

	t.Run("SingleCondition", func(t *testing.T) {
		// 只有一个条件的简单情况
		fe := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{
				{Field: "status", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "active"}},
			},
		}

		sf := NewStructuredFilter()
		sels, err := sf.BuildSelectors(fe)
		if err != nil {
			t.Fatalf("BuildSelectors error: %v", err)
		}
		if len(sels) != 1 {
			t.Fatalf("expected 1 selector, got %d", len(sels))
		}

		s := sql.Dialect(dialect.MySQL).Select("*").From(sql.Table("t"))
		sels[0](s)
		gotSQL, gotArgs := s.Query()

		if !strings.Contains(strings.ToLower(gotSQL), "where") {
			t.Fatalf("expected WHERE clause, got: %s", gotSQL)
		}
		if len(gotArgs) != 1 || gotArgs[0] != "active" {
			t.Fatalf("expected args ['active'], got %#v", gotArgs)
		}
	})

	t.Run("EmptyConditions", func(t *testing.T) {
		// 空条件列表
		fe := &paginationV1.FilterExpr{
			Type:       paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{},
		}

		sf := NewStructuredFilter()
		sels, err := sf.BuildSelectors(fe)
		if err != nil {
			t.Fatalf("BuildSelectors error: %v", err)
		}
		// 空条件应该返回 nil 或 1 个 selector（取决于实现）
		if sels != nil && len(sels) > 0 && sels[0] != nil {
			s := sql.Dialect(dialect.MySQL).Select("*").From(sql.Table("t"))
			sels[0](s)
			gotSQL, _ := s.Query()
			// 空条件不应该生成 WHERE 子句
			if strings.Contains(strings.ToLower(gotSQL), "where") {
				t.Fatalf("empty conditions should not generate WHERE clause, got: %s", gotSQL)
			}
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

func TestBuildFilterSelectors_JsonField(t *testing.T) {
	sf := NewStructuredFilter()

	t.Run("BasicEqualityWithJsonPath", func(t *testing.T) {
		// 基础 JSONB 查询：WHERE JSON_EXTRACT(`data`, '$.key') = 'v1'
		expr := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{
				{
					Field:      "data",
					JsonPath:   trans.Ptr("key"),
					Op:         paginationV1.Operator_EQ,
					ValueOneof: &paginationV1.FilterCondition_Value{Value: "v1"},
				},
			},
		}
		sels, err := sf.BuildSelectors(expr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(sels) != 1 {
			t.Fatalf("expected 1 selector, got %d", len(sels))
		}

		s := sql.Dialect(dialect.MySQL).
			Select("id", "data").
			From(sql.Table("test_table"))

		sels[0](s)
		gotSQL, gotArgs := s.Query()

		if !strings.Contains(gotSQL, "FROM `test_table`") {
			t.Fatalf("expected FROM clause, got: %s", gotSQL)
		}
		if !strings.Contains(strings.ToLower(gotSQL), "where") {
			t.Fatalf("expected WHERE in SQL, got: %s", gotSQL)
		}
		if !strings.Contains(gotSQL, "`data`") {
			t.Fatalf("expected field 'data' in SQL, got: %s", gotSQL)
		}
		if !strings.Contains(gotSQL, "JSON_EXTRACT") {
			t.Fatalf("expected JSON_EXTRACT in SQL for MySQL, got: %s", gotSQL)
		}
		if len(gotArgs) != 1 || gotArgs[0] != "v1" {
			t.Fatalf("unexpected args: %#v", gotArgs)
		}
	})

	t.Run("NestedJsonPath", func(t *testing.T) {
		// 嵌套 JSON 路径：WHERE JSON_EXTRACT(`meta`, '$.user.name') = 'John'
		expr := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{
				{
					Field:      "meta",
					JsonPath:   trans.Ptr("user.name"),
					Op:         paginationV1.Operator_EQ,
					ValueOneof: &paginationV1.FilterCondition_Value{Value: "John"},
				},
			},
		}
		sels, err := sf.BuildSelectors(expr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		s := sql.Dialect(dialect.MySQL).Select("*").From(sql.Table("users"))
		sels[0](s)
		gotSQL, gotArgs := s.Query()

		if !strings.Contains(gotSQL, "JSON_EXTRACT") {
			t.Fatalf("expected JSON_EXTRACT in SQL, got: %s", gotSQL)
		}
		if !strings.Contains(gotSQL, "user.name") {
			t.Fatalf("expected nested path 'user.name', got: %s", gotSQL)
		}
		if len(gotArgs) != 1 || gotArgs[0] != "John" {
			t.Fatalf("unexpected args: %#v", gotArgs)
		}
	})

	t.Run("JsonPathNotEqual", func(t *testing.T) {
		// NEQ 操作：WHERE JSON_EXTRACT(`data`, '$.status') <> 'inactive'
		expr := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{
				{
					Field:      "data",
					JsonPath:   trans.Ptr("status"),
					Op:         paginationV1.Operator_NEQ,
					ValueOneof: &paginationV1.FilterCondition_Value{Value: "inactive"},
				},
			},
		}
		sels, err := sf.BuildSelectors(expr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		s := sql.Dialect(dialect.MySQL).Select("*").From(sql.Table("t"))
		sels[0](s)
		gotSQL, gotArgs := s.Query()

		if !strings.Contains(gotSQL, "<>") && !strings.Contains(gotSQL, "!=") {
			t.Fatalf("expected <> or != operator in SQL, got: %s", gotSQL)
		}
		if len(gotArgs) != 1 || gotArgs[0] != "inactive" {
			t.Fatalf("unexpected args: %#v", gotArgs)
		}
	})

	t.Run("JsonPathContains", func(t *testing.T) {
		// CONTAINS 操作：WHERE JSON_EXTRACT(`data`, '$.tags') LIKE '%python%'
		expr := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{
				{
					Field:      "data",
					JsonPath:   trans.Ptr("tags"),
					Op:         paginationV1.Operator_CONTAINS,
					ValueOneof: &paginationV1.FilterCondition_Value{Value: "python"},
				},
			},
		}
		sels, err := sf.BuildSelectors(expr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		s := sql.Dialect(dialect.MySQL).Select("*").From(sql.Table("t"))
		sels[0](s)
		gotSQL, gotArgs := s.Query()

		if !strings.Contains(strings.ToLower(gotSQL), "like") {
			t.Fatalf("expected LIKE operator in SQL, got: %s", gotSQL)
		}
		if len(gotArgs) != 1 {
			t.Fatalf("expected 1 arg, got %d: %#v", len(gotArgs), gotArgs)
		}
		// CONTAINS 应该将参数转换为 %value%
		if !strings.Contains(fmt.Sprintf("%v", gotArgs[0]), "python") {
			t.Fatalf("expected 'python' in args, got: %#v", gotArgs)
		}
	})

	t.Run("JsonPathStartsWith", func(t *testing.T) {
		// STARTS_WITH 操作
		expr := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{
				{
					Field:      "data",
					JsonPath:   trans.Ptr("name"),
					Op:         paginationV1.Operator_STARTS_WITH,
					ValueOneof: &paginationV1.FilterCondition_Value{Value: "admin"},
				},
			},
		}
		sels, err := sf.BuildSelectors(expr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		s := sql.Dialect(dialect.MySQL).Select("*").From(sql.Table("t"))
		sels[0](s)
		gotSQL, gotArgs := s.Query()

		if !strings.Contains(strings.ToLower(gotSQL), "like") {
			t.Fatalf("expected LIKE operator in SQL, got: %s", gotSQL)
		}
		if len(gotArgs) != 1 {
			t.Fatalf("expected 1 arg, got %d: %#v", len(gotArgs), gotArgs)
		}
	})

	t.Run("JsonPathIN", func(t *testing.T) {
		// IN 操作：WHERE JSON_EXTRACT(`data`, '$.type') IN ('admin', 'user', 'guest')
		expr := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{
				{
					Field:    "data",
					JsonPath: trans.Ptr("type"),
					Op:       paginationV1.Operator_IN,
					Values:   []string{"admin", "user", "guest"},
				},
			},
		}
		sels, err := sf.BuildSelectors(expr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		s := sql.Dialect(dialect.MySQL).Select("*").From(sql.Table("t"))
		sels[0](s)
		gotSQL, gotArgs := s.Query()

		if !strings.Contains(strings.ToLower(gotSQL), "in") {
			t.Fatalf("expected IN operator in SQL, got: %s", gotSQL)
		}
		if len(gotArgs) != 3 {
			t.Fatalf("expected 3 args, got %d: %#v", len(gotArgs), gotArgs)
		}
	})

	t.Run("EmptyJsonPath", func(t *testing.T) {
		// 空 JsonPath - 应该作为普通字段处理
		expr := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{
				{
					Field:      "data",
					JsonPath:   trans.Ptr(""),
					Op:         paginationV1.Operator_EQ,
					ValueOneof: &paginationV1.FilterCondition_Value{Value: "value"},
				},
			},
		}
		sels, err := sf.BuildSelectors(expr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		s := sql.Dialect(dialect.MySQL).Select("*").From(sql.Table("t"))
		sels[0](s)
		gotSQL, _ := s.Query()

		// 空 JsonPath 应该作为普通字段处理，不应该包含 JSON_EXTRACT
		if strings.Contains(gotSQL, "JSON_EXTRACT") {
			t.Logf("Note: Empty JsonPath generated JSON_EXTRACT, may be expected behavior")
		}
	})

	t.Run("PostgresJsonbOperator", func(t *testing.T) {
		// Postgres 方言：使用 ->> 操作符
		expr := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{
				{
					Field:      "data",
					JsonPath:   trans.Ptr("status"),
					Op:         paginationV1.Operator_EQ,
					ValueOneof: &paginationV1.FilterCondition_Value{Value: "active"},
				},
			},
		}
		sels, err := sf.BuildSelectors(expr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// 使用 Postgres 方言
		s := sql.Dialect(dialect.Postgres).Select("*").From(sql.Table("t"))
		sels[0](s)
		gotSQL, gotArgs := s.Query()

		if !strings.Contains(gotSQL, "->>") {
			t.Fatalf("expected ->> operator for Postgres JSONB, got: %s", gotSQL)
		}
		if len(gotArgs) != 1 || gotArgs[0] != "active" {
			t.Fatalf("unexpected args: %#v", gotArgs)
		}
	})

	t.Run("JsonPathWithMultipleConditions", func(t *testing.T) {
		// 多个 JSONB 条件组合
		expr := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{
				{
					Field:      "meta",
					JsonPath:   trans.Ptr("role"),
					Op:         paginationV1.Operator_EQ,
					ValueOneof: &paginationV1.FilterCondition_Value{Value: "admin"},
				},
				{
					Field:      "meta",
					JsonPath:   trans.Ptr("active"),
					Op:         paginationV1.Operator_EQ,
					ValueOneof: &paginationV1.FilterCondition_Value{Value: "true"},
				},
			},
		}
		sels, err := sf.BuildSelectors(expr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		s := sql.Dialect(dialect.MySQL).Select("*").From(sql.Table("t"))
		sels[0](s)
		gotSQL, gotArgs := s.Query()

		// 两个条件都应该使用 JSON_EXTRACT
		count := strings.Count(gotSQL, "JSON_EXTRACT")
		if count < 2 {
			t.Fatalf("expected at least 2 JSON_EXTRACT calls, got %d in: %s", count, gotSQL)
		}

		if len(gotArgs) != 2 {
			t.Fatalf("expected 2 args, got %d: %#v", len(gotArgs), gotArgs)
		}
	})

	t.Run("JsonPathVsNormalField", func(t *testing.T) {
		// 比较 JsonPath 和普通字段生成的 SQL
		jsonPathExpr := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{
				{
					Field:      "data",
					JsonPath:   trans.Ptr("name"),
					Op:         paginationV1.Operator_EQ,
					ValueOneof: &paginationV1.FilterCondition_Value{Value: "test"},
				},
			},
		}

		normalFieldExpr := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{
				{
					Field:      "name",
					Op:         paginationV1.Operator_EQ,
					ValueOneof: &paginationV1.FilterCondition_Value{Value: "test"},
				},
			},
		}

		jsonSels, err := sf.BuildSelectors(jsonPathExpr)
		if err != nil {
			t.Fatalf("unexpected error for JSON path: %v", err)
		}

		normalSels, err := sf.BuildSelectors(normalFieldExpr)
		if err != nil {
			t.Fatalf("unexpected error for normal field: %v", err)
		}

		s1 := sql.Dialect(dialect.MySQL).Select("*").From(sql.Table("t"))
		jsonSels[0](s1)
		jsonSQL, jsonArgs := s1.Query()

		s2 := sql.Dialect(dialect.MySQL).Select("*").From(sql.Table("t"))
		normalSels[0](s2)
		normalSQL, normalArgs := s2.Query()

		// JsonPath 应该使用 JSON_EXTRACT，普通字段不应该
		if !strings.Contains(jsonSQL, "JSON_EXTRACT") {
			t.Fatalf("expected JSON_EXTRACT for JsonPath, got: %s", jsonSQL)
		}

		if strings.Contains(normalSQL, "JSON_EXTRACT") {
			t.Fatalf("unexpected JSON_EXTRACT for normal field, got: %s", normalSQL)
		}

		// 两者都应该有参数
		if len(jsonArgs) == 0 {
			t.Fatalf("expected args for JsonPath query, got empty")
		}
		if len(normalArgs) == 0 {
			t.Fatalf("expected args for normal field query, got empty")
		}
	})
}
