package filter

import (
	"fmt"
	"strings"
	"testing"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"go.einride.tech/aip/filtering"
	v1alpha1 "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

func walk(e *v1alpha1.Expr) {
	if e == nil {
		return
	}

	// 根据表达式类型进行处理
	switch kind := e.ExprKind.(type) {
	case *v1alpha1.Expr_ConstExpr:
		fmt.Printf("常量节点: %v\n", kind.ConstExpr)

	case *v1alpha1.Expr_IdentExpr:
		fmt.Printf("标识符节点: %s\n", kind.IdentExpr.Name)

	case *v1alpha1.Expr_CallExpr:
		// AIP 过滤中的运算符（如 =, AND, OR）会被解析为函数调用
		fmt.Printf("函数/运算符节点: %s\n", kind.CallExpr.Function)
		// 遍历函数参数
		for _, arg := range kind.CallExpr.Args {
			walk(arg)
		}

	case *v1alpha1.Expr_SelectExpr:
		fmt.Printf("字段选择节点: %s\n", kind.SelectExpr.Field)
		walk(kind.SelectExpr.Operand)

	default:
		fmt.Printf("其他类型节点: %T\n", kind)
	}
}

func TestAIP_ParseFilterString(t *testing.T) {
	// 1. 定义符合 AIP-160 标准的过滤字符串
	filterString := "name = 'example' AND (create_time > '2025-01-01T00:00:00Z' OR status = 1)"

	// 2. 定义声明（Declarations）
	declarationOptions := []filtering.DeclarationOption{
		filtering.DeclareStandardFunctions(),
		filtering.DeclareIdent("name", filtering.TypeString),
		filtering.DeclareIdent("create_time", filtering.TypeString),
		filtering.DeclareIdent("status", filtering.TypeInt),
	}
	declarations, err := filtering.NewDeclarations(declarationOptions...)

	// 3. 解析字符串
	aipExpr, err := filtering.ParseFilterString(filterString, declarations)
	if err != nil {
		t.Fatalf("failed to parse filter string: %v", err)
	}

	// 4. 遍历解析后的表达式（AST）
	fmt.Println("开始遍历表达式节点：")
	walk(aipExpr.CheckedExpr.GetExpr())
}

func TestAIP_Parser(t *testing.T) {
	// 1. 定义符合 AIP-160 标准的过滤字符串
	filterString := "name = 'example' AND (create_time > '2025-01-01T00:00:00Z' OR status = 1)"

	// 2. 使用 Parser 直接解析字符串
	var parser filtering.Parser
	parser.Init(filterString)
	actual, err := parser.Parse()
	if err != nil {
		t.Fatalf("failed to parse filter string: %v", err)
	}

	// 3. 遍历解析后的表达式（AST）
	fmt.Println("开始遍历表达式节点：")
	walk(actual.GetExpr())
}

func TestFilterStringConverter_Empty(t *testing.T) {
	fsc := NewFilterStringConverter()
	got, err := fsc.Convert("")
	if err != nil {
		t.Fatalf("Convert(empty) returned error: %v", err)
	}
	if got != nil {
		t.Fatalf("Convert(empty) = %v, want nil", got)
	}
}

func TestFilterStringConverter_ConvertComplex(t *testing.T) {
	fsc := NewFilterStringConverter()
	filterString := "name = 'example' AND (create_time > '2025-01-01T00:00:00Z' OR status = 1)"

	got, err := fsc.Convert(filterString)
	if err != nil {
		t.Fatalf("Convert returned error: %v", err)
	}
	if got == nil {
		t.Fatalf("Convert returned nil FilterExpr")
	}

	// 顶层应为 AND，包含三个简单条件（name, create_time, status）
	if got.Type != paginationV1.ExprType_AND {
		t.Fatalf("unexpected ExprType: got %v want %v", got.Type, paginationV1.ExprType_AND)
	}
	if len(got.Conditions) != 3 {
		t.Fatalf("unexpected number of conditions: got %d want %d", len(got.Conditions), 3)
	}

	// helper: extract value string safely
	getVal := func(c *paginationV1.FilterCondition) string {
		if c == nil || c.ValueOneof == nil {
			return ""
		}
		if v, ok := c.ValueOneof.(*paginationV1.FilterCondition_Value); ok {
			return v.Value
		}
		return ""
	}

	// 条件 0: name = 'example'
	c0 := got.Conditions[0]
	if c0.Field != "name" {
		t.Fatalf("cond0 field = %q, want %q", c0.Field, "name")
	}
	if c0.Op != paginationV1.Operator_EQ {
		t.Fatalf("cond0 op = %v, want %v", c0.Op, paginationV1.Operator_EQ)
	}
	if !strings.Contains(getVal(c0), "example") {
		t.Fatalf("cond0 value does not contain %q: %q", "example", getVal(c0))
	}

	// 条件 1: create_time > '2025-01-01T00:00:00Z'
	c1 := got.Conditions[1]
	if c1.Field != "create_time" {
		t.Fatalf("cond1 field = %q, want %q", c1.Field, "create_time")
	}
	if c1.Op != paginationV1.Operator_GT {
		t.Fatalf("cond1 op = %v, want %v", c1.Op, paginationV1.Operator_GT)
	}
	if !strings.Contains(getVal(c1), "2025-01-01") {
		t.Fatalf("cond1 value does not contain %q: %q", "2025-01-01", getVal(c1))
	}

	// 条件 2: status = 1
	c2 := got.Conditions[2]
	if c2.Field != "status" {
		t.Fatalf("cond2 field = %q, want %q", c2.Field, "status")
	}
	if c2.Op != paginationV1.Operator_EQ {
		t.Fatalf("cond2 op = %v, want %v", c2.Op, paginationV1.Operator_EQ)
	}
	// 数字常量在 proto 字符串化中可能表现为 int64_value:1 等，使用包含 "1" 做宽松断言
	if !strings.Contains(getVal(c2), "1") {
		t.Fatalf("cond2 value does not contain %q: %q", "1", getVal(c2))
	}
}

func TestFilterStringConverter_mapOperator(t *testing.T) {
	fsc := NewFilterStringConverter()

	cases := []struct {
		in  string
		out paginationV1.Operator
	}{
		{"=", paginationV1.Operator_EQ},
		{"==", paginationV1.Operator_EQ},
		{"!=", paginationV1.Operator_NEQ},
		{">", paginationV1.Operator_GT},
		{">=", paginationV1.Operator_GTE},
		{"<", paginationV1.Operator_LT},
		{"<=", paginationV1.Operator_LTE},
		{"in", paginationV1.Operator_IN},
		{"not in", paginationV1.Operator_NIN},
		{"contains", paginationV1.Operator_CONTAINS},
		{"startsWith", paginationV1.Operator_STARTS_WITH},
		{"endsWith", paginationV1.Operator_ENDS_WITH},
		{"is null", paginationV1.Operator_IS_NULL},
		{"is not null", paginationV1.Operator_IS_NOT_NULL},
		{"unknown", paginationV1.Operator_OPERATOR_UNSPECIFIED},
	}

	for _, c := range cases {
		got := fsc.mapOperator(c.in)
		if got != c.out {
			t.Fatalf("mapOperator(%q) = %v, want %v", c.in, got, c.out)
		}
	}
}
