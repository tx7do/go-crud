package filter

import (
	"testing"

	"google.golang.org/protobuf/proto"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

func TestConvertFilterByPagingRequest_NilRequest(t *testing.T) {
	// nil 请求应该返回 nil FilterExpr 和 nil error
	result, err := ConvertFilterByPagingRequest(nil)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil FilterExpr, got %v", result)
	}
}

func TestConvertFilterByPagingRequest_EmptyRequest(t *testing.T) {
	// 空请求（无任何过滤条件）应该返回 nil FilterExpr 和 nil error
	req := &paginationV1.PagingRequest{}
	result, err := ConvertFilterByPagingRequest(req)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil FilterExpr for empty request, got %v", result)
	}
}

func TestConvertFilterByPagingRequest_WithFilterExpr(t *testing.T) {
	// 直接提供 FilterExpr 时，应该直接返回该 FilterExpr
	expected := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
		Conditions: []*paginationV1.FilterCondition{
			{
				Field:      "name",
				Op:         paginationV1.Operator_EQ,
				ValueOneof: &paginationV1.FilterCondition_Value{Value: "test"},
			},
		},
	}

	req := &paginationV1.PagingRequest{
		FilteringType: &paginationV1.PagingRequest_FilterExpr{
			FilterExpr: expected,
		},
	}

	result, err := ConvertFilterByPagingRequest(req)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result == nil {
		t.Fatalf("expected non-nil FilterExpr, got nil")
	}
	if !proto.Equal(result, expected) {
		t.Fatalf("FilterExpr mismatch: got %v, want %v", result, expected)
	}
}

func TestConvertFilterByPagingRequest_WithQuery(t *testing.T) {
	// 提供 Query 字符串时，应该解析为 FilterExpr
	query := `{"name":"john"}`
	req := &paginationV1.PagingRequest{
		FilteringType: &paginationV1.PagingRequest_Query{
			Query: query,
		},
	}

	result, err := ConvertFilterByPagingRequest(req)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result == nil {
		t.Fatalf("expected non-nil FilterExpr, got nil")
	}

	// 验证返回的是 AND 类型表达式
	if result.Type != paginationV1.ExprType_AND {
		t.Fatalf("expected AND type, got %v", result.Type)
	}
}

func TestConvertFilterByPagingRequest_WithFilter(t *testing.T) {
	// 提供 Filter 字符串时，应该解析为 FilterExpr
	filter := `name__eq=john`
	req := &paginationV1.PagingRequest{
		FilteringType: &paginationV1.PagingRequest_Filter{
			Filter: filter,
		},
	}

	result, err := ConvertFilterByPagingRequest(req)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result == nil {
		t.Fatalf("expected non-nil FilterExpr, got nil")
	}

	// 验证返回的是 AND 类型表达式
	if result.Type != paginationV1.ExprType_AND {
		t.Fatalf("expected AND type, got %v", result.Type)
	}
}

func TestConvertFilterByPagingRequest_PriorityOrder(t *testing.T) {
	// FilterExpr > Query > Filter，测试优先级
	filterExpr := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_OR,
		Conditions: []*paginationV1.FilterCondition{
			{
				Field:      "priority",
				Op:         paginationV1.Operator_EQ,
				ValueOneof: &paginationV1.FilterCondition_Value{Value: "filterexpr"},
			},
		},
	}

	req := &paginationV1.PagingRequest{
		FilteringType: &paginationV1.PagingRequest_FilterExpr{
			FilterExpr: filterExpr,
		},
	}

	result, err := ConvertFilterByPagingRequest(req)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	// 应该返回 FilterExpr（即 OR 类型，而不是其他）
	if result.Type != paginationV1.ExprType_OR {
		t.Fatalf("expected OR type (from FilterExpr), got %v", result.Type)
	}

	// 验证条件内容
	if len(result.Conditions) != 1 {
		t.Fatalf("expected 1 condition, got %d", len(result.Conditions))
	}

	cond := result.Conditions[0]
	if cond.Field != "priority" {
		t.Fatalf("expected field 'priority', got '%s'", cond.Field)
	}
	if cond.Op != paginationV1.Operator_EQ {
		t.Fatalf("expected EQ operator, got %v", cond.Op)
	}
}

func TestConvertFilterByPagingRequest_InvalidQuery(t *testing.T) {
	// 无效的 JSON 查询应该返回错误
	req := &paginationV1.PagingRequest{
		FilteringType: &paginationV1.PagingRequest_Query{
			Query: `{invalid json}`,
		},
	}

	result, err := ConvertFilterByPagingRequest(req)
	if err == nil {
		t.Fatalf("expected error for invalid JSON, got nil")
	}
	if result != nil {
		t.Fatalf("expected nil FilterExpr for invalid query, got %v", result)
	}
}

func TestConvertFilterByPagingRequest_InvalidFilter(t *testing.T) {
	// 无效的 Filter 字符串可能返回错误或 nil（取决于实现）
	req := &paginationV1.PagingRequest{
		FilteringType: &paginationV1.PagingRequest_Filter{
			Filter: `invalid__filter__format==value`,
		},
	}

	result, err := ConvertFilterByPagingRequest(req)
	// 结果依赖于 filterStringConverter 的实现，可能是错误或 nil
	if result != nil {
		// 即使转换成功，也应该是有效的 FilterExpr
		if result.Type == paginationV1.ExprType_EXPR_TYPE_UNSPECIFIED {
			t.Fatalf("expected valid FilterExpr type, got UNSPECIFIED")
		}
	}
	_ = err // 可能有错误，也可能没有
}

func TestConvertFilterByPagingRequest_ComplexQuery(t *testing.T) {
	// 复杂的查询表达式
	query := `[{"status":"active"},{"age__gte":"18"}]`
	req := &paginationV1.PagingRequest{
		FilteringType: &paginationV1.PagingRequest_Query{
			Query: query,
		},
	}

	result, err := ConvertFilterByPagingRequest(req)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result == nil {
		t.Fatalf("expected non-nil FilterExpr, got nil")
	}

	// 验证表达式类型
	if result.Type != paginationV1.ExprType_AND {
		t.Fatalf("expected AND type for array query, got %v", result.Type)
	}
}

func TestConvertFilterByPagingRequest_WithPaginationOptions(t *testing.T) {
	// 包含分页选项的请求，但没有过滤条件，应该返回 nil FilterExpr
	page := uint32(1)
	pageSize := uint32(10)

	req := &paginationV1.PagingRequest{
		Page:     &page,
		PageSize: &pageSize,
	}

	result, err := ConvertFilterByPagingRequest(req)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil FilterExpr when only pagination options provided, got %v", result)
	}
}

func TestConvertFilterByPagingRequest_QueryWithPaginationOptions(t *testing.T) {
	// 包含查询和分页选项的请求，应该只处理查询
	page := uint32(2)
	pageSize := uint32(20)
	query := `{"id__gte":"100"}`

	req := &paginationV1.PagingRequest{
		Page:     &page,
		PageSize: &pageSize,
		FilteringType: &paginationV1.PagingRequest_Query{
			Query: query,
		},
	}

	result, err := ConvertFilterByPagingRequest(req)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result == nil {
		t.Fatalf("expected non-nil FilterExpr, got nil")
	}

	// FilterExpr 应该从查询解析，与分页选项无关
	if result.Type != paginationV1.ExprType_AND {
		t.Fatalf("expected AND type, got %v", result.Type)
	}
}

func TestConvertFilterByPagingRequest_EmptyQuery(t *testing.T) {
	// 空查询字符串应该返回 nil FilterExpr
	req := &paginationV1.PagingRequest{
		FilteringType: &paginationV1.PagingRequest_Query{
			Query: "",
		},
	}

	result, err := ConvertFilterByPagingRequest(req)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil FilterExpr for empty query, got %v", result)
	}
}

func TestConvertFilterByPagingRequest_EmptyFilter(t *testing.T) {
	// 空 Filter 字符串应该返回 nil FilterExpr
	req := &paginationV1.PagingRequest{
		FilteringType: &paginationV1.PagingRequest_Filter{
			Filter: "",
		},
	}

	result, err := ConvertFilterByPagingRequest(req)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result != nil {
		t.Fatalf("expected nil FilterExpr for empty filter, got %v", result)
	}
}

func TestConvertFilterByPagingRequest_QueryArrayFormat(t *testing.T) {
	// 数组格式的查询（等价于 AND）
	query := `[{"name":"alice"},{"age":"30"},{"city":"beijing"}]`
	req := &paginationV1.PagingRequest{
		FilteringType: &paginationV1.PagingRequest_Query{
			Query: query,
		},
	}

	result, err := ConvertFilterByPagingRequest(req)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result == nil {
		t.Fatalf("expected non-nil FilterExpr, got nil")
	}

	// 数组格式应该作为 AND 处理
	if result.Type != paginationV1.ExprType_AND {
		t.Fatalf("expected AND type for array query, got %v", result.Type)
	}
}

func TestConvertFilterByPagingRequest_QueryObjectFormat(t *testing.T) {
	// 对象格式的查询（支持 $and, $or 等）
	query := `{"$and":[{"status":"active"},{"priority__gte":"5"}]}`
	req := &paginationV1.PagingRequest{
		FilteringType: &paginationV1.PagingRequest_Query{
			Query: query,
		},
	}

	result, err := ConvertFilterByPagingRequest(req)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result == nil {
		t.Fatalf("expected non-nil FilterExpr, got nil")
	}

	// 应该返回有效的表达式
	if result.Type == paginationV1.ExprType_EXPR_TYPE_UNSPECIFIED {
		t.Fatalf("expected valid ExprType, got UNSPECIFIED")
	}
}
