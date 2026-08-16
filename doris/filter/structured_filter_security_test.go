package filter

import (
	"strings"
	"testing"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

// TestBuildFilterSelectors_InjectionFieldsNeutralized 验证携带 SQL 注入载荷的过滤字段名
// 无法进入 WHERE 子句：要么被直接拒绝（返回错误，fail-closed），
// 要么经 snake_case 归一化后载荷被彻底破坏（产物不含任何 SQL 元字符）。
func TestBuildFilterSelectors_InjectionFieldsNeutralized(t *testing.T) {
	hostileFields := []string{
		`preferences.x' = 'y' OR '1'='1`, // 引号逃逸（JSON key，必须报错）
		`preferences.x'),('y`,            // JSON key 引号逃逸变体（必须报错）
		`name = 'a' OR 1=1 --`,           // 注释 + 恒真
		`id; DROP TABLE users`,           // 语句拼接
		"status` --",                     // 反引号逃逸
		`(select version())`,             // 表达式注入
	}

	dangerous := []string{"`", "'", "(", ")", ";", "--", "/*"}

	for _, field := range hostileFields {
		expr := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{
				{Field: field, Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "1"}},
			},
		}

		sf := NewStructuredFilter()
		parts, _, err := sf.buildParts(expr)
		if err != nil {
			continue // 被拒绝，安全
		}
		joined := strings.Join(parts, " ")
		for _, d := range dangerous {
			if strings.Contains(joined, d) {
				t.Errorf("hostile field %q leaked %q into WHERE: %q", field, d, joined)
			}
		}
	}
}

// TestBuildFilterSelectors_JSONKeyMustBeValid 单独验证 JSON key 含非法字符时必须报错
// （这是引号逃逸注入的主通道）。
func TestBuildFilterSelectors_JSONKeyMustBeValid(t *testing.T) {
	for _, jsonKey := range []string{`x' OR '1'='1`, `x'),('y`, `x'; --`} {
		expr := &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{
				{Field: "preferences." + jsonKey, Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "1"}},
			},
		}
		sf := NewStructuredFilter()
		if _, _, err := sf.buildParts(expr); err == nil {
			t.Errorf("hostile JSON key %q must be rejected with an error", jsonKey)
		}
	}
}

// TestBuildFilterSelectors_ValidFieldsStillWork 验证合法字段（含 JSON 路径）不受影响。
func TestBuildFilterSelectors_ValidFieldsStillWork(t *testing.T) {
	expr := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
		Conditions: []*paginationV1.FilterCondition{
			{Field: "name", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "bob"}},
			{Field: "preferences.daily_email", Op: paginationV1.Operator_EQ, ValueOneof: &paginationV1.FilterCondition_Value{Value: "1"}},
		},
	}

	sf := NewStructuredFilter()
	parts, _, err := sf.buildParts(expr)
	if err != nil {
		t.Fatalf("valid expr should build, got error: %v", err)
	}
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d: %v", len(parts), parts)
	}
	joined := strings.Join(parts, " ")
	if want := "JSONExtractString(preferences, 'daily_email')"; !strings.Contains(joined, want) {
		t.Errorf("expected JSON expression %q in %q", want, joined)
	}
	if want := "name = ?"; !strings.Contains(joined, want) {
		t.Errorf("expected plain condition %q in %q", want, joined)
	}
}
