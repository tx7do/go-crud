package pagination

import (
	"strings"
	"testing"

	"github.com/tx7do/go-crud/influxdb/query"
)

// TestOffsetPaginator_LimitActuallyApplied 验证 LIMIT/OFFSET 真正落到 builder 上。
// 此前分页器用运行时断言探测 SetLimit(int64)/SetSkip(int64)，而 Builder 只有
// Limit(int)/Offset(int)，断言永不命中——LIMIT 从未生效。
func TestOffsetPaginator_LimitActuallyApplied(t *testing.T) {
	b := query.NewQueryBuilder("m")
	if b == nil {
		t.Fatal("NewQueryBuilder returned nil")
	}
	NewOffsetPaginator().BuildClause(b, 30, 20)

	sqlStr := b.Build()
	if !strings.Contains(sqlStr, "LIMIT 20") {
		t.Errorf("expected LIMIT 20 in query, got %q", sqlStr)
	}
	if !strings.Contains(sqlStr, "OFFSET 30") {
		t.Errorf("expected OFFSET 30 in query, got %q", sqlStr)
	}
}

// TestPagePaginator_LimitActuallyApplied 页码模式：page=2,size=15 → OFFSET 15 LIMIT 15。
func TestPagePaginator_LimitActuallyApplied(t *testing.T) {
	b := query.NewQueryBuilder("m")
	NewPagePaginator().BuildClause(b, 2, 15)

	sqlStr := b.Build()
	if !strings.Contains(sqlStr, "LIMIT 15") {
		t.Errorf("expected LIMIT 15 in query, got %q", sqlStr)
	}
	if !strings.Contains(sqlStr, "OFFSET 15") {
		t.Errorf("expected OFFSET 15 in query, got %q", sqlStr)
	}
}

// TestTokenPaginator_SizeApplied token 模式（WithSize 修复后 pageSize 真正生效）。
func TestTokenPaginator_SizeApplied(t *testing.T) {
	b := query.NewQueryBuilder("m")
	NewTokenPaginator().BuildClause(b, "", 25)

	sqlStr := b.Build()
	if !strings.Contains(sqlStr, "LIMIT 25") {
		t.Errorf("expected LIMIT 25 in query, got %q", sqlStr)
	}
}
