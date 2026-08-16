package pagination

import (
	"strings"
	"testing"

	"entgo.io/ent/dialect/sql"
)

func buildPagingSQL(t *testing.T, offset, limit int) string {
	t.Helper()
	sel := NewOffsetPaginator().BuildSelector(offset, limit)
	s := sql.Select("*").From(sql.Table("users"))
	sel(s)
	q, _ := s.Query()
	return q
}

// TestOffsetPaginator_ArgumentOrder 验证 OFFSET/LIMIT 参数不再互换
// （此前 WithLimit(offset).WithOffset(limit) 导致 offset 分页全部返回错误页窗口）。
func TestOffsetPaginator_ArgumentOrder(t *testing.T) {
	q := buildPagingSQL(t, 30, 20)
	if !strings.Contains(q, "LIMIT 20") {
		t.Errorf("expected LIMIT 20, got %q", q)
	}
	if !strings.Contains(q, "OFFSET 30") {
		t.Errorf("expected OFFSET 30, got %q", q)
	}
}

// TestOffsetPaginator_ZeroOffsetNoPagingStyle 验证 no_paging 兜底调用形态
// BuildSelector(0, N) 产生 OFFSET 0 LIMIT N（而非修复前的 OFFSET N LIMIT 1）。
func TestOffsetPaginator_ZeroOffsetNoPagingStyle(t *testing.T) {
	q := buildPagingSQL(t, 0, 100)
	if !strings.Contains(q, "LIMIT 100") {
		t.Errorf("expected LIMIT 100, got %q", q)
	}
	if strings.Contains(q, "OFFSET 100") {
		t.Errorf("offset must not be swapped into OFFSET 100, got %q", q)
	}
}
