package elasticsearch

import (
	"strings"
	"testing"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

// TestBuildSearchQuery_Wiring 验证 Search 的 query 真正来自请求
// （此前 ParseQueryString 返回值被丢弃，Search 恒为 match_all）。
func TestBuildSearchQuery_Wiring(t *testing.T) {
	// JSON 对象
	q := BuildSearchQuery(&paginationV1.PagingRequest{
		FilteringType: &paginationV1.PagingRequest_Query{Query: `{"status":"active"}`},
	})
	if q != "status:active" {
		t.Errorf("object query: got %q, want %q", q, "status:active")
	}

	// JSON 对象数组：多条件 AND 连接（顺序无关断言）
	q = BuildSearchQuery(&paginationV1.PagingRequest{
		FilteringType: &paginationV1.PagingRequest_Query{Query: `[{"a":"1"},{"b":"2"}]`},
	})
	if !strings.Contains(q, "a:1") || !strings.Contains(q, "b:2") || !strings.Contains(q, " AND ") {
		t.Errorf("array query: got %q", q)
	}

	// 非法/空输入 → 空串（match_all）
	if q := BuildSearchQuery(&paginationV1.PagingRequest{
		FilteringType: &paginationV1.PagingRequest_Query{Query: "not-json"},
	}); q != "" {
		t.Errorf("invalid query should be empty, got %q", q)
	}
	if q := BuildSearchQuery(&paginationV1.PagingRequest{}); q != "" {
		t.Errorf("empty query should be empty, got %q", q)
	}
	if q := BuildSearchQuery(nil); q != "" {
		t.Errorf("nil request should be empty, got %q", q)
	}
}

// TestClampPageSize 验证 page size 上限与默认值。
func TestClampPageSize(t *testing.T) {
	cases := []struct {
		in   uint32
		want int
	}{
		{0, DefaultPageSize},
		{1, 1},
		{20, 20},
		{10000, 10000},
		{10001, MaxPageSize},
		{4000000000, MaxPageSize},
	}
	for _, c := range cases {
		if got := ClampPageSize(c.in); got != c.want {
			t.Errorf("ClampPageSize(%d) = %d, want %d", c.in, got, c.want)
		}
	}
}
