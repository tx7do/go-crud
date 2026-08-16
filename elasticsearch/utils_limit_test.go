package elasticsearch

import "testing"

// TestQueryKeyPattern_RejectsLeadingDash 验证 - 前缀键被拒
// （query_string 中 -field:val 是 NOT 语义，可翻转过滤实现过度返回）。
func TestQueryKeyPattern_RejectsLeadingDash(t *testing.T) {
	reject := []string{"-status", "-", "-user_id", "-a-b"}
	for _, k := range reject {
		if queryKeyPattern.MatchString(k) {
			t.Errorf("key %q must be rejected", k)
		}
	}
	accept := []string{"status", "user_id", "user-id", "a.b", "field2"}
	for _, k := range accept {
		if !queryKeyPattern.MatchString(k) {
			t.Errorf("key %q must be accepted", k)
		}
	}
}
