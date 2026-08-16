package query

import "testing"

// TestLimitKeywordPattern 验证 LIMIT 关键字词边界匹配：
// 此前用 strings.Contains(upper,"LIMIT") 会把 rate_limit 列名误判成已含 LIMIT。
func TestLimitKeywordPattern(t *testing.T) {
	matched := []string{"SELECT * FROM t LIMIT 1", "select x limit 5", "SELECT * FROM t WHERE a=1 Limit 10"}
	for _, c := range matched {
		if !LimitKeywordPattern.MatchString(c) {
			t.Errorf("should match LIMIT keyword: %q", c)
		}
	}
	notMatched := []string{"SELECT rate_limit FROM t", "SELECT x_limit_settings", "SELECT * FROM t WHERE a=1", "ratelimit", "LIMITx"}
	for _, c := range notMatched {
		if LimitKeywordPattern.MatchString(c) {
			t.Errorf("should NOT match column name containing 'limit': %q", c)
		}
	}
}
