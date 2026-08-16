package field

import (
	"strings"
	"testing"

	"entgo.io/ent/dialect/sql"
)

func buildSelectSQL(t *testing.T, table string, fields []string) string {
	t.Helper()
	s := sql.Select("*").From(sql.Table(table))
	NewFieldSelector().BuildSelect(s, fields)
	q, _ := s.Query()
	return q
}

// TestFieldSelector_HostilePathDropped 验证 FieldMask 路径的每一段（此前仅校验
// 第一个点之后的部分，前缀可携带注入载荷）都过硬性标识符校验，敌意路径
// 不会进入 SELECT 列表。
func TestFieldSelector_HostilePathDropped(t *testing.T) {
	hostile := []string{
		"(select version()) -- \xff.name", // H-2 原始攻击链：非法 UTF-8 前缀 + 合法后缀列
		"id) OR (1=1 --",
		"a`.name",
		"name AS (select version())",
		"user name",
		"id; DROP TABLE users",
	}
	for _, path := range hostile {
		for _, table := range []string{"users", "custom_table"} {
			q := buildSelectSQL(t, table, []string{path})
			// 注意：ent 正常输出会用反引号引用表/列名（`users`），
			// 这里只检查注入载荷相关字符与子查询标记。
			for _, d := range []string{"(", ")", "'", ";", "--", " AS ", "version()", "1=1", "DROP TABLE"} {
				if strings.Contains(q, d) {
					t.Errorf("hostile mask path %q leaked %q into SQL on %s: %q", path, d, table, q)
				}
			}
		}
	}
}

// TestFieldSelector_ValidPathStillWorks 验证合法路径不受影响。
// 注：entgo 的 NormalizePaths 会把 "user.name" 拍平为 "user_name"（既有行为），
// 此处仅断言合法输入仍产生正常 SELECT 且无注入残留。
func TestFieldSelector_ValidPathStillWorks(t *testing.T) {
	q := buildSelectSQL(t, "users", []string{"name"})
	if !strings.Contains(q, "name") {
		t.Fatalf("expected column name in SELECT, got %q", q)
	}
	// 带点路径：每段均为标识符 → 通过校验（后缀列校验 + 前缀硬性校验），
	// 最终按既有行为拍平为单列名
	q2 := buildSelectSQL(t, "users", []string{"user.name"})
	if strings.ContainsAny(q2, "();'") || strings.Contains(q2, " AS ") {
		t.Fatalf("valid dotted path must not introduce metacharacters, got %q", q2)
	}
	if !strings.Contains(q2, "user") || !strings.Contains(q2, "name") {
		t.Fatalf("expected flattened column from dotted path, got %q", q2)
	}
}
