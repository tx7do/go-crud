package field

import "testing"

// TestNormalizePaths_ValidAndInvalid 验证 FieldMask 路径逐段过标识符白名单：
// 合法路径（含点分隔）原样保留，含元字符/非法 UTF-8 的路径整条置空
// （此前反引号包裹导致投影/字段选择全部被拒，FieldMask 静默失效）。
func TestNormalizePaths_ValidAndInvalid(t *testing.T) {
	out := NormalizePaths([]string{"name", "user.age", "id) OR (1=1 --", "a`,b", "id\xffd' OR '1'='1"})
	if out[0] != "name" || out[1] != "user.age" {
		t.Errorf("valid paths must pass through, got %v", out)
	}
	if out[2] != "" || out[3] != "" || out[4] != "" {
		t.Errorf("hostile paths must be dropped, got %v", out)
	}
}
