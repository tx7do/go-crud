package field

import (
	"testing"
)

// TestNormalizePaths_EscapesBackticks 验证 FieldMask 路径中的反引号会被转义，
// `a`,(select version()),`b` 这类注入载荷成为单个无害的带引号标识符。
func TestNormalizePaths_EscapesBackticks(t *testing.T) {
	out := NormalizePaths([]string{"a`,(select version()),`b"})
	want := "`a``,(select version()),``b`"
	if out[0] != want {
		t.Errorf("NormalizePaths() = %q, want %q", out[0], want)
	}
}

// TestNormalizePaths_ValidPathsUnchanged 验证正常路径行为不变。
func TestNormalizePaths_ValidPathsUnchanged(t *testing.T) {
	out := NormalizePaths([]string{"name", "user.name", "*"})
	if out[0] != "`name`" {
		t.Errorf("plain field: got %q, want %q", out[0], "`name`")
	}
	if out[1] != "`user`.`name`" {
		t.Errorf("dotted field: got %q, want %q", out[1], "`user`.`name`")
	}
	if out[2] != "*" {
		t.Errorf("star: got %q, want %q", out[2], "*")
	}
}
