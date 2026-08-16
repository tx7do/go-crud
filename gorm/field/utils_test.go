package field

import (
	"testing"
)

// TestNormalizePaths_RejectsMetacharPaths 验证含元字符的 FieldMask 路径
// 被白名单整条置空（F-2：与 entgo/mongodb/opensearch/influxdb/clickhouse/
// doris 六模块对齐）。此前 gorm 仅靠反引号转义，现补 identifierPattern。
func TestNormalizePaths_RejectsMetacharPaths(t *testing.T) {
	out := NormalizePaths([]string{"a`,(select version()),`b", "id) OR (1=1", "col; DROP", "1abc", "a-b"})
	for i, v := range out {
		if v != "" {
			t.Errorf("metachar path %d must be dropped, got %q", i, v)
		}
	}
}

// TestNormalizePaths_ValidPathsUnchanged 验证正常路径行为不变（合法标识符
// 段仍反引号包裹，* 保留）。
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
