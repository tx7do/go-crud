package field

import (
	"fmt"
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
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

// TestBuildSelect_SkipsEmptyPathsAfterNormalize 验证 B 回归修复：合法+非法
// 字段混排时，NormalizePaths 将非法段置空，BuildSelect 跳过空项后仅保留
// 合法项，与仅传合法项的结果一致（无逗号残缺/非法片段泄漏）。与
// clickhouse/doris/entgo/influxdb/mongodb/opensearch 六模块的
// field_selector 对齐。
func TestBuildSelect_SkipsEmptyPathsAfterNormalize(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{DryRun: true})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	fs := NewFieldSelector()
	// 基线：仅两个合法项
	baseline := fs.BuildSelect(db.Session(&gorm.Session{DryRun: true}), []string{"name", "id"})
	// 混排：合法 "name"、"id" 与非法 "col; DROP"、"1abc"
	mixed := fs.BuildSelect(db.Session(&gorm.Session{DryRun: true}), []string{"name", "col; DROP", "id", "1abc"})
	// 混排过滤后应与基线一致：仅两个合法列，无非法片段
	bs := fmt.Sprintf("%v", baseline.Statement.Selects)
	ms := fmt.Sprintf("%v", mixed.Statement.Selects)
	if bs != ms {
		t.Errorf("mixed (post-skip) must equal baseline:\n  baseline=%s\n  mixed=%s", bs, ms)
	}
	if !strings.Contains(ms, "`name`") || !strings.Contains(ms, "`id`") {
		t.Errorf("valid columns must remain, got %s", ms)
	}
	if strings.Contains(ms, "DROP") || strings.Contains(ms, "1abc") {
		t.Errorf("invalid path leaked into selects: %s", ms)
	}
}

// TestBuildSelect_AllEmptyReturnsUnmodified 全部字段非法时跳过空项后列表
// 为空，BuildSelect 返回未修改的 db（不调用 Select，Statement.Selects 为空）。
func TestBuildSelect_AllEmptyReturnsUnmodified(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{DryRun: true})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	fs := NewFieldSelector()
	after := fs.BuildSelect(db.Session(&gorm.Session{DryRun: true}), []string{"col; DROP", "1abc", "a-b"})
	// 全空时 BuildSelect 短路返回 db，不调用 Select：Selects 表示为空
	s := fmt.Sprintf("%v", after.Statement.Selects)
	if s != "[]" {
		t.Errorf("all-invalid input must leave Selects empty, got %s", s)
	}
}
