package field

import (
	"testing"

	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// TestNormalizePaths_InvalidPathDropped 验证含 SQL 元字符/点路径的 FieldMask
// 路径整条置空（防注入与防 Select panic），合法路径正常转义。
func TestNormalizePaths_InvalidPathDropped(t *testing.T) {
	out := NormalizePaths([]string{
		"a`,(select version()),`b", // 注入载荷
		"user.name",                // 点路径（下游 Select 不支持，置空防 panic）
		"name",                     // 合法
		"*",                        // 通配符保留
		"id\xffd' OR '1'='1",       // 非法 UTF-8 载荷
	})
	if out[0] != "" {
		t.Errorf("injection payload path must be dropped, got %q", out[0])
	}
	if out[1] != "" {
		t.Errorf("dotted path must be dropped (downstream Select only accepts simple idents), got %q", out[1])
	}
	if out[2] != "`name`" {
		t.Errorf("valid path: got %q, want %q", out[2], "`name`")
	}
	if out[3] != "" {
		t.Errorf("star path must be dropped (star would panic in Builder.Select), got %q", out[3])
	}
	if out[4] != "" {
		t.Errorf("invalid-UTF-8 payload must be dropped, got %q", out[4])
	}
}

// TestMaskSet_UnwrapsBackticks 验证 MaskSet 剥离反引号，
// 掩码集合能与原始列名匹配（修复掩码更新静默失效）。
func TestMaskSet_UnwrapsBackticks(t *testing.T) {
	fm := &fieldmaskpb.FieldMask{Paths: []string{"`name`", "`update_time`"}}
	mask := MaskSet(fm)
	if !mask["name"] || !mask["update_time"] {
		t.Errorf("MaskSet must unwrap backticked paths, got %v", mask)
	}

	// 未归一化的原始路径同样可用
	mask2 := MaskSet(&fieldmaskpb.FieldMask{Paths: []string{"status"}})
	if !mask2["status"] {
		t.Errorf("raw path must be kept, got %v", mask2)
	}

	if got := MaskSet(nil); len(got) != 0 {
		t.Errorf("nil mask must be empty, got %v", got)
	}
}
