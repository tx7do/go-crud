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

// TestNormalizePaths_Idempotent 验证归一化幂等：已归一化（反引号包裹）的合法
// 标识符再次归一化保持不变，因此 Repository.Get 先 NormalizeFieldMaskPaths
// 再经 BuildSelector 二次归一化不会把路径判为非法。
func TestNormalizePaths_Idempotent(t *testing.T) {
	raw := []string{"id", "name", "user.name", "*", "a`,(select version()),`b"}
	once := NormalizePaths(raw)
	twice := NormalizePaths(once)

	for i := range once {
		if once[i] != twice[i] {
			t.Errorf("re-normalizing %q changed it to %q (want idempotent)", once[i], twice[i])
		}
	}
	if once[0] != "`id`" || once[1] != "`name`" {
		t.Errorf("valid paths must be backtick-wrapped, got %q and %q", once[0], once[1])
	}

	// an already-normalized FieldMask survives a second NormalizeFieldMaskPaths
	fm := &fieldmaskpb.FieldMask{Paths: []string{"id", "name"}}
	NormalizeFieldMaskPaths(fm)
	first := append([]string(nil), fm.Paths...)
	NormalizeFieldMaskPaths(fm)
	for i := range first {
		if first[i] != fm.Paths[i] {
			t.Errorf("second NormalizeFieldMaskPaths changed %q to %q", first[i], fm.Paths[i])
		}
	}

	// a backtick-wrapped invalid payload is still rejected
	if out := NormalizePaths([]string{"`a`,(select version()),`b`"}); out[0] != "" {
		t.Errorf("quoted injection payload must be dropped, got %q", out[0])
	}
}
