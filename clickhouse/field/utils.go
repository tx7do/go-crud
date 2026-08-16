package field

import (
	"regexp"
	"strings"

	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// identifierPattern 单段标识符白名单：字母/下划线开头，仅含字母、数字、下划线。
// FieldMask 路径来自客户端请求，任何一段不合法即整条路径作废（置空），
// 防止注入载荷进入 SELECT/UPDATE 列，也避免下游 Builder.Select 对
// 点路径/转义路径 panic（客户端可触发的 DoS）。
var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// NormalizeFieldMaskPaths normalizes the paths in the given FieldMask to snake_case
func NormalizeFieldMaskPaths(fm *fieldmaskpb.FieldMask) {
	if fm == nil || len(fm.GetPaths()) == 0 {
		return
	}

	fm.Normalize()

	fm.Paths = NormalizePaths(fm.Paths)
}

// NormalizePaths 将字段路径标准化：合法标识符段加反引号并转义内部反引号（防注入），
// 含非法字符的路径整条置空（调用方应跳过空串），保留 *。
func NormalizePaths(fields []string) []string {
	res := make([]string, len(fields))
	for i, f := range fields {
		f = strings.TrimSpace(f)
		if f == "" {
			res[i] = f
			continue
		}
		parts := strings.Split(f, ".")
		valid := true
		// 下游 Builder.Select 仅接受单个简单标识符（`name`），多段路径即使
		// 每段合法也会触发 Select 的 panic——直接整条丢弃。
		if len(parts) > 1 {
			valid = false
			parts = nil
		}
		for j, p := range parts {
			p = strings.TrimSpace(p)
			if p == "*" {
				parts[j] = p
				continue
			}
			if !identifierPattern.MatchString(p) {
				valid = false
				break
			}
			// 内部反引号必须成对转义，否则 `a`,(select version()),`b`
			// 这类输入可注入子查询。
			parts[j] = "`" + strings.ReplaceAll(p, "`", "``") + "`"
		}
		if !valid {
			res[i] = ""
			continue
		}
		res[i] = strings.Join(parts, ".")
	}
	return res
}

// MaskSet 将（已归一化的）FieldMask 路径转为集合，剥离包裹标识符的反引号，
// 便于与结构体字段名/列名做原始匹配（此前反引号包裹的路径与原始列名永不匹配，
// 导致掩码更新/查询静默失效）。
func MaskSet(fm *fieldmaskpb.FieldMask) map[string]bool {
	mask := map[string]bool{}
	if fm == nil {
		return mask
	}
	for _, p := range fm.Paths {
		parts := strings.Split(p, ".")
		for i, part := range parts {
			parts[i] = strings.Trim(part, "`")
		}
		mask[strings.Join(parts, ".")] = true
	}
	return mask
}
