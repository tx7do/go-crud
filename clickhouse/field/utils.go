package field

import (
	"strings"

	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// NormalizeFieldMaskPaths normalizes the paths in the given FieldMask to snake_case
func NormalizeFieldMaskPaths(fm *fieldmaskpb.FieldMask) {
	if fm == nil || len(fm.GetPaths()) == 0 {
		return
	}

	fm.Normalize()

	fm.Paths = NormalizePaths(fm.Paths)
}

// NormalizePaths 将字段路径标准化：为标识符添加反引号并转义内部反引号（防注入），保留 *。
func NormalizePaths(fields []string) []string {
	res := make([]string, len(fields))
	for i, f := range fields {
		f = strings.TrimSpace(f)
		if f == "" {
			res[i] = f
			continue
		}
		parts := strings.Split(f, ".")
		for j, p := range parts {
			p = strings.TrimSpace(p)
			if p == "*" || p == "" {
				parts[j] = p
			} else {
				// FieldMask 路径来自客户端请求，内部反引号必须成对转义，
				// 否则 `a`,(select version()),`b` 这类输入可注入子查询。
				parts[j] = "`" + strings.ReplaceAll(p, "`", "``") + "`"
			}
		}
		res[i] = strings.Join(parts, ".")
	}
	return res
}
