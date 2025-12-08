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

// NormalizePaths 将字段路径标准化（简单地为标识符添加反引号，保留 *）。
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
				parts[j] = "`" + p + "`"
			}
		}
		res[i] = strings.Join(parts, ".")
	}
	return res
}
