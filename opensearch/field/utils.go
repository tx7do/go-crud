package field

import (
	"regexp"
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
var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// NormalizePaths 将字段路径标准化：每一段都必须通过标识符白名单，
// 含 SQL 元字符/非法 UTF-8 的路径整条置空（调用方跳过空串）。
// 注意：此处只校验不加反引号——mongodb/opensearch/influxdb 的投影字段是
// 字符串 key（非 SQL 标识符），此前加反引号导致 fieldNameRegexp 全部拒绝，
// FieldMask 静默失效（全字段返回）。保留 "*" 供 InfluxDB 的全选语义。
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
			parts[j] = p
		}
		if !valid {
			res[i] = ""
			continue
		}
		res[i] = strings.Join(parts, ".")
	}
	return res
}
