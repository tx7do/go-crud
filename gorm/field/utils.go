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

// identifierPattern 是标识符白名单：字母/下划线开头，后随字母数字下划线。
// 与 entgo/mongodb/opensearch/influxdb/clickhouse/doris 六模块的
// NormalizePaths 对齐（F-2：此前 gorm 唯一缺此校验，仅靠反引号转义）。
var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// NormalizePaths 将字段路径标准化：每一段先过标识符白名单（非白名单段整条
// 路径置空），再为段添加反引号并转义内部反引号（防注入），保留 *。
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
			if p == "*" || p == "" {
				parts[j] = p
				continue
			}
			// 白名单校验：非标识符段整条路径置空（调用方跳过）。
			if !identifierPattern.MatchString(p) {
				valid = false
				break
			}
			// FieldMask 路径来自客户端请求，内部反引号必须成对转义，
			// 否则 `a`,(select version()),`b` 这类输入可注入子查询。
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
