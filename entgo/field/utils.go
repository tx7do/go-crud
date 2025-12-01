package field

import (
	"github.com/tx7do/go-utils/stringcase"
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

// NormalizePaths normalizes a slice of paths to snake_case
func NormalizePaths(paths []string) []string {
	if len(paths) == 0 {
		return paths
	}

	for i, field := range paths {
		if field == "id_" || field == "_id" {
			field = "id"
		}
		paths[i] = stringcase.ToSnakeCase(field)
	}

	return paths
}

// ApplyFieldMaskSelect 将 fieldmask 转换为 snake_case 并通过 apply 回调传入。
// - apply: 接受归一化字段并调用，例如: func(ps ...string) { builder.Select(ps...) }
// - mask: 传入的 FieldMask，nil 或 空时不做任何操作
func ApplyFieldMaskSelect(apply func(...string), mask *fieldmaskpb.FieldMask) {
	if apply == nil || mask == nil || len(mask.GetPaths()) == 0 {
		return
	}

	NormalizeFieldMaskPaths(mask)

	if len(mask.GetPaths()) > 0 {
		apply(mask.GetPaths()...)
	}
}

// ApplyFieldMaskToBuilder 接受一个带 Select(...string) 方法的 builder 和 FieldMask，
// 将 paths 归一化为 snake_case（并将 id_/_id 归为 id），然后调用 builder.Select(paths...) 并返回 builder。
// - R 是 Select 方法的返回类型（例如 *ent.UserSelect）
// - B 是拥有 Select(...string) R 方法的类型（例如 *ent.UserQuery）
// 返回 (R, bool): bool 表示是否实际调用了 Select（即 mask 非空）。
func ApplyFieldMaskToBuilder[R any, B interface{ Select(fields ...string) R }](builder B, mask *fieldmaskpb.FieldMask) (R, bool) {
	var zero R
	if mask == nil || len(mask.GetPaths()) == 0 {
		return zero, false
	}

	NormalizeFieldMaskPaths(mask)

	if len(mask.GetPaths()) == 0 {
		return zero, false
	}

	return builder.Select(mask.GetPaths()...), true
}
