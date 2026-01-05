package mixin

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/mixin"
)

var pathRe = regexp.MustCompile(`^/(?:\d+/)*$`)

// 确保 TreePath 实现了 ent.Mixin 接口
var _ ent.Mixin = (*TreePath)(nil)

type TreePath struct {
	mixin.Schema
}

func (TreePath) Fields() []ent.Field {
	return []ent.Field{
		field.String("path").
			Comment(`树路径，规范：
- 根节点: "/"
- 非根节点: "/1/2/3/"（以 "/" 开头且以 "/" 结尾）
- 禁止空字符串（NULL 表示未设置）
- 示例: "/", "/101/", "/101/202/303/"`).
			MaxLen(1024).
			Nillable().
			//Immutable().
			Validate(func(s string) error {
				// NULL 会被 Nillable 处理，空字符串视为未设置
				if s == "" {
					return nil
				}
				if !pathRe.MatchString(s) {
					return fmt.Errorf("path must be in format: '/', '/1/', '/1/2/'")
				}
				return nil
			}),
	}
}

func (TreePath) Hooks() []ent.Hook {
	return []ent.Hook{
		// 阻止无效路径变更
		func(next ent.Mutator) ent.Mutator {
			return ent.MutateFunc(func(ctx context.Context, m ent.Mutation) (ent.Value, error) {
				if v, ok := m.Field("path"); ok {
					if p, ok2 := v.(string); ok2 && p != "" {
						if !strings.HasSuffix(p, "/") {
							return nil, fmt.Errorf("path must end with '/' (e.g., '/1/2/3/')")
						}
						if len(p) > 1 && !strings.HasPrefix(p, "/") {
							return nil, fmt.Errorf("path must start with '/'")
						}
					}
				}
				return next.Mutate(ctx, m)
			})
		},
	}
}
