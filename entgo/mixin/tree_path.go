package mixin

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/mixin"
	"github.com/tx7do/go-crud/entgo/rule"
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
- 根节点: /
- 非根节点: /1/2/3/（以 / 开头且以 / 结尾）
- 禁止空字符串（NULL 表示未设置）
- 示例: /, /101/, /101/202/303/`).
			MaxLen(1024).
			Optional().
			Nillable(),
		//Validate(func(s string) error {
		//	// NULL 会被 Nillable 处理，空字符串视为未设置
		//	if s == "" {
		//		return nil
		//	}
		//	if !pathRe.MatchString(s) {
		//		return fmt.Errorf("path must be in format: '/', '/1/', '/1/2/'")
		//	}
		//	return nil
		//}),
	}
}

func (TreePath) Hooks() []ent.Hook {
	return []ent.Hook{
		//validatePathHook(),
		//computedPathHook(),
	}
}

// validatePathHook 返回用于校验 path 字段的 Hook
func validatePathHook() ent.Hook {
	return func(next ent.Mutator) ent.Mutator {
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
	}
}

// ensureComputedPath: 当未显式设置 path 时，尝试根据 parent_id 或 parent edge 计算并设置 path。
// 若 mutation 支持 SetField，会调用 setter；否则不做修改（不阻塞）。
func ensureComputedPath(m ent.Mutation) {
	// 如果已经显式设置了 path（且非空），则不覆盖
	if v, ok := m.Field("path"); ok {
		if s, ok2 := v.(string); ok2 && s != "" {
			return
		}
	}

	type clientGetter interface {
		Client() any
	}

	_, ok := rule.GetClientFromMutation(m)
	if !ok {
		fmt.Println("Mutation does not implement Client()")
		return
	}

	type ParentQuerier interface {
		ParentID() (id uint32, exists bool)
	}
	if m.Op().Is(ent.OpCreate) {
		if pq, ok := m.(ParentQuerier); ok {
			pid, exists := pq.ParentID()
			fmt.Printf("ParentID from ParentQuerier: %d, exists: %v\n", pid, exists)
		}
	}

	// 尝试从 parent_id 字段或 edge parent 提取 parent id
	var parentID uint64
	if v, ok := m.Field("parent_id"); ok {
		switch id := v.(type) {
		case int:
			parentID = uint64(id)
		case int32:
			parentID = uint64(id)
		case int64:
			parentID = uint64(id)
		case uint:
			parentID = uint64(id)
		case uint32:
			parentID = uint64(id)
		case uint64:
			parentID = id
		}
	} else {
		// 兼容生成的 mutation 提供 EdgeIDs 方法
		if ei, ok := m.(interface{ EdgeIDs(string) []int }); ok {
			ids := ei.EdgeIDs("parent")
			if len(ids) > 0 {
				parentID = uint64(ids[0])
			}
		}
	}

	// 构造简单 path：root 或 "/{parentID}/"
	var computed string
	if parentID == 0 {
		computed = "/"
	} else {
		computed = fmt.Sprintf("/%d/", parentID)
	}

	// 尝试通过反射调用 SetField 方法，避免接口字面量断言引起的歧义
	rv := reflect.ValueOf(m)
	if mf := rv.MethodByName("SetField"); mf.IsValid() {
		if mf.Type().Kind() == reflect.Func && mf.Type().NumIn() == 2 {
			// 检查第一个参数是否为 string（常见签名为 SetField(string, any/interface{})）
			if mf.Type().In(0).Kind() == reflect.String {
				// 调用 SetField("path", computed)
				mf.Call([]reflect.Value{reflect.ValueOf("path"), reflect.ValueOf(computed)})
			}
		}
	}
}

// computedPathHook 返回用于在创建/更新前自动注入 path 的 Hook
func computedPathHook() ent.Hook {
	return func(next ent.Mutator) ent.Mutator {
		return ent.MutateFunc(func(ctx context.Context, m ent.Mutation) (ent.Value, error) {
			// 在下游 Mutator 之前尝试设置 path（若未显式设置）
			ensureComputedPath(m)
			return next.Mutate(ctx, m)
		})
	}
}
