package rule

import (
	"context"
	"fmt"

	"entgo.io/ent"
	"entgo.io/ent/entql"
	"entgo.io/ent/privacy"

	"github.com/tx7do/go-crud/viewer"
)

type (
	// Filter is the interface that wraps the Where function
	// for filtering nodes in queries and mutations.
	Filter interface {
		// Where applies a filter on the executed query/mutation.
		Where(entql.P)
	}
)

// DenyIfNoViewer is a rule that denies the operation if there is no viewer in the context.
func DenyIfNoViewer() privacy.QueryMutationRule {
	return privacy.ContextQueryMutationRule(func(ctx context.Context) error {
		_, exist := viewer.FromContext(ctx)
		if !exist {
			return privacy.Denyf("viewer-context is missing")
		}
		// Skip to the next privacy rule (equivalent to returning nil).
		return privacy.Skip
	})
}

// AllowIfAdmin is a rule that returns Allow decision if the viewer is admin.
func AllowIfAdmin() privacy.QueryMutationRule {
	return privacy.ContextQueryMutationRule(func(ctx context.Context) error {
		vc, exist := viewer.FromContext(ctx)
		if !exist {
			return privacy.Skip
		}
		if vc.IsPlatformContext() || vc.IsSystemContext() {
			return privacy.Allow
		}
		// Skip to the next privacy rule (equivalent to returning nil).
		return privacy.Skip
	})
}

// TenantFilterRule 是一个通用的租户过滤规则，用于在查询时注入租户过滤条件。
// 该规则会根据当前 ViewerContext 中的租户信息，动态添加租户过滤谓词，确保数据隔离和安全性。
// 适用于包含 tenant_id 字段的实体查询。
func TenantFilterRule(ctx context.Context, f Filter) error {
	vc, exist := viewer.FromContext(ctx)
	// 如果身份丢失，安全起见应直接拒绝操作（Deny），而不是跳过
	if !exist {
		return fmt.Errorf("security: missing ViewerContext in context")
	}

	// 平台管理视图/系统视图放行：允许查看全量数据
	if vc.IsPlatformContext() || vc.IsSystemContext() {
		return nil // 在 Privacy 逻辑中 nil 相当于 Skip
	}

	tid := vc.TenantID()

	// 注入租户过滤谓词
	f.Where(
		entql.Uint64EQ(tid).Field("tenant_id"),
	)

	return nil
}

func OwnerOnlyRule(ctx context.Context, f Filter) error {
	vc, exist := viewer.FromContext(ctx)
	// 如果身份丢失，安全起见应直接拒绝操作（Deny），而不是跳过
	if !exist {
		return fmt.Errorf("security: missing ViewerContext in context")
	}

	// 平台管理视图/系统视图放行：允许查看全量数据
	if vc.IsPlatformContext() || vc.IsSystemContext() {
		return nil // 在 Privacy 逻辑中 nil 相当于 Skip
	}

	uid := vc.UserID()

	// 注入归属者过滤谓词
	f.Where(
		entql.Or(
			entql.Uint64EQ(uid).Field("created_by"),
			entql.Uint32EQ(uint32(uid)).Field("created_by"),
		),
	)

	return nil
}

// PermissionRule 是一个通用的数据权限过滤规则，用于在查询时注入基于数据权限范围的过滤条件。
// 该规则会根据当前 ViewerContext 中的数据权限信息，动态添加过滤谓词，确保数据访问符合权限要求。
// 适用于包含 org_unit_id 和 created_by 字段的实体查询。
func PermissionRule(ctx context.Context, f Filter) error {
	vc, exist := viewer.FromContext(ctx)
	// 如果身份丢失，安全起见应直接拒绝操作（Deny），而不是跳过
	if !exist {
		return fmt.Errorf("security: missing ViewerContext in context")
	}

	// 平台管理视图/系统视图放行：允许查看全量数据
	if vc.IsPlatformContext() || vc.IsSystemContext() {
		return nil // 在 Privacy 逻辑中 nil 相当于 Skip
	}

	// 获取数据范围列表
	scopes := vc.DataScope()
	if len(scopes) == 0 {
		// 如果没有任何定义的 scope，默认应拒绝访问（安全兜底）
		return fmt.Errorf("security: no data scope defined for current user")
	}

	// 构建并集谓词 (OR 逻辑)
	var predicates []entql.P
	for _, s := range scopes {
		switch s.ScopeType {
		case viewer.ScopeTypeAll:
			return nil // 只要有一个 scope 是 All，直接放行

		case viewer.ScopeTypeSelf:
			uid := vc.UserID()
			// 仅限本人：匹配 created_by 字段
			predicates = append(predicates, entql.Or(
				entql.Uint64EQ(uid).Field("created_by"),
				entql.Uint32EQ(uint32(uid)).Field("created_by"),
			))

		case viewer.ScopeTypeUnit:
			// 匹配组织单元 ID
			for _, id := range s.TargetIDs {
				predicates = append(predicates, entql.Or(
					entql.Uint64EQ(id).Field("org_unit_id"),
					entql.Uint32EQ(uint32(id)).Field("org_unit_id"),
				))
			}

		case viewer.ScopeTypeUser:
			// 匹配指定用户 ID
			for _, id := range s.TargetIDs {
				predicates = append(predicates, entql.Or(
					entql.Uint64EQ(id).Field("created_by"),
					entql.Uint32EQ(uint32(id)).Field("created_by"),
				))
			}

		case viewer.ScopeTypeNone:
			// 显式禁止访问：如果命中 None，直接返回错误或在该 Case 下清空谓词并 Deny
			return fmt.Errorf("security: data access is explicitly denied by policy")

		default:
			// 未知的 scope 类型，忽略处理
			continue
		}
	}

	// 5. 将所有 scope 逻辑通过 OR 连接，注入 Where 子句
	if len(predicates) > 0 {
		p := predicates[0]
		for i := 1; i < len(predicates); i++ {
			p = entql.Or(p, predicates[i])
		}
		f.Where(p)
	} else {
		// 有 scope 但没解析出有效谓词，防御性拒绝
		return fmt.Errorf("security: invalid data scope configuration")
	}

	return nil
}

// SoftDeleteRule 注入软删除过滤规则，隐藏已软删除的数据记录
func SoftDeleteRule(ctx context.Context, f Filter) error {
	vc, exist := viewer.FromContext(ctx)
	// 如果身份丢失，安全起见应直接拒绝操作（Deny），而不是跳过
	if !exist {
		return fmt.Errorf("security: missing ViewerContext in context")
	}

	// 平台管理视图/系统视图放行：允许查看全量数据
	if vc.IsPlatformContext() || vc.IsSystemContext() {
		return nil // 在 Privacy 逻辑中 nil 相当于 Skip
	}

	// 注入软删除过滤谓词
	f.Where(
		entql.FieldNil("deleted_at"),
	)

	return nil
}

// DenyFieldsMutationRule 彻底解耦版：不依赖任何 ent 相关包
// 参数说明:
// ctx: 上下文
// m: 传入的对象，在 Mutation 阶段它是具体的 Mutation 实例
// fields: 需要保护（禁止修改）的字段名
func DenyFieldsMutationRule(ctx context.Context, m ent.Mutation, fields ...string) error {
	// 获取 Viewer
	vc, ok := viewer.FromContext(ctx)
	if !ok || vc == nil {
		return fmt.Errorf("security: missing viewer context")
	}

	// 平台管理视图/系统视图放行
	if vc.IsPlatformContext() || vc.IsSystemContext() {
		return nil
	}

	for _, fieldName := range fields {
		// 检查字段是否被 Set (赋值)
		if _, set := m.Field(fieldName); set {
			return fmt.Errorf("security: field '%s' is read-only for current user", fieldName)
		}
		// 检查字段是否被 Clear (针对可选字段的置空操作)
		if m.FieldCleared(fieldName) {
			return fmt.Errorf("security: field '%s' cannot be cleared by current user", fieldName)
		}
	}

	// 如果 f 不是 Mutation (例如是 Query)，或者通过了校验，则跳过
	return nil
}

// LimitFieldAccessRule 限制特定字段的访问修改权限
// 参数说明:
// ctx: 上下文
// m: 传入的对象，在 Mutation 阶段它是具体的 Mutation 实例
// field: 需要保护的字段名
// requiredPermission: 修改该字段所需的权限标识
func LimitFieldAccessRule(ctx context.Context, m ent.Mutation, field string, requiredPermission string) error {
	vc, ok := viewer.FromContext(ctx)
	if !ok || vc == nil {
		return fmt.Errorf("security: missing viewer context")
	}

	// 平台管理视图/系统视图放行
	if vc.IsPlatformContext() || vc.IsSystemContext() {
		return nil
	}

	// 检查是否拥有修改该字段的权限
	if !vc.HasPermission(requiredPermission, m.Type()) {
		_, isSet := m.Field(field)
		isCleared := m.FieldCleared(field)

		if isSet || isCleared {
			return fmt.Errorf("security: insufficient permission '%s' to modify field '%s' on %s",
				requiredPermission, field, m.Type())
		}
	}

	return nil
}
