package viewer

import "context"

// Context 定义当前访问者（Viewer）上下文接口，适用于查询和更新等操作。
// 增加了权限相关方法，便于在 Hook/Policy 中判断是否允许更新/删除等操作。
type Context interface {
	// UserID 返回当前用户ID
	UserID() uint64

	// TenantID 返回租户ID
	TenantID() uint64

	// OrgUnitID 返回当前身份挂载的组织单元 ID
	OrgUnitID() uint64

	// Permissions 返回当前 Viewer 的权限列表（可用于细粒度判断）
	Permissions() []string

	// Roles 返回当前 Viewer 的角色列表（可选，用于审计或策略）
	Roles() []string

	// DataScope 返回当前身份的数据权限范围（用于 SQL 拼接）
	DataScope() []DataScope

	// TraceID 返回当前请求的 Trace ID（用于日志跟踪）
	TraceID() string

	// HasPermission 判断是否具有某个动作/资源的权限（如 "update:user"）
	HasPermission(action, resource string) bool

	// IsPlatformContext 当前是否处于平台管理视图（tenant_id == 0）
	IsPlatformContext() bool

	// IsTenantContext 当前是否处于租户业务视图（tenant_id > 0）
	IsTenantContext() bool

	// IsSystemContext 判断是否为系统后台任务
	IsSystemContext() bool

	// ShouldAudit 返回是否需要记录审计日志（便于在中间件/Hook 中快速判断）
	ShouldAudit() bool
}

type contextKey struct{}

// WithContext 将 Context 注入 context
func WithContext(ctx context.Context, vc Context) context.Context {
	return context.WithValue(ctx, contextKey{}, vc)
}

// FromContext 从 context 中提取 Context
func FromContext(ctx context.Context) (Context, bool) {
	v := ctx.Value(contextKey{})
	vc, ok := v.(Context)
	return vc, ok
}

// MustFromContext 从 context 中提取 Context，若不存在则返回一个默认的 NoopContext
func MustFromContext(ctx context.Context) Context {
	if ctx == nil {
		return NewNoopContext()
	}
	if v := ctx.Value(contextKey{}); v != nil {
		if vc, ok := v.(Context); ok && vc != nil {
			return vc
		}
	}
	return NewNoopContext()
}
