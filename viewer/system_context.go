package viewer

import "context"

// SystemContext 是系统后台任务（定时任务、服务间调用等非用户发起的请求）
// 所使用的固定语义上下文实现：身份字段全部为零值、不携带任何权限/角色/
// 数据范围、永远处于平台视图且被标记为系统上下文、权限判定一律放行、
// 不产生审计日志。
//
// 配套的 EnforceTenant 对系统上下文放行（不注入租户谓词），系统级维护
// 任务（如全量扫描、归档清理）因此可跨租户执行。
type SystemContext struct{}

// NewSystemContext 构造系统后台任务上下文。
func NewSystemContext() Context {
	return SystemContext{}
}

// WithSystemContext 向 ctx 注入系统后台任务身份。
// 定时任务/服务间调用等无用户身份的执行路径经此获得满足租户隐私规则
// 强制要求的 ViewerContext。
func WithSystemContext(ctx context.Context) context.Context {
	return WithContext(ctx, NewSystemContext())
}

func (v SystemContext) UserID() uint64 {
	return 0
}

func (v SystemContext) TenantID() uint64 {
	return 0
}

func (v SystemContext) OrgUnitID() uint64 {
	return 0
}

func (v SystemContext) Permissions() []string {
	return []string{}
}

func (v SystemContext) Roles() []string {
	return []string{}
}

func (v SystemContext) DataScope() []DataScope {
	return []DataScope{}
}

func (v SystemContext) TraceID() string {
	return ""
}

func (v SystemContext) HasPermission(action, resource string) bool {
	return true
}

func (v SystemContext) IsPlatformContext() bool {
	return true
}

func (v SystemContext) IsTenantContext() bool {
	return false
}

func (v SystemContext) IsSystemContext() bool {
	return true
}

func (v SystemContext) ShouldAudit() bool {
	return false
}
