package viewer

// noopContext 实现 Context 接口，用于表示匿名或未授权用户
type noopContext struct{}

func (noopContext) UserID() uint64                 { return 0 }
func (noopContext) TenantID() uint64               { return 0 }
func (noopContext) OrgUnitID() uint64              { return 0 }
func (noopContext) Permissions() []string          { return nil }
func (noopContext) Roles() []string                { return nil }
func (noopContext) DataScope() []DataScope         { return nil }
func (noopContext) TraceID() string                { return "" }
func (noopContext) HasPermission(_, _ string) bool { return false }
func (noopContext) IsPlatformContext() bool        { return false }
func (noopContext) IsTenantContext() bool          { return false }
func (noopContext) IsSystemContext() bool          { return false }
func (noopContext) ShouldAudit() bool              { return false }

// NewNoopContext 创建一个匿名上下文实例
func NewNoopContext() Context {
	return noopContext{}
}
