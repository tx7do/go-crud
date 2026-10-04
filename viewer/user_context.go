package viewer

// UserContext 是携带身份字段的标准用户上下文实现：
// uid/tid/ouid/traceID 与数据范围经构造注入并原样回读。
//
// 语义约定（与细粒度权限、审计链路解耦）：
//   - 不携带权限/角色列表：Permissions/Roles 恒为 nil，HasPermission 恒拒——
//     细粒度权限判定走各接入系统自己的 authz 链路，不在 viewer 层承载；
//   - 永远不是系统上下文（IsSystemContext 恒 false），默认不记审计日志；
//   - 平台/租户视图随 tenant_id 是否为零翻转，这是租户隔离闸门的判定依据
//     （见 EnforceTenant）。
type UserContext struct {
	uid        uint64
	tid        uint64
	ouid       uint64
	traceID    string
	dataScopes []DataScope
}

// NewUserContext 构造标准用户上下文。
func NewUserContext(
	uid uint64,
	tid uint64,
	ouid uint64,
	traceID string,
	dataScopes []DataScope,
) Context {
	return UserContext{
		uid:        uid,
		tid:        tid,
		ouid:       ouid,
		dataScopes: dataScopes,
		traceID:    traceID,
	}
}

func (v UserContext) UserID() uint64 {
	return v.uid
}

func (v UserContext) TenantID() uint64 {
	return v.tid
}

func (v UserContext) OrgUnitID() uint64 {
	return v.ouid
}

func (v UserContext) Permissions() []string {
	return nil
}

func (v UserContext) Roles() []string {
	return nil
}

func (v UserContext) DataScope() []DataScope {
	return v.dataScopes
}

func (v UserContext) TraceID() string {
	return v.traceID
}

func (v UserContext) HasPermission(_, _ string) bool {
	return false
}

func (v UserContext) IsPlatformContext() bool {
	return v.tid == 0
}

func (v UserContext) IsTenantContext() bool {
	return v.tid > 0
}

func (v UserContext) IsSystemContext() bool {
	return false
}

func (v UserContext) ShouldAudit() bool {
	return false
}
