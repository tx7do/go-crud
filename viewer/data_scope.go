package viewer

// ScopeType 定义数据权限范围的类型
type ScopeType string

const (
	// ScopeTypeSelf 仅限本人创建/拥有的数据 (created_by = uid)
	ScopeTypeSelf ScopeType = "SELF"

	// ScopeTypeUnit 组织维度隔离。
	// 逻辑：TargetIDs 中包含所有授权的组织 ID。
	// 如果是“本部门及下级”，TargetIDs 存放展开后的所有子 ID；
	// 如果是“仅本部门”，TargetIDs 只存放当前部门 ID。
	ScopeTypeUnit ScopeType = "UNIT"

	// ScopeTypeUser 指定的用户列表 (created_by IN [...user_ids])
	ScopeTypeUser ScopeType = "USER"

	// ScopeTypeAll 全量放行 (不注入过滤条件)
	ScopeTypeAll ScopeType = "ALL"

	// ScopeTypeNone 禁止任何数据访问 (拒绝策略)
	ScopeTypeNone ScopeType = "NONE"
)

// DataScope 定义数据权限范围
type DataScope struct {
	ScopeType ScopeType // 数据权限范围类型
	TargetIDs []uint64  // 具体的 ID 集合
}
