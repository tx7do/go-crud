package viewer

// DataScope 定义数据权限范围
type DataScope struct {
	ScopeType string   // "department", "region", "all"
	TargetIDs []uint32 // 具体的 ID 集合
}
