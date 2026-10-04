// 本文件为 user_context.go / system_context.go 的单元测试（标准库断言，
// 包保持零第三方依赖）。
//
// SystemContext 是系统后台任务的固定语义实现：零值身份、空权限/角色/数据
// 范围、平台视图、系统上下文、权限判定一律放行、不记审计。这里逐项锁定
// 上述语义，防止系统身份被改成携带租户或放行范围变化，导致系统级任务
// 被租户闸门误拦或数据范围被意外放大。
//
// UserContext 验证注入身份的原样回读与平台/租户视图随 tenant_id 翻转，
// 以及与身份注入无关的固定语义（无权限/角色、权限判定恒拒、非系统上下文、
// 不记审计）。
package viewer

import (
	"context"
	"reflect"
	"testing"
)

// TestNewSystemContext_ReturnsSystemContext 验证 NewSystemContext 返回的
// 接口值动态类型就是 SystemContext（空结构体，没有任何可变状态可供外部篡改）。
func TestNewSystemContext_ReturnsSystemContext(t *testing.T) {
	vc := NewSystemContext()
	if vc == nil {
		t.Fatal("NewSystemContext returned nil")
	}
	if _, ok := vc.(SystemContext); !ok {
		t.Fatalf("NewSystemContext must return SystemContext dynamic type, got %T", vc)
	}
}

// TestWithSystemContext_RoundTrip 验证 WithSystemContext 注入的系统身份能被
// FromContext 原样取回（注入链路可用），且注入前裸 context 中不存在 viewer。
func TestWithSystemContext_RoundTrip(t *testing.T) {
	base := context.Background()
	if _, ok := FromContext(base); ok {
		t.Fatal("bare context must not contain a viewer")
	}

	ctx := WithSystemContext(base)
	vc, ok := FromContext(ctx)
	if !ok {
		t.Fatal("WithSystemContext-injected viewer must be retrievable via FromContext")
	}
	if _, isSys := vc.(SystemContext); !isSys {
		t.Fatalf("retrieved viewer must be SystemContext, got %T", vc)
	}

	// 取回的身份必须呈现系统语义，而非普通用户语义
	if !vc.IsSystemContext() {
		t.Fatal("system viewer must report IsSystemContext=true")
	}
	if !vc.IsPlatformContext() {
		t.Fatal("system viewer has tenant_id==0 and belongs to the platform view")
	}
	if vc.IsTenantContext() {
		t.Fatal("system viewer must not belong to the tenant view")
	}
	if got := vc.UserID(); got != 0 {
		t.Fatalf("system UserID = %d, want 0", got)
	}
	if got := vc.TenantID(); got != 0 {
		t.Fatalf("system TenantID = %d, want 0", got)
	}
}

// TestSystemContext_FixedGetterSemantics 逐项锁定 SystemContext 全部 getter
// 的固定返回值。这些值都是硬编码常量，任何一项漂移都会在这里被抓住。
func TestSystemContext_FixedGetterSemantics(t *testing.T) {
	vc := NewSystemContext()
	if vc == nil {
		t.Fatal("NewSystemContext returned nil")
	}

	identityGetters := []struct {
		name string
		got  func() any
		want any
	}{
		{"UserID 恒为 0", func() any { return vc.UserID() }, uint64(0)},
		{"TenantID 恒为 0", func() any { return vc.TenantID() }, uint64(0)},
		{"OrgUnitID 恒为 0", func() any { return vc.OrgUnitID() }, uint64(0)},
		{"Permissions 恒为空列表", func() any { return vc.Permissions() }, []string{}},
		{"Roles 恒为空列表", func() any { return vc.Roles() }, []string{}},
		{"DataScope 恒为空列表", func() any { return vc.DataScope() }, []DataScope{}},
		{"TraceID 恒为空串", func() any { return vc.TraceID() }, ""},
		{"ShouldAudit 恒为 false", func() any { return vc.ShouldAudit() }, false},
		{"IsPlatformContext 恒为 true", func() any { return vc.IsPlatformContext() }, true},
		{"IsTenantContext 恒为 false", func() any { return vc.IsTenantContext() }, false},
		{"IsSystemContext 恒为 true", func() any { return vc.IsSystemContext() }, true},
	}
	for _, tc := range identityGetters {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.got(); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestSystemContext_HasPermissionAlwaysGrants 验证系统身份的权限判定对任意
// 动作/资源组合一律放行（系统任务必须能执行全量维护操作）。
func TestSystemContext_HasPermissionAlwaysGrants(t *testing.T) {
	vc := NewSystemContext()
	if vc == nil {
		t.Fatal("NewSystemContext returned nil")
	}

	for _, comb := range [][2]string{
		{"read", "user"},
		{"update", "role"},
		{"", ""},
		{"任意动作", "任意资源"},
	} {
		if !vc.HasPermission(comb[0], comb[1]) {
			t.Fatalf("system viewer must grant action=%q resource=%q", comb[0], comb[1])
		}
	}
}

// TestUserContext_GettersReturnInjectedIdentity 验证注入 UserContext 的身份
// 字段（uid/tid/ouid/traceID/数据范围）必须能从对应 getter 原样取回，且
// 平台/租户视图语义严格随 tenant_id 是否为零翻转——这是租户隔离闸门的
// 判定依据。
func TestUserContext_GettersReturnInjectedIdentity(t *testing.T) {
	tests := []struct {
		name         string
		uid          uint64
		tid          uint64
		ouid         uint64
		traceID      string
		dataScopes   []DataScope
		wantPlatform bool
		wantTenant   bool
	}{
		{
			name:         "tenant view tid>0",
			uid:          11,
			tid:          22,
			ouid:         33,
			traceID:      "trace-tenant-001",
			dataScopes:   []DataScope{{ScopeType: ScopeTypeAll}},
			wantPlatform: false,
			wantTenant:   true,
		},
		{
			name:         "platform view tid=0",
			uid:          1,
			tid:          0,
			ouid:         2,
			traceID:      "",
			dataScopes:   []DataScope{{ScopeType: ScopeTypeSelf}},
			wantPlatform: true,
			wantTenant:   false,
		},
		{
			name:    "multi scopes with unit targets",
			uid:     100,
			tid:     200,
			ouid:    300,
			traceID: "trace-multi",
			dataScopes: []DataScope{
				{ScopeType: ScopeTypeSelf},
				{ScopeType: ScopeTypeUnit, TargetIDs: []uint64{31, 32}},
			},
			wantPlatform: false,
			wantTenant:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vc := NewUserContext(tc.uid, tc.tid, tc.ouid, tc.traceID, tc.dataScopes)

			if got := vc.UserID(); got != tc.uid {
				t.Fatalf("UserID = %d, want %d", got, tc.uid)
			}
			if got := vc.TenantID(); got != tc.tid {
				t.Fatalf("TenantID = %d, want %d", got, tc.tid)
			}
			if got := vc.OrgUnitID(); got != tc.ouid {
				t.Fatalf("OrgUnitID = %d, want %d", got, tc.ouid)
			}
			if got := vc.TraceID(); got != tc.traceID {
				t.Fatalf("TraceID = %q, want %q", got, tc.traceID)
			}
			if got := vc.DataScope(); !reflect.DeepEqual(got, tc.dataScopes) {
				t.Fatalf("DataScope = %v, want %v", got, tc.dataScopes)
			}
			if got := vc.IsPlatformContext(); got != tc.wantPlatform {
				t.Fatalf("IsPlatformContext = %v, want %v (must follow tenant_id==0 semantics)", got, tc.wantPlatform)
			}
			if got := vc.IsTenantContext(); got != tc.wantTenant {
				t.Fatalf("IsTenantContext = %v, want %v (must follow tenant_id>0 semantics)", got, tc.wantTenant)
			}
		})
	}
}

// TestUserContext_FixedSemantics 验证 UserContext 与身份注入无关的固定语义：
// 不携带权限/角色列表（细粒度权限走 authz 链路而非 viewer）、HasPermission
// 恒拒、永远不是系统上下文、不记审计日志。
func TestUserContext_FixedSemantics(t *testing.T) {
	vc := NewUserContext(1, 2, 3, "trace-fixed", []DataScope{{ScopeType: ScopeTypeSelf}})

	if got := vc.Permissions(); got != nil {
		t.Fatalf("UserContext must carry no permission list, got %v", got)
	}
	if got := vc.Roles(); got != nil {
		t.Fatalf("UserContext must carry no role list, got %v", got)
	}
	if vc.IsSystemContext() {
		t.Fatal("user identity must never be a system context")
	}
	if vc.ShouldAudit() {
		t.Fatal("user identity must not request audit logging by default")
	}

	for _, comb := range [][2]string{{"read", "user"}, {"", ""}} {
		if vc.HasPermission(comb[0], comb[1]) {
			t.Fatalf("user identity permission check must deny action=%q resource=%q", comb[0], comb[1])
		}
	}
}

// TestUserContext_ContextRoundTrip 验证 WithContext 注入的 UserContext 能被
// FromContext 完整取回（身份无损耗）；未注入的裸 context 取不到 viewer。
func TestUserContext_ContextRoundTrip(t *testing.T) {
	scopes := []DataScope{
		{ScopeType: ScopeTypeUnit, TargetIDs: []uint64{5, 6}},
	}
	ctx := WithContext(context.Background(), NewUserContext(101, 202, 303, "trace-xyz", scopes))

	vc, ok := FromContext(ctx)
	if !ok {
		t.Fatal("WithContext-injected viewer must be retrievable via FromContext")
	}
	if got := vc.UserID(); got != 101 {
		t.Fatalf("UserID = %d, want 101", got)
	}
	if got := vc.TenantID(); got != 202 {
		t.Fatalf("TenantID = %d, want 202", got)
	}
	if got := vc.OrgUnitID(); got != 303 {
		t.Fatalf("OrgUnitID = %d, want 303", got)
	}
	if got := vc.TraceID(); got != "trace-xyz" {
		t.Fatalf("TraceID = %q, want %q", got, "trace-xyz")
	}
	if got := vc.DataScope(); !reflect.DeepEqual(got, scopes) {
		t.Fatalf("DataScope = %v, want %v", got, scopes)
	}

	if _, ok := FromContext(context.Background()); ok {
		t.Fatal("context without injected viewer must not yield a viewer")
	}
}
