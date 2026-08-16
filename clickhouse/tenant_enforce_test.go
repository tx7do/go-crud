package clickhouse

import (
	"context"
	"strings"
	"testing"

	"github.com/tx7do/go-crud/clickhouse/mixin"
	"github.com/tx7do/go-crud/clickhouse/query"
	"github.com/tx7do/go-crud/viewer"
)

// scopedEntity 嵌入 TenantID mixin，实现 viewer.ScopedModel。
type scopedEntity struct {
	mixin.TenantID
	Name string `db:"name"`
}

// nonScopedEntity 无 TenantID mixin，不实现 viewer.ScopedModel。
type nonScopedEntity struct {
	Name string `db:"name"`
}

type testEnforceViewer struct {
	tid      uint64
	platform bool
	system   bool
}

func (v testEnforceViewer) UserID() uint64                 { return 0 }
func (v testEnforceViewer) TenantID() uint64               { return v.tid }
func (v testEnforceViewer) OrgUnitID() uint64              { return 0 }
func (v testEnforceViewer) Permissions() []string          { return nil }
func (v testEnforceViewer) Roles() []string                { return nil }
func (v testEnforceViewer) DataScope() []viewer.DataScope  { return nil }
func (v testEnforceViewer) TraceID() string                { return "" }
func (v testEnforceViewer) HasPermission(_, _ string) bool { return false }
func (v testEnforceViewer) IsPlatformContext() bool        { return v.platform }
func (v testEnforceViewer) IsTenantContext() bool          { return v.tid > 0 && !v.platform }
func (v testEnforceViewer) IsSystemContext() bool          { return v.system }
func (v testEnforceViewer) ShouldAudit() bool              { return false }

// TestInjectTenantFilter_TenantContextInjects 租户业务视图下注入 tenant_id 谓词。
func TestInjectTenantFilter_TenantContextInjects(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	qb := query.NewQueryBuilder("t", nil)
	if err := InjectTenantFilterIntoBuilder[scopedEntity](ctx, qb); err != nil {
		t.Fatalf("inject: %v", err)
	}
	sqlStr, args := qb.BuildWhereParam()
	if !strings.Contains(strings.ToLower(sqlStr), "tenant_id") {
		t.Errorf("tenant context must inject tenant_id predicate, got SQL: %q", sqlStr)
	}
	if len(args) == 0 {
		t.Errorf("tenant predicate must produce a bound arg")
	}
}

// TestInjectTenantFilter_PlatformContextSkips 平台视图不注入。
func TestInjectTenantFilter_PlatformContextSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 0, platform: true})
	qb := query.NewQueryBuilder("t", nil)
	if err := InjectTenantFilterIntoBuilder[scopedEntity](ctx, qb); err != nil {
		t.Fatalf("inject: %v", err)
	}
	sqlStr, args := qb.BuildWhereParam()
	if strings.Contains(strings.ToLower(sqlStr), "tenant_id") {
		t.Errorf("platform context must NOT inject tenant_id, got SQL: %q", sqlStr)
	}
	if len(args) != 0 {
		t.Errorf("platform context must not produce args, got %d", len(args))
	}
}

// TestInjectTenantFilter_MissingViewerFailClosed 缺身份报错。
func TestInjectTenantFilter_MissingViewerFailClosed(t *testing.T) {
	qb := query.NewQueryBuilder("t", nil)
	err := InjectTenantFilterIntoBuilder[scopedEntity](context.Background(), qb)
	if err == nil {
		t.Errorf("missing viewer must fail-closed")
	}
}

// TestInjectTenantFilter_NonScopedSkips 非 tenant 实体跳过。
func TestInjectTenantFilter_NonScopedSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	qb := query.NewQueryBuilder("t", nil)
	err := InjectTenantFilterIntoBuilder[nonScopedEntity](ctx, qb)
	if err != nil {
		t.Errorf("non-scoped entity must pass through, got %v", err)
	}
	sqlStr, args := qb.BuildWhereParam()
	if strings.Contains(strings.ToLower(sqlStr), "tenant_id") {
		t.Errorf("non-scoped entity must not inject tenant_id, got SQL: %q", sqlStr)
	}
	if len(args) != 0 {
		t.Errorf("non-scoped entity must not produce args, got %d", len(args))
	}
}

// TestEnforceOnScopedInstanceAny_StructForcesTenantID 验证 B.12 修复：
// Client.BatchInsert 现在通过 EnforceOnScopedInstanceAny 对每个指针元素
// 在租户业务视图下强制覆盖 tenant_id。
func TestEnforceOnScopedInstanceAny_StructForcesTenantID(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 42})
	ent := &scopedEntity{Name: "x"}
	if err := viewer.EnforceOnScopedInstanceAny(ctx, ent); err != nil {
		t.Fatalf("enforce: %v", err)
	}
	if ent.TenantID.TenantID == nil || *ent.TenantID.TenantID != 42 {
		t.Errorf("must force-set tenant_id to 42, got %v", ent.TenantID.TenantID)
	}
}

// TestEnforceOnScopedInstanceAny_NonScopedPassesThrough 非租户实体放行，
// 无 force-set。
func TestEnforceOnScopedInstanceAny_NonScopedPassesThrough(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 42})
	ent := &nonScopedEntity{Name: "x"}
	if err := viewer.EnforceOnScopedInstanceAny(ctx, ent); err != nil {
		t.Fatalf("non-scoped must pass through, got %v", err)
	}
}

// TestEnforceOnScopedInstanceAny_MissingViewerFailClosed 缺身份报错。
func TestEnforceOnScopedInstanceAny_MissingViewerFailClosed(t *testing.T) {
	ent := &scopedEntity{Name: "x"}
	err := viewer.EnforceOnScopedInstanceAny(context.Background(), ent)
	if err == nil {
		t.Fatalf("missing viewer must fail-closed")
	}
}
