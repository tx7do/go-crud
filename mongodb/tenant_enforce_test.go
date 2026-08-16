package mongodb

import (
	"context"
	"testing"

	bsonV2 "go.mongodb.org/mongo-driver/v2/bson"

	"github.com/tx7do/go-crud/mongodb/mixin"
	"github.com/tx7do/go-crud/mongodb/query"
	"github.com/tx7do/go-crud/viewer"
)

// scopedEntity 嵌入 TenantID mixin，实现 viewer.ScopedModel。
type scopedEntity struct {
	mixin.TenantID
	Name string `bson:"name"`
}

// nonScopedEntity 无 TenantID mixin，不实现 viewer.ScopedModel。
type nonScopedEntity struct {
	Name string `bson:"name"`
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
	qb := query.NewQueryBuilder()
	if err := InjectTenantFilterIntoBuilder[scopedEntity](ctx, qb); err != nil {
		t.Fatalf("inject: %v", err)
	}
	filterDoc, _, _ := qb.BuildFindOne()
	m, ok := filterDoc.(bsonV2.M)
	if !ok {
		t.Fatalf("filter not bson.M: %T", filterDoc)
	}
	if m["tenant_id"] != uint64(7) {
		t.Errorf("tenant_id must be injected as 7, got %v", m["tenant_id"])
	}
}

// TestInjectTenantFilter_PlatformContextSkips 平台视图不注入。
func TestInjectTenantFilter_PlatformContextSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 0, platform: true})
	qb := query.NewQueryBuilder()
	if err := InjectTenantFilterIntoBuilder[scopedEntity](ctx, qb); err != nil {
		t.Fatalf("inject: %v", err)
	}
	filterDoc, _, _ := qb.BuildFindOne()
	m, _ := filterDoc.(bsonV2.M)
	if _, hasKey := m["tenant_id"]; hasKey {
		t.Errorf("platform context must not inject tenant_id predicate")
	}
}

// TestInjectTenantFilter_MissingViewerFailClosed 缺身份报错。
func TestInjectTenantFilter_MissingViewerFailClosed(t *testing.T) {
	qb := query.NewQueryBuilder()
	err := InjectTenantFilterIntoBuilder[scopedEntity](context.Background(), qb)
	if err == nil {
		t.Errorf("missing viewer must fail-closed")
	}
}

// TestInjectTenantFilter_NonScopedSkips 非 tenant 实体跳过（不注入、不报错）。
func TestInjectTenantFilter_NonScopedSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	qb := query.NewQueryBuilder()
	// nonScopedEntity 不实现 ScopedModel，IsTenantScopedType 返回 false
	err := InjectTenantFilterIntoBuilder[nonScopedEntity](ctx, qb)
	if err != nil {
		t.Errorf("non-scoped entity must pass through, got %v", err)
	}
	filterDoc, _, _ := qb.BuildFindOne()
	m, _ := filterDoc.(bsonV2.M)
	if _, hasKey := m["tenant_id"]; hasKey {
		t.Errorf("non-scoped entity must not inject tenant_id predicate")
	}
}

// TestEnforceOnScopedInstance_TenantContextSets 实例级强制覆盖 tenant_id。
func TestEnforceOnScopedInstance_TenantContextSets(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	var e scopedEntity
	// 试图预设他租户
	other := uint32(99)
	e.TenantID.SetTenantID(other)
	if err := viewer.EnforceOnScopedInstance(ctx, &e); err != nil {
		t.Fatalf("enforce: %v", err)
	}
	got := e.TenantID.GetTenantID()
	if got == nil || *got != 7 {
		t.Errorf("tenant_id must be force-set to 7, got %v", got)
	}
}
