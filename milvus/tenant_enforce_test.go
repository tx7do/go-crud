package milvus

import (
	"context"
	"testing"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tx7do/go-crud/milvus/mixin"
	"github.com/tx7do/go-crud/viewer"
)

// scopedEntity 嵌入 TenantID mixin，实现 viewer.ScopedModel。
type scopedEntity struct {
	mixin.TenantID
	Name string `milvus:"name:name"`
}

// nonScopedEntity 无 TenantID mixin，不实现 viewer.ScopedModel。
type nonScopedEntity struct {
	Name string `milvus:"name:name"`
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

// TestInjectTenantFilterIntoExpr_TenantContextInjects
// 租户业务视图下注入 tenant_id 谓词；已有表达式被括号包裹后 AND 合并。
func TestInjectTenantFilterIntoExpr_TenantContextInjects(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})

	got, err := InjectTenantFilterIntoExpr[scopedEntity](ctx, "")
	require.NoError(t, err)
	assert.Equal(t, "tenant_id == 7", got)

	got, err = InjectTenantFilterIntoExpr[scopedEntity](ctx, "status == 1")
	require.NoError(t, err)
	assert.Equal(t, "(status == 1) and tenant_id == 7", got)
}

// TestInjectTenantFilterIntoExpr_PlatformContextSkips 平台视图不注入。
func TestInjectTenantFilterIntoExpr_PlatformContextSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 0, platform: true})

	got, err := InjectTenantFilterIntoExpr[scopedEntity](ctx, "status == 1")
	require.NoError(t, err)
	assert.Equal(t, "status == 1", got)
}

// TestInjectTenantFilterIntoExpr_MissingViewerFailClosed 缺身份报错。
func TestInjectTenantFilterIntoExpr_MissingViewerFailClosed(t *testing.T) {
	_, err := InjectTenantFilterIntoExpr[scopedEntity](context.Background(), "")
	assert.ErrorIs(t, err, viewer.ErrMissingViewer)
}

// TestInjectTenantFilterIntoExpr_NonScopedSkips 非 tenant 实体原样透传。
func TestInjectTenantFilterIntoExpr_NonScopedSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})

	got, err := InjectTenantFilterIntoExpr[nonScopedEntity](ctx, "status == 1")
	require.NoError(t, err)
	assert.Equal(t, "status == 1", got)
}

// TestCheckTenantColumn_Match 列值与 Viewer 租户一致放行。
func TestCheckTenantColumn_Match(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	rs := client.ResultSet{entity.NewColumnInt64("tenant_id", []int64{7})}
	assert.NoError(t, checkTenantColumn[scopedEntity](ctx, rs, 0))
}

// TestCheckTenantColumn_Mismatch 列值与 Viewer 租户不一致报错。
func TestCheckTenantColumn_Mismatch(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	rs := client.ResultSet{entity.NewColumnInt64("tenant_id", []int64{8})}
	assert.Equal(t, errTenantMismatch, checkTenantColumn[scopedEntity](ctx, rs, 0))
}

// TestCheckTenantColumn_MissingColumn 列缺失（无租户标识的行）视同不匹配。
func TestCheckTenantColumn_MissingColumn(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	rs := client.ResultSet{entity.NewColumnInt64("name", []int64{1})}
	assert.Equal(t, errTenantMismatch, checkTenantColumn[scopedEntity](ctx, rs, 0))
}

// TestCheckTenantColumn_NonScopedSkips 非 tenant 实体放行（无校验）。
func TestCheckTenantColumn_NonScopedSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	rs := client.ResultSet{entity.NewColumnInt64("tenant_id", []int64{99})}
	assert.NoError(t, checkTenantColumn[nonScopedEntity](ctx, rs, 0))
}

// TestCheckTenantColumn_PlatformSkips 平台视图放行。
func TestCheckTenantColumn_PlatformSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 0, platform: true})
	rs := client.ResultSet{entity.NewColumnInt64("tenant_id", []int64{99})}
	assert.NoError(t, checkTenantColumn[scopedEntity](ctx, rs, 0))
}

// TestCheckTenantColumn_MissingViewerFailClosed 缺身份报错。
func TestCheckTenantColumn_MissingViewerFailClosed(t *testing.T) {
	rs := client.ResultSet{entity.NewColumnInt64("tenant_id", []int64{7})}
	assert.ErrorIs(t, checkTenantColumn[scopedEntity](context.Background(), rs, 0), viewer.ErrMissingViewer)
}

// TestEnforceOnScopedInstance_TenantContextSets 实例级强制覆盖 tenant_id。
func TestEnforceOnScopedInstance_TenantContextSets(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	var e scopedEntity
	e.SetTenantID(99)
	require.NoError(t, viewer.EnforceOnScopedInstance(ctx, &e))
	got := e.GetTenantID()
	require.NotNil(t, got)
	assert.Equal(t, uint32(7), *got)
}

// TestEnforceOnScopedInstance_MissingViewerFailClosed 缺身份报错。
func TestEnforceOnScopedInstance_MissingViewerFailClosed(t *testing.T) {
	var e scopedEntity
	assert.ErrorIs(t, viewer.EnforceOnScopedInstance(context.Background(), &e), viewer.ErrMissingViewer)
}
