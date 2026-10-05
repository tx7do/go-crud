package weaviate

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	wvFilters "github.com/weaviate/weaviate-go-client/v4/weaviate/filters"

	"github.com/tx7do/go-crud/viewer"
	"github.com/tx7do/go-crud/weaviate/mixin"
)

// scopedEntity 嵌入 TenantID mixin，实现 viewer.ScopedModel。
type scopedEntity struct {
	mixin.TenantID
	Title string `json:"title"`
}

// nonScopedEntity 无 TenantID mixin，不实现 viewer.ScopedModel。
type nonScopedEntity struct {
	Title string `json:"title"`
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

// TestInjectTenantFilter_TenantContextInjectsIntoNilFilter
// 租户业务视图下，nil 条件变为仅含 tenant_id 匹配的条件。
func TestInjectTenantFilter_TenantContextInjectsIntoNilFilter(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})

	fb, err := InjectTenantFilter[scopedEntity](ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, fb)
	assert.Contains(t, fb.String(), "tenant_id")
}

// TestInjectTenantFilter_TenantContextMergesExistingFilter
// 已有条件与租户条件 And 合并（原条件作为第一个操作数，语义保留）。
func TestInjectTenantFilter_TenantContextMergesExistingFilter(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	caller := wvFilters.Where().WithPath([]string{"title"}).WithOperator(wvFilters.Equal).WithValueText("x")

	fb, err := InjectTenantFilter[scopedEntity](ctx, caller)
	require.NoError(t, err)
	require.NotNil(t, fb)
	s := fb.String()
	assert.Contains(t, s, "And")
	assert.Contains(t, s, "title")
	assert.Contains(t, s, "tenant_id")
}

// TestInjectTenantFilter_PlatformContextSkips 平台视图不注入。
func TestInjectTenantFilter_PlatformContextSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 0, platform: true})

	fb, err := InjectTenantFilter[scopedEntity](ctx, nil)
	require.NoError(t, err)
	assert.Nil(t, fb)
}

// TestInjectTenantFilter_MissingViewerFailClosed 缺身份报错（fail-closed）。
func TestInjectTenantFilter_MissingViewerFailClosed(t *testing.T) {
	_, err := InjectTenantFilter[scopedEntity](context.Background(), nil)
	assert.ErrorIs(t, err, viewer.ErrMissingViewer)
}

// TestInjectTenantFilter_NonScopedSkips 非 tenant 实体原样透传。
func TestInjectTenantFilter_NonScopedSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})

	fb, err := InjectTenantFilter[nonScopedEntity](ctx, nil)
	require.NoError(t, err)
	assert.Nil(t, fb)
}

// TestPropsTenantID 属性 tenant_id 解析：合法数值（float64/int64/int/uint32）、
// 类型不符、越界、缺失。
func TestPropsTenantID(t *testing.T) {
	tid, found := propsTenantID(map[string]any{"tenant_id": float64(7)})
	assert.True(t, found)
	assert.Equal(t, uint32(7), tid)

	tid, found = propsTenantID(map[string]any{"tenant_id": int64(7)})
	assert.True(t, found)
	assert.Equal(t, uint32(7), tid)

	tid, found = propsTenantID(map[string]any{"tenant_id": 7})
	assert.True(t, found)
	assert.Equal(t, uint32(7), tid)

	tid, found = propsTenantID(map[string]any{"tenant_id": uint32(7)})
	assert.True(t, found)
	assert.Equal(t, uint32(7), tid)

	_, found = propsTenantID(map[string]any{"tenant_id": "not-an-int"})
	assert.False(t, found)

	_, found = propsTenantID(map[string]any{"tenant_id": -1.0})
	assert.False(t, found)

	_, found = propsTenantID(map[string]any{"tenant_id": float64(1) * 1099511627776}) // ≈1<<40，超 uint32
	assert.False(t, found)

	_, found = propsTenantID(map[string]any{"title": "x"})
	assert.False(t, found)

	_, found = propsTenantID(map[string]any{})
	assert.False(t, found)
}

// TestVerifyTenantOnProps_Match 租户匹配放行。
func TestVerifyTenantOnProps_Match(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	assert.NoError(t, verifyTenantOnProps[scopedEntity](ctx, map[string]any{"tenant_id": float64(7)}))
}

// TestVerifyTenantOnProps_Mismatch 租户不匹配报错。
func TestVerifyTenantOnProps_Mismatch(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	assert.Equal(t, errTenantMismatch,
		verifyTenantOnProps[scopedEntity](ctx, map[string]any{"tenant_id": float64(8)}))
}

// TestVerifyTenantOnProps_MissingTenantInProps
// 租户视图下属性缺 tenant_id（他租户或未落租户的对象）视同不匹配。
func TestVerifyTenantOnProps_MissingTenantInProps(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	assert.Equal(t, errTenantMismatch,
		verifyTenantOnProps[scopedEntity](ctx, map[string]any{"title": "x"}))
}

// TestVerifyTenantOnProps_NonScopedSkips 非 tenant 实体放行（无校验）。
func TestVerifyTenantOnProps_NonScopedSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	assert.NoError(t, verifyTenantOnProps[nonScopedEntity](ctx, map[string]any{"tenant_id": float64(99)}))
}

// TestVerifyTenantOnProps_PlatformSkips 平台视图放行。
func TestVerifyTenantOnProps_PlatformSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 0, platform: true})
	assert.NoError(t, verifyTenantOnProps[scopedEntity](ctx, map[string]any{"tenant_id": float64(99)}))
}

// TestVerifyTenantOnProps_MissingViewerFailClosed 缺身份报错。
func TestVerifyTenantOnProps_MissingViewerFailClosed(t *testing.T) {
	assert.ErrorIs(t,
		verifyTenantOnProps[scopedEntity](context.Background(), map[string]any{"tenant_id": float64(7)}),
		viewer.ErrMissingViewer)
}

// TestEnforceOnScopedInstance_TenantContextSets 实例级强制覆盖 tenant_id。
func TestEnforceOnScopedInstance_TenantContextSets(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	var e scopedEntity
	other := uint32(99)
	e.SetTenantID(other)
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
