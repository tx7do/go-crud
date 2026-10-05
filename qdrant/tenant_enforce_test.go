package qdrant

import (
	"context"
	"testing"

	qdrant "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tx7do/go-crud/qdrant/mixin"
	"github.com/tx7do/go-crud/viewer"
)

// scopedEntity 嵌入 TenantID mixin，实现 viewer.ScopedModel。
type scopedEntity struct {
	mixin.TenantID
	Name string `json:"name"`
}

// nonScopedEntity 无 TenantID mixin，不实现 viewer.ScopedModel。
type nonScopedEntity struct {
	Name string `json:"name"`
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
// 租户业务视图下，nil Filter 变为仅含 tenant_id 匹配条件的 Filter。
func TestInjectTenantFilter_TenantContextInjectsIntoNilFilter(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})

	f, err := InjectTenantFilterIntoQdrantFilter[scopedEntity](ctx, nil)
	require.NoError(t, err)
	require.NotNil(t, f)
	require.Len(t, f.Must, 1)
	require.NotNil(t, f.Must[0].GetField())
	assert.Equal(t, "tenant_id", f.Must[0].GetField().GetKey())
	require.NotNil(t, f.Must[0].GetField().GetMatch())
	assert.Equal(t, int64(7), f.Must[0].GetField().GetMatch().GetInteger())
}

// TestInjectTenantFilter_TenantContextWrapsExistingFilter
// 已有 Filter 被包装为单一条件并与 tenant_id 条件 AND 合并，原结构保持不变。
func TestInjectTenantFilter_TenantContextWrapsExistingFilter(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	orig := &qdrant.Filter{
		Should: []*qdrant.Condition{qdrant.NewMatchKeyword("tag", "x")},
	}

	f, err := InjectTenantFilterIntoQdrantFilter[scopedEntity](ctx, orig)
	require.NoError(t, err)
	require.NotNil(t, f)
	require.Len(t, f.Must, 2)
	// 原有 Filter 作为整体成为第一个 Must 条件（语义保留）。
	assert.Equal(t, orig, f.Must[0].GetFilter())
	assert.Same(t, orig, f.Must[0].GetFilter())
	assert.Equal(t, orig.Should, f.Must[0].GetFilter().Should)
	// tenant_id 条件为第二个 Must 条件。
	require.NotNil(t, f.Must[1].GetField())
	assert.Equal(t, int64(7), f.Must[1].GetField().GetMatch().GetInteger())
	assert.Nil(t, f.Should)
}

// TestInjectTenantFilter_PlatformContextSkips 平台视图不注入。
func TestInjectTenantFilter_PlatformContextSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 0, platform: true})

	f, err := InjectTenantFilterIntoQdrantFilter[scopedEntity](ctx, nil)
	require.NoError(t, err)
	assert.Nil(t, f)
}

// TestInjectTenantFilter_MissingViewerFailClosed 缺身份报错（fail-closed）。
func TestInjectTenantFilter_MissingViewerFailClosed(t *testing.T) {
	_, err := InjectTenantFilterIntoQdrantFilter[scopedEntity](context.Background(), nil)
	assert.ErrorIs(t, err, viewer.ErrMissingViewer)
}

// TestInjectTenantFilter_NonScopedSkips 非 tenant 实体原样透传。
func TestInjectTenantFilter_NonScopedSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})

	f, err := InjectTenantFilterIntoQdrantFilter[nonScopedEntity](ctx, nil)
	require.NoError(t, err)
	assert.Nil(t, f)
}

// TestPayloadTenantID 载荷 tenant_id 解析：合法整数、类型不符、越界、缺失。
func TestPayloadTenantID(t *testing.T) {
	ok, _ := qdrant.TryValueMap(map[string]any{"tenant_id": int64(7)})
	tid, found := payloadTenantID(ok)
	assert.True(t, found)
	assert.Equal(t, uint32(7), tid)

	wrongType, _ := qdrant.TryValueMap(map[string]any{"tenant_id": "not-an-int"})
	_, found = payloadTenantID(wrongType)
	assert.False(t, found)

	neg, _ := qdrant.TryValueMap(map[string]any{"tenant_id": int64(-1)})
	_, found = payloadTenantID(neg)
	assert.False(t, found)

	overflow, _ := qdrant.TryValueMap(map[string]any{"tenant_id": int64(1) << 40})
	_, found = payloadTenantID(overflow)
	assert.False(t, found)

	_, found = payloadTenantID(map[string]*qdrant.Value{})
	assert.False(t, found)
}

// TestVerifyTenantOnPayload_Match 租户匹配放行。
func TestVerifyTenantOnPayload_Match(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	payload, err := qdrant.TryValueMap(map[string]any{"tenant_id": int64(7)})
	require.NoError(t, err)
	assert.NoError(t, verifyTenantOnPayload[scopedEntity](ctx, payload))
}

// TestVerifyTenantOnPayload_Mismatch 租户不匹配报错。
func TestVerifyTenantOnPayload_Mismatch(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	payload, err := qdrant.TryValueMap(map[string]any{"tenant_id": int64(8)})
	require.NoError(t, err)
	assert.Equal(t, errTenantMismatch, verifyTenantOnPayload[scopedEntity](ctx, payload))
}

// TestVerifyTenantOnPayload_MissingTenantInPayload
// 租户视图下载荷缺 tenant_id（他租户或未落租户的点）视同不匹配。
func TestVerifyTenantOnPayload_MissingTenantInPayload(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	payload, _ := qdrant.TryValueMap(map[string]any{"name": "x"})
	assert.Equal(t, errTenantMismatch, verifyTenantOnPayload[scopedEntity](ctx, payload))
}

// TestVerifyTenantOnPayload_NonScopedSkips 非 tenant 实体放行（无校验）。
func TestVerifyTenantOnPayload_NonScopedSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	payload, _ := qdrant.TryValueMap(map[string]any{"tenant_id": int64(99)})
	assert.NoError(t, verifyTenantOnPayload[nonScopedEntity](ctx, payload))
}

// TestVerifyTenantOnPayload_PlatformSkips 平台视图放行。
func TestVerifyTenantOnPayload_PlatformSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 0, platform: true})
	payload, _ := qdrant.TryValueMap(map[string]any{"tenant_id": int64(99)})
	assert.NoError(t, verifyTenantOnPayload[scopedEntity](ctx, payload))
}

// TestVerifyTenantOnPayload_MissingViewerFailClosed 缺身份报错。
func TestVerifyTenantOnPayload_MissingViewerFailClosed(t *testing.T) {
	payload, _ := qdrant.TryValueMap(map[string]any{"tenant_id": int64(7)})
	assert.ErrorIs(t, verifyTenantOnPayload[scopedEntity](context.Background(), payload), viewer.ErrMissingViewer)
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
