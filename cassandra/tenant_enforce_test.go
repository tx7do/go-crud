package cassandra

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tx7do/go-crud/cassandra/mixin"
	"github.com/tx7do/go-crud/viewer"
)

// scopedEntity 嵌入 TenantID mixin，实现 viewer.ScopedModel。
type scopedEntity struct {
	mixin.TenantID
	Title string
}

// nonScopedEntity 无 TenantID mixin，不实现 viewer.ScopedModel。
type nonScopedEntity struct {
	Title string
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

// TestInjectTenantWhere_TenantContextAppendsToEmptyWhere
// 租户业务视图下，空 WHERE 变为仅含 tenant_id 匹配谓词（位置参数）。
func TestInjectTenantWhere_TenantContextAppendsToEmptyWhere(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})

	where, args, err := InjectTenantWhere[scopedEntity](ctx, "", nil)
	require.NoError(t, err)
	assert.Equal(t, "tenant_id = ?", where)
	assert.Equal(t, []any{int64(7)}, args)
}

// TestInjectTenantWhere_TenantContextWrapsExistingWhere
// 已有 WHERE 片段被括号包裹后与租户谓词 AND 合并，参数按位置追加
// （CQL 无命名参数，不存在参数名冲突）。
func TestInjectTenantWhere_TenantContextWrapsExistingWhere(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})

	where, args, err := InjectTenantWhere[scopedEntity](ctx, "age > ?", []any{18})
	require.NoError(t, err)
	assert.Equal(t, "(age > ?) AND tenant_id = ?", where)
	assert.Equal(t, []any{18, int64(7)}, args)
}

// TestInjectTenantWhere_PlatformContextSkips 平台视图不注入。
func TestInjectTenantWhere_PlatformContextSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 0, platform: true})

	where, args, err := InjectTenantWhere[scopedEntity](ctx, "age > ?", []any{18})
	require.NoError(t, err)
	assert.Equal(t, "age > ?", where)
	assert.Equal(t, []any{18}, args)
}

// TestInjectTenantWhere_MissingViewerFailClosed 缺身份报错（fail-closed）。
func TestInjectTenantWhere_MissingViewerFailClosed(t *testing.T) {
	_, _, err := InjectTenantWhere[scopedEntity](context.Background(), "", nil)
	assert.ErrorIs(t, err, viewer.ErrMissingViewer)
}

// TestInjectTenantWhere_NonScopedSkips 非 tenant 实体原样透传。
func TestInjectTenantWhere_NonScopedSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})

	where, args, err := InjectTenantWhere[nonScopedEntity](ctx, "age > ?", []any{18})
	require.NoError(t, err)
	assert.Equal(t, "age > ?", where)
	assert.Equal(t, []any{18}, args)
}

// TestRowTenantID 行租户列解析：合法整数（int64/int）、类型不符、越界、缺失。
func TestRowTenantID(t *testing.T) {
	tid, found := rowTenantID(map[string]any{"tenant_id": int64(7)})
	assert.True(t, found)
	assert.Equal(t, uint32(7), tid)

	tid, found = rowTenantID(map[string]any{"tenant_id": 7})
	assert.True(t, found)
	assert.Equal(t, uint32(7), tid)

	_, found = rowTenantID(map[string]any{"tenant_id": "not-an-int"})
	assert.False(t, found)

	_, found = rowTenantID(map[string]any{"tenant_id": int64(-1)})
	assert.False(t, found)

	_, found = rowTenantID(map[string]any{"tenant_id": int64(1) << 40})
	assert.False(t, found)

	_, found = rowTenantID(map[string]any{"Title": "x"})
	assert.False(t, found)

	_, found = rowTenantID(map[string]any{"tenant_id": nil})
	assert.False(t, found)
}

// TestVerifyTenantOnRow_Match 租户匹配放行。
func TestVerifyTenantOnRow_Match(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	assert.NoError(t, verifyTenantOnRow[scopedEntity](ctx, map[string]any{"tenant_id": int64(7)}))
}

// TestVerifyTenantOnRow_Mismatch 租户不匹配报错。
func TestVerifyTenantOnRow_Mismatch(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	assert.Equal(t, errTenantMismatch,
		verifyTenantOnRow[scopedEntity](ctx, map[string]any{"tenant_id": int64(8)}))
}

// TestVerifyTenantOnRow_MissingTenantInRow 缺租户列视同不匹配。
func TestVerifyTenantOnRow_MissingTenantInRow(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	assert.Equal(t, errTenantMismatch,
		verifyTenantOnRow[scopedEntity](ctx, map[string]any{"Title": "x"}))
}

// TestVerifyTenantOnRow_NonScopedSkips 非 tenant 实体放行。
func TestVerifyTenantOnRow_NonScopedSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	assert.NoError(t, verifyTenantOnRow[nonScopedEntity](ctx, map[string]any{"tenant_id": int64(99)}))
}

// TestVerifyTenantOnRow_PlatformSkips 平台视图放行。
func TestVerifyTenantOnRow_PlatformSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 0, platform: true})
	assert.NoError(t, verifyTenantOnRow[scopedEntity](ctx, map[string]any{"tenant_id": int64(99)}))
}

// TestVerifyTenantOnRow_MissingViewerFailClosed 缺身份报错。
func TestVerifyTenantOnRow_MissingViewerFailClosed(t *testing.T) {
	assert.ErrorIs(t,
		verifyTenantOnRow[scopedEntity](context.Background(), map[string]any{"tenant_id": int64(7)}),
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
