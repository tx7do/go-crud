package neo4j

import (
	"context"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tx7do/go-crud/neo4j/mixin"
	"github.com/tx7do/go-crud/viewer"
)

// scopedEntity 嵌入 TenantID mixin，实现 viewer.ScopedModel。
type scopedEntity struct {
	mixin.TenantID
	Name string
}

// nonScopedEntity 无 TenantID mixin，不实现 viewer.ScopedModel。
type nonScopedEntity struct {
	Name string
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

// TestInjectTenantPredicate_TenantContextAppendsToEmptyWhere
// 租户业务视图下，空 WHERE 变为仅含 tenant_id 匹配谓词，值经保留参数传递。
func TestInjectTenantPredicate_TenantContextAppendsToEmptyWhere(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})

	where, params, err := InjectTenantPredicate[scopedEntity](ctx, "", nil)
	require.NoError(t, err)
	assert.Equal(t, "n.`tenant_id` = $__tid", where)
	require.Len(t, params, 1)
	assert.Equal(t, int64(7), params["__tid"])
}

// TestInjectTenantPredicate_TenantContextWrapsExistingWhere
// 已有 WHERE 片段被括号包裹后与租户谓词 AND 合并；调用方参数被复制合并
// （不原地改动调用方表）。
func TestInjectTenantPredicate_TenantContextWrapsExistingWhere(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	caller := map[string]any{"age": int64(3)}

	where, params, err := InjectTenantPredicate[scopedEntity](ctx, "n.age > $age", caller)
	require.NoError(t, err)
	assert.Equal(t, "(n.age > $age) AND n.`tenant_id` = $__tid", where)
	require.Len(t, params, 2)
	assert.Equal(t, int64(3), params["age"])
	assert.Equal(t, int64(7), params["__tid"])
	// 调用方表不被原地改动。
	assert.Len(t, caller, 1)
	assert.NotContains(t, caller, "__tid")
}

// TestInjectTenantPredicate_PlatformContextSkips 平台视图不注入。
func TestInjectTenantPredicate_PlatformContextSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 0, platform: true})

	where, params, err := InjectTenantPredicate[scopedEntity](ctx, "n.age > $age", map[string]any{"age": int64(3)})
	require.NoError(t, err)
	assert.Equal(t, "n.age > $age", where)
	assert.Len(t, params, 1)
}

// TestInjectTenantPredicate_MissingViewerFailClosed 缺身份报错（fail-closed）。
func TestInjectTenantPredicate_MissingViewerFailClosed(t *testing.T) {
	_, _, err := InjectTenantPredicate[scopedEntity](context.Background(), "", nil)
	assert.ErrorIs(t, err, viewer.ErrMissingViewer)
}

// TestInjectTenantPredicate_NonScopedSkips 非 tenant 实体原样透传。
func TestInjectTenantPredicate_NonScopedSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})

	where, params, err := InjectTenantPredicate[nonScopedEntity](ctx, "", nil)
	require.NoError(t, err)
	assert.Empty(t, where)
	assert.Nil(t, params)
}

// TestInjectTenantPredicate_ParamCollision 携带保留参数名的调用方表被拒绝。
func TestInjectTenantPredicate_ParamCollision(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})

	_, _, err := InjectTenantPredicate[scopedEntity](ctx, "", map[string]any{"__tid": int64(9)})
	assert.ErrorIs(t, err, ErrInvalidRequest)
}

// TestNodeTenantID 节点属性 tenant_id 解析：合法整数、类型不符、越界、缺失。
func TestNodeTenantID(t *testing.T) {
	tid, found := nodeTenantID(neo4j.Node{Props: map[string]any{"tenant_id": int64(7)}})
	assert.True(t, found)
	assert.Equal(t, uint32(7), tid)

	_, found = nodeTenantID(neo4j.Node{Props: map[string]any{"tenant_id": "not-an-int"}})
	assert.False(t, found)

	_, found = nodeTenantID(neo4j.Node{Props: map[string]any{"tenant_id": int64(-1)}})
	assert.False(t, found)

	_, found = nodeTenantID(neo4j.Node{Props: map[string]any{"tenant_id": int64(1) << 40}})
	assert.False(t, found)

	_, found = nodeTenantID(neo4j.Node{Props: map[string]any{"name": "x"}})
	assert.False(t, found)

	_, found = nodeTenantID(neo4j.Node{})
	assert.False(t, found)
}

// TestVerifyTenantOnNode_Match 租户匹配放行。
func TestVerifyTenantOnNode_Match(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	node := neo4j.Node{Props: map[string]any{"tenant_id": int64(7)}}
	assert.NoError(t, verifyTenantOnNode[scopedEntity](ctx, node))
}

// TestVerifyTenantOnNode_Mismatch 租户不匹配报错。
func TestVerifyTenantOnNode_Mismatch(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	node := neo4j.Node{Props: map[string]any{"tenant_id": int64(8)}}
	assert.Equal(t, errTenantMismatch, verifyTenantOnNode[scopedEntity](ctx, node))
}

// TestVerifyTenantOnNode_MissingTenantInNode
// 租户视图下节点缺 tenant_id 属性（他租户或未落租户的节点）视同不匹配。
func TestVerifyTenantOnNode_MissingTenantInNode(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	node := neo4j.Node{Props: map[string]any{"name": "x"}}
	assert.Equal(t, errTenantMismatch, verifyTenantOnNode[scopedEntity](ctx, node))
}

// TestVerifyTenantOnNode_NonScopedSkips 非 tenant 实体放行（无校验）。
func TestVerifyTenantOnNode_NonScopedSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 7})
	node := neo4j.Node{Props: map[string]any{"tenant_id": int64(99)}}
	assert.NoError(t, verifyTenantOnNode[nonScopedEntity](ctx, node))
}

// TestVerifyTenantOnNode_PlatformSkips 平台视图放行。
func TestVerifyTenantOnNode_PlatformSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), testEnforceViewer{tid: 0, platform: true})
	node := neo4j.Node{Props: map[string]any{"tenant_id": int64(99)}}
	assert.NoError(t, verifyTenantOnNode[scopedEntity](ctx, node))
}

// TestVerifyTenantOnNode_MissingViewerFailClosed 缺身份报错。
func TestVerifyTenantOnNode_MissingViewerFailClosed(t *testing.T) {
	node := neo4j.Node{Props: map[string]any{"tenant_id": int64(7)}}
	assert.ErrorIs(t, verifyTenantOnNode[scopedEntity](context.Background(), node), viewer.ErrMissingViewer)
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
