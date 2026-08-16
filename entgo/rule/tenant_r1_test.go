package rule

import (
	"context"
	"testing"

	"entgo.io/ent/dialect/sql"
	"github.com/tx7do/go-crud/viewer"
)

// stubUpdateBuilder 是一个最小化的变更构建器 mock，记录 Where 是否被调用
// 及其 predicate。用于验证 InjectTenantWhereIntoBuilder 的三态行为。
type stubUpdateBuilder struct {
	whereCalled bool
	pred        func(*sql.Selector)
}

func (s *stubUpdateBuilder) Where(ps ...func(*sql.Selector)) *stubUpdateBuilder {
	if len(ps) > 0 {
		s.whereCalled = true
		s.pred = ps[0]
	}
	return s
}

// 以下两个方法令 *stubUpdateBuilder 实现 viewer.ScopedModel，使类型门控
// (IsTenantScopedType) 判定为 tenant-scoped，从而让注入测试走到
// injectTenantWhereReflect。仅用于测试，无实际 tenant 语义。
func (s *stubUpdateBuilder) GetTenantID() *uint32 { return nil }
func (s *stubUpdateBuilder) SetTenantID(uint32)    {}

// badSigBuilder 实现 viewer.ScopedModel（通过类型门控），但其 Where 签名
// 与预期的 Where(...func(*sql.Selector)) 不符。用于验证
// injectTenantWhereReflect 在签名不匹配时 fail-closed，而非静默跳过——
// 这是 B.10 的核心：ent 升级若改了 Where 签名，注入必须报错而非放过。
type badSigBuilder struct{}

func (*badSigBuilder) Where(int) int { return 0 } // 非变参、参数类型不符
func (*badSigBuilder) GetTenantID() *uint32 { return nil }
func (*badSigBuilder) SetTenantID(uint32)    {}

// stubQueryViewer 实现 viewer.Context，可配置 tenant/platform/system。
type stubQueryViewer struct {
	tid      uint64
	platform bool
	system   bool
}

func (s *stubQueryViewer) UserID() uint64                 { return 0 }
func (s *stubQueryViewer) TenantID() uint64               { return s.tid }
func (s *stubQueryViewer) OrgUnitID() uint64              { return 0 }
func (s *stubQueryViewer) Permissions() []string          { return nil }
func (s *stubQueryViewer) Roles() []string                { return nil }
func (s *stubQueryViewer) DataScope() []viewer.DataScope  { return nil }
func (s *stubQueryViewer) TraceID() string                { return "" }
func (s *stubQueryViewer) HasPermission(_, _ string) bool { return false }
func (s *stubQueryViewer) IsPlatformContext() bool        { return s.platform }
func (s *stubQueryViewer) IsTenantContext() bool          { return s.tid > 0 && !s.platform }
func (s *stubQueryViewer) IsSystemContext() bool          { return s.system }
func (s *stubQueryViewer) ShouldAudit() bool              { return false }

// TestInjectTenantWhereIntoBuilder_TenantContextInjects 验证租户业务视图下
// Where 被调用注入 tenant_id 谓词（闭合 R-1 写侧行级缺口）。
func TestInjectTenantWhereIntoBuilder_TenantContextInjects(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), &stubQueryViewer{tid: 7})
	b := &stubUpdateBuilder{}
	if err := InjectTenantWhereIntoBuilder[stubUpdateBuilder](ctx, b); err != nil {
		t.Fatalf("inject: %v", err)
	}
	if !b.whereCalled {
		t.Errorf("tenant context must call Where to inject predicate")
	}
	if b.pred == nil {
		t.Errorf("predicate must be set")
	}
}

// TestInjectTenantWhereIntoBuilder_PlatformContextSkips 平台视图不注入。
func TestInjectTenantWhereIntoBuilder_PlatformContextSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), &stubQueryViewer{tid: 0, platform: true})
	b := &stubUpdateBuilder{}
	if err := InjectTenantWhereIntoBuilder[stubUpdateBuilder](ctx, b); err != nil {
		t.Fatalf("inject: %v", err)
	}
	if b.whereCalled {
		t.Errorf("platform context must NOT inject predicate")
	}
}

// TestInjectTenantWhereIntoBuilder_MissingViewerFailClosed 缺身份报错。
func TestInjectTenantWhereIntoBuilder_MissingViewerFailClosed(t *testing.T) {
	b := &stubUpdateBuilder{}
	err := InjectTenantWhereIntoBuilder[stubUpdateBuilder](context.Background(), b)
	if err == nil {
		t.Errorf("missing viewer must fail-closed")
	}
	if b.whereCalled {
		t.Errorf("fail-closed must not call Where")
	}
}

// TestInjectTenantWhereIntoBuilder_NonTenantBuilderSkips 非 tenant-scoped
// 类型（struct{} 不实现 ScopedModel）经类型门控直接放行，不报错、不注入。
func TestInjectTenantWhereIntoBuilder_NonTenantBuilderSkips(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), &stubQueryViewer{tid: 7})
	// 非 tenant-scoped：struct{} 不实现 viewer.ScopedModel → 类型门控放行
	err := InjectTenantWhereIntoBuilder[struct{}](ctx, struct{}{})
	if err != nil {
		t.Errorf("non-tenant builder must pass through, got %v", err)
	}
}

// TestInjectTenantWhereIntoBuilder_BadSignatureFailsClosed tenant-scoped
// 类型但 Where 签名不符（模拟 ent 升级改签名）时必须 fail-closed 报错，
// 而非静默跳过。这是 B.10 的回归测试：签名失配 = 注入失败 = 拒绝。
func TestInjectTenantWhereIntoBuilder_BadSignatureFailsClosed(t *testing.T) {
	ctx := viewer.WithContext(context.Background(), &stubQueryViewer{tid: 7})
	b := &badSigBuilder{}
	// badSigBuilder 实现 ScopedModel（过类型门控）但 Where 签名不符
	err := InjectTenantWhereIntoBuilder[badSigBuilder](ctx, b)
	if err == nil {
		t.Fatalf("tenant-scoped builder with unexpected Where signature must fail-closed")
	}
}
