package rule_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	// 应用侧 ent 包对框架 ent.Op/ent.Value 做了别名导出，直接复用避免重名。
	"github.com/tx7do/go-crud/entgo/ent"
	"github.com/tx7do/go-crud/entgo/ent/enttest"
	"github.com/tx7do/go-crud/entgo/ent/user"
	"github.com/tx7do/go-crud/entgo/rule"
	"github.com/tx7do/go-crud/viewer"
	_ "github.com/xiaoqidun/entps"
)

// mutationTestViewer 实现 viewer.Context，可配置租户/平台/系统身份。
type mutationTestViewer struct {
	tid      uint64
	platform bool
	system   bool
}

func (s *mutationTestViewer) UserID() uint64                 { return 0 }
func (s *mutationTestViewer) TenantID() uint64               { return s.tid }
func (s *mutationTestViewer) OrgUnitID() uint64              { return 0 }
func (s *mutationTestViewer) Permissions() []string          { return nil }
func (s *mutationTestViewer) Roles() []string                { return nil }
func (s *mutationTestViewer) DataScope() []viewer.DataScope  { return nil }
func (s *mutationTestViewer) TraceID() string                { return "" }
func (s *mutationTestViewer) HasPermission(_, _ string) bool { return false }
func (s *mutationTestViewer) IsPlatformContext() bool        { return s.platform }
func (s *mutationTestViewer) IsTenantContext() bool          { return s.tid > 0 && !s.platform }
func (s *mutationTestViewer) IsSystemContext() bool          { return s.system }
func (s *mutationTestViewer) ShouldAudit() bool              { return false }

func viewerCtx(tid uint64) context.Context {
	return viewer.WithContext(context.Background(), &mutationTestViewer{tid: tid})
}

func platformCtx() context.Context {
	return viewer.WithContext(context.Background(), &mutationTestViewer{platform: true})
}

// newMutationTestClient 打开独立的内存 sqlite 并跑自动迁移。
// enttest.Open 会导入 ent/runtime（policy hook 在此注册到 user.Hooks[0]），
// 因此 TenantPrivacy.EvalMutation 在真实 builder 执行链上生效。
func newMutationTestClient(t *testing.T) *ent.Client {
	t.Helper()
	dsn := fmt.Sprintf("file:tenant_mut_%s?mode=memory&cache=shared&_fk=1",
		strings.ReplaceAll(t.Name(), "/", "_"))
	cli := enttest.Open(t, "sqlite3", dsn)
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

// seedTenantUsers 以租户 tid 的身份创建用户（走 EvalMutation Create 分支，
// tenant_id 被强制覆盖为 tid），返回创建的实体。
func seedTenantUsers(t *testing.T, cli *ent.Client, tid uint64, names ...string) []*ent.User {
	t.Helper()
	ctx := viewerCtx(tid)
	users := make([]*ent.User, 0, len(names))
	for _, n := range names {
		u, err := cli.User.Create().SetName(n).Save(ctx)
		if err != nil {
			t.Fatalf("seed user %q (tenant %d): %v", n, tid, err)
		}
		users = append(users, u)
	}
	return users
}

// countNames 查询指定租户可见的用户名集合（经查询侧租户谓词）。
func countNames(t *testing.T, cli *ent.Client, tid uint64) map[string]bool {
	t.Helper()
	names, err := cli.User.Query().Select(user.FieldName).Strings(viewerCtx(tid))
	if err != nil {
		t.Fatalf("query tenant %d users: %v", tid, err)
	}
	set := make(map[string]bool, len(names))
	for _, n := range names {
		set[n] = true
	}
	return set
}

// TestTenantPrivacy_DeleteOneID_CrossTenantDenied 是上游缺失的关键回归测试：
// DeleteOneID 按主键删除、生成代码本身无租户条件，本测试验证 EvalMutation
// 注入的 WhereP 谓词确实被 DeleteOne 执行路径应用（主键谓词 AND 租户谓词），
// 跨租户主键删除 0 行命中并返回 NotFound。
func TestTenantPrivacy_DeleteOneID_CrossTenantDenied(t *testing.T) {
	cli := newMutationTestClient(t)
	seedTenantUsers(t, cli, 7, "own1", "own2")
	other := seedTenantUsers(t, cli, 9, "other1", "other2")

	err := cli.User.DeleteOneID(other[0].ID).Exec(viewerCtx(7))
	if err == nil {
		t.Fatal("cross-tenant DeleteOneID must be denied")
	}
	if !ent.IsNotFound(err) {
		t.Fatalf("expect NotFound, got %v", err)
	}
	if got := countNames(t, cli, 9); len(got) != 2 {
		t.Fatalf("tenant 9 rows must survive, got %v", got)
	}
}

// TestTenantPrivacy_DeleteOneID_OwnTenantSucceeds 本租户主键删除正常执行。
func TestTenantPrivacy_DeleteOneID_OwnTenantSucceeds(t *testing.T) {
	cli := newMutationTestClient(t)
	own := seedTenantUsers(t, cli, 7, "own1", "own2")
	seedTenantUsers(t, cli, 9, "other1")

	if err := cli.User.DeleteOneID(own[0].ID).Exec(viewerCtx(7)); err != nil {
		t.Fatalf("own-tenant DeleteOneID: %v", err)
	}
	if got := countNames(t, cli, 7); got["own1"] || len(got) != 1 {
		t.Fatalf("own row must be deleted, got %v", got)
	}
	if got := countNames(t, cli, 9); len(got) != 1 {
		t.Fatalf("tenant 9 rows must survive, got %v", got)
	}
}

// TestTenantPrivacy_Delete_BulkTenantScoped 批量 Delete 不带任何 Where 时，
// 注入的租户谓词把删除范围限定在本租户行内（修复前会波及全部租户）。
func TestTenantPrivacy_Delete_BulkTenantScoped(t *testing.T) {
	cli := newMutationTestClient(t)
	seedTenantUsers(t, cli, 7, "own1", "own2")
	seedTenantUsers(t, cli, 9, "other1", "other2")

	n, err := cli.User.Delete().Exec(viewerCtx(7))
	if err != nil {
		t.Fatalf("bulk delete: %v", err)
	}
	if n != 2 {
		t.Fatalf("only tenant-7 rows may be deleted, deleted %d", n)
	}
	if got := countNames(t, cli, 9); len(got) != 2 {
		t.Fatalf("tenant 9 rows must survive bulk delete, got %v", got)
	}
}

// TestTenantPrivacy_UpdateOneID_CrossTenantDenied 跨租户主键更新 0 行命中
// 返回 NotFound，且原行内容不变。
func TestTenantPrivacy_UpdateOneID_CrossTenantDenied(t *testing.T) {
	cli := newMutationTestClient(t)
	seedTenantUsers(t, cli, 7, "own1")
	other := seedTenantUsers(t, cli, 9, "other1")

	if _, err := cli.User.UpdateOneID(other[0].ID).SetName("hacked").Save(viewerCtx(7)); err == nil {
		t.Fatal("cross-tenant UpdateOneID must be denied")
	} else if !ent.IsNotFound(err) {
		t.Fatalf("expect NotFound, got %v", err)
	}
	if got := countNames(t, cli, 9); !got["other1"] || len(got) != 1 {
		t.Fatalf("tenant 9 row must be intact, got %v", got)
	}
}

// TestTenantPrivacy_Update_BulkTenantScoped 批量 Update 不带 Where 时只波及
// 本租户行。
func TestTenantPrivacy_Update_BulkTenantScoped(t *testing.T) {
	cli := newMutationTestClient(t)
	seedTenantUsers(t, cli, 7, "own1", "own2")
	seedTenantUsers(t, cli, 9, "other1", "other2")

	n, err := cli.User.Update().SetName("renamed").Save(viewerCtx(7))
	if err != nil {
		t.Fatalf("bulk update: %v", err)
	}
	if n != 2 {
		t.Fatalf("only tenant-7 rows may be updated, updated %d", n)
	}
	if got := countNames(t, cli, 7); !got["renamed"] || len(got) != 1 {
		t.Fatalf("both tenant 7 rows must be renamed, got %v", got)
	}
	if got := countNames(t, cli, 9); got["renamed"] || len(got) != 2 {
		t.Fatalf("tenant 9 rows must be intact, got %v", got)
	}
}

// TestTenantPrivacy_PlatformContextBypassesInjection 平台视图不注入谓词，
// 可跨租户执行 UpdateOne/DeleteOne。
func TestTenantPrivacy_PlatformContextBypassesInjection(t *testing.T) {
	cli := newMutationTestClient(t)
	own := seedTenantUsers(t, cli, 7, "own1")
	other := seedTenantUsers(t, cli, 9, "other1")

	if _, err := cli.User.UpdateOneID(other[0].ID).SetName("admin-renamed").Save(platformCtx()); err != nil {
		t.Fatalf("platform cross-tenant update: %v", err)
	}
	if err := cli.User.DeleteOneID(own[0].ID).Exec(platformCtx()); err != nil {
		t.Fatalf("platform cross-tenant delete: %v", err)
	}
}

// TestTenantPrivacy_MissingViewerDenied 缺身份时 fail-closed 拒绝删除。
func TestTenantPrivacy_MissingViewerDenied(t *testing.T) {
	cli := newMutationTestClient(t)
	own := seedTenantUsers(t, cli, 7, "own1")

	if err := cli.User.DeleteOneID(own[0].ID).Exec(context.Background()); err == nil {
		t.Fatal("missing viewer must fail-closed")
	}
	if got := countNames(t, cli, 7); len(got) != 1 {
		t.Fatalf("row must survive denied delete, got %v", got)
	}
}

// TestTenantPrivacy_TenantIDFieldGuard 验证 SET tenant_id 的守卫语义与
// 行级谓词互补：冗余设置（同租户）放行，改租户一律拒绝，行不被迁移。
// Update builder 无 SetTenantID（字段 Immutable），守卫针对的是经
// Mutation().SetField 写入的通用/反射路径。
func TestTenantPrivacy_TenantIDFieldGuard(t *testing.T) {
	cli := newMutationTestClient(t)
	own := seedTenantUsers(t, cli, 7, "own1")

	// 冗余设置：旧行、新值、访问者同为租户 7 → 放行
	u := cli.User.UpdateOneID(own[0].ID)
	if err := u.Mutation().SetField("tenant_id", uint32(7)); err != nil {
		t.Fatalf("SetField: %v", err)
	}
	if _, err := u.Save(viewerCtx(7)); err != nil {
		t.Fatalf("redundant tenant_id set must be allowed, got %v", err)
	}
	// 改租户 → 拒绝
	u2 := cli.User.UpdateOneID(own[0].ID)
	if err := u2.Mutation().SetField("tenant_id", uint32(9)); err != nil {
		t.Fatalf("SetField: %v", err)
	}
	if _, err := u2.Save(viewerCtx(7)); err == nil {
		t.Fatal("cross-tenant tenant_id change must be denied")
	}
	if got := countNames(t, cli, 7); len(got) != 1 {
		t.Fatalf("row must stay in tenant 7, got %v", got)
	}
	if got := countNames(t, cli, 9); len(got) != 0 {
		t.Fatalf("tenant 9 must stay empty, got %v", got)
	}
}

// stubBareMutation 是不实现 WhereP 的最小 ent.Mutation（模拟 entql feature
// 未开启的生成代码），用于验证 fail-closed 路径。
type stubBareMutation struct{ op ent.Op }

func (m *stubBareMutation) Op() ent.Op                          { return m.op }
func (m *stubBareMutation) Type() string                        { return "Stub" }
func (m *stubBareMutation) Fields() []string                    { return nil }
func (m *stubBareMutation) Field(string) (ent.Value, bool)      { return nil, false }
func (m *stubBareMutation) SetField(string, ent.Value) error    { return nil }
func (m *stubBareMutation) AddedFields() []string               { return nil }
func (m *stubBareMutation) AddedField(string) (ent.Value, bool) { return nil, false }
func (m *stubBareMutation) AddField(string, ent.Value) error    { return nil }
func (m *stubBareMutation) ClearedFields() []string             { return nil }
func (m *stubBareMutation) FieldCleared(string) bool            { return false }
func (m *stubBareMutation) ClearField(string) error             { return nil }
func (m *stubBareMutation) ResetField(string) error             { return nil }
func (m *stubBareMutation) AddedEdges() []string                { return nil }
func (m *stubBareMutation) AddedIDs(string) []ent.Value         { return nil }
func (m *stubBareMutation) RemovedEdges() []string              { return nil }
func (m *stubBareMutation) RemovedIDs(string) []ent.Value       { return nil }
func (m *stubBareMutation) ClearedEdges() []string              { return nil }
func (m *stubBareMutation) EdgeCleared(string) bool             { return false }
func (m *stubBareMutation) ClearEdge(string) error              { return nil }
func (m *stubBareMutation) ResetEdge(string) error              { return nil }
func (m *stubBareMutation) OldField(context.Context, string) (ent.Value, error) {
	return nil, fmt.Errorf("not supported")
}

// TestTenantPrivacy_MutationWithoutWherePFailsClosed 验证缺 WhereP 的
// mutation 在租户视图下报错而非静默放行（B.10 fail-closed 原则）。
func TestTenantPrivacy_MutationWithoutWherePFailsClosed(t *testing.T) {
	r := rule.TenantPrivacy[uint32]{}
	for _, op := range []ent.Op{ent.OpUpdate, ent.OpUpdateOne, ent.OpDelete, ent.OpDeleteOne} {
		if err := r.EvalMutation(viewerCtx(7), &stubBareMutation{op: op}); err == nil {
			t.Errorf("%s without WhereP must fail-closed", op)
		}
	}
	// 平台视图先于注入短路，无 WhereP 也放行
	if err := r.EvalMutation(platformCtx(), &stubBareMutation{op: ent.OpUpdate}); err != nil {
		t.Errorf("platform context must bypass injection, got %v", err)
	}
}
