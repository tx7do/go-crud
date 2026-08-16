package gorm_test

import (
	"context"
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	gormcrud "github.com/tx7do/go-crud/gorm"
	gormmixin "github.com/tx7do/go-crud/gorm/mixin"
	"github.com/tx7do/go-crud/viewer"
)

// tenantTestEntity 嵌入 TenantID mixin，触发租户隔离强制。
type tenantTestEntity struct {
	gorm.Model
	gormmixin.TenantID
	Name string
}

func openTenantTestDB(t *testing.T) *gorm.DB {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	sqlDB, _ := db.DB()
	sqlDB.SetMaxOpenConns(1)
	if err := db.AutoMigrate(&tenantTestEntity{}); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}
	// 必须注册租户回调（NewClient 路径会自动注册，但这里直接用 gorm.Open
	// 绕过了 NewClient，故手动注册以测试回调逻辑本身）
	if err := gormcrud.RegisterTenantCallbacks(db); err != nil {
		t.Fatalf("register tenant callbacks: %v", err)
	}
	return db
}

// viewerCtx 构造一个指定租户的 viewer Context。
func viewerCtx(tid uint64) context.Context {
	return viewer.WithContext(context.Background(), &stubViewer{tid: tid})
}

// platformViewerCtx 构造一个平台视图（tid==0）的 viewer Context。
func platformViewerCtx() context.Context {
	return viewer.WithContext(context.Background(), &stubViewer{tid: 0})
}

type stubViewer struct{ tid uint64 }

func (s *stubViewer) UserID() uint64                 { return 0 }
func (s *stubViewer) TenantID() uint64               { return s.tid }
func (s *stubViewer) OrgUnitID() uint64              { return 0 }
func (s *stubViewer) Permissions() []string          { return nil }
func (s *stubViewer) Roles() []string                { return nil }
func (s *stubViewer) DataScope() []viewer.DataScope  { return nil }
func (s *stubViewer) TraceID() string                { return "" }
func (s *stubViewer) HasPermission(_, _ string) bool { return false }
func (s *stubViewer) IsPlatformContext() bool        { return s.tid == 0 }
func (s *stubViewer) IsTenantContext() bool          { return s.tid > 0 }
func (s *stubViewer) IsSystemContext() bool          { return false }
func (s *stubViewer) ShouldAudit() bool              { return false }

// TestTenantEnforce_QueryInjectsPredicate 租户业务视图下，查询必须带上
// tenant_id = ? 谓词。
func TestTenantEnforce_QueryInjectsPredicate(t *testing.T) {
	db := openTenantTestDB(t)
	ctx := viewerCtx(7)

	var out tenantTestEntity
	tx := db.WithContext(ctx).Session(&gorm.Session{DryRun: true}).First(&out, 1)
	if tx.Error != nil {
		t.Fatalf("query: %v", tx.Error)
	}
	sql := tx.Statement.SQL.String()
	if !strings.Contains(sql, "tenant_id") {
		t.Errorf("tenant context query must inject tenant_id predicate, got SQL: %q", sql)
	}
	if !strings.Contains(sql, "?") {
		t.Errorf("tenant predicate must be parameterized, got SQL: %q", sql)
	}
}

// TestTenantEnforce_PlatformContextPassThrough 平台视图（tid==0）不注入谓词。
func TestTenantEnforce_PlatformContextPassThrough(t *testing.T) {
	db := openTenantTestDB(t)
	ctx := platformViewerCtx()

	var out tenantTestEntity
	tx := db.WithContext(ctx).Session(&gorm.Session{DryRun: true}).First(&out, 1)
	if tx.Error != nil {
		t.Fatalf("query: %v", tx.Error)
	}
	sql := tx.Statement.SQL.String()
	if strings.Contains(strings.ToLower(sql), "tenant_id") {
		t.Errorf("platform context must NOT inject tenant predicate, got SQL: %q", sql)
	}
}

// TestTenantEnforce_MissingViewerFailClosed 缺 ViewerContext 必须报错。
func TestTenantEnforce_MissingViewerFailClosed(t *testing.T) {
	db := openTenantTestDB(t)
	// 无 viewer context
	var out tenantTestEntity
	tx := db.WithContext(context.Background()).First(&out, 1)
	if tx.Error == nil {
		t.Errorf("missing viewer context must fail-closed, got nil error")
	}
}

// TestTenantEnforce_CreateForcesTenantID 租户业务视图下 Create 强制覆盖
// tenant_id，即使实体设置了其他租户值也会被改写为当前 viewer 的租户。
func TestTenantEnforce_CreateForcesTenantID(t *testing.T) {
	db := openTenantTestDB(t)
	ctx := viewerCtx(7)

	bad := &tenantTestEntity{Name: "x"}
	other := uint32(99)
	// 通过 mixin 字段显式赋值他租户，验证强制覆盖
	tenantField := &bad.TenantID // mixin.TenantID 嵌入实例
	tenantField.TenantID = &other
	if err := db.WithContext(ctx).Create(bad).Error; err != nil {
		t.Fatalf("create: %v", err)
	}
	// 重新读出验证 tenant_id 被强制为 7
	var got tenantTestEntity
	if err := db.WithContext(ctx).First(&got, bad.ID).Error; err != nil {
		t.Fatalf("re-read: %v", err)
	}
	stored := got.TenantID.TenantID
	if stored == nil || *stored != 7 {
		t.Errorf("tenant_id must be force-set to 7, got %v", stored)
	}
}
