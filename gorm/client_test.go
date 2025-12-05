package gorm

import (
	"testing"

	"gorm.io/gorm"
)

// TestNewClient_CreateSQLiteDB 验证通过 driverName/dsn 能在内存中创建 DB
func TestNewClient_CreateSQLiteDB(t *testing.T) {
	opts := []Option{
		Option(func(c *Client) {
			c.driverName = "go_sqlite"
			c.dsn = ":memory:"
			c.enableMigrate = false
		}),
	}

	c, err := NewClient(opts...)
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}
	if c == nil || c.DB == nil {
		t.Fatalf("expected DB created, got nil")
	}

	// 简单执行一个 ping 风格操作：创建临时表并删除以验证 DB 可用
	type tmp struct {
		ID uint
	}
	if err = c.Migrator().CreateTable(&tmp{}); err != nil {
		t.Fatalf("create table failed: %v", err)
	}
	if err = c.Migrator().DropTable(&tmp{}); err != nil {
		t.Fatalf("DROP TABLE failed: %v", err)
	}
}

// TestNewClient_BeforeAfterOpen 验证 beforeOpen 和 afterOpen 回调被调用
func TestNewClient_BeforeAfterOpen(t *testing.T) {
	var beforeCalled, afterCalled bool

	opts := []Option{
		Option(func(c *Client) {
			c.driverName = "go_sqlite"
			c.dsn = ":memory:"
			// 注入 beforeOpen
			c.beforeOpen = append(c.beforeOpen, func(db *gorm.DB) error {
				beforeCalled = true
				return nil
			})
			// 注入 afterOpen
			c.afterOpen = append(c.afterOpen, func(db *gorm.DB) error {
				afterCalled = true
				return nil
			})
		}),
	}

	c, err := NewClient(opts...)
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}
	if !beforeCalled {
		t.Fatalf("beforeOpen not called")
	}
	if !afterCalled {
		t.Fatalf("afterOpen not called")
	}
	if c == nil || c.DB == nil {
		t.Fatalf("expected DB created, got nil")
	}
}

// TestNewClient_AutoMigrateWithGetMigrateModels 验证 getMigrateModels 注入的模型会被 AutoMigrate
func TestNewClient_AutoMigrateWithGetMigrateModels(t *testing.T) {
	type Person struct {
		ID   uint
		Name string
	}

	opts := []Option{
		Option(func(c *Client) {
			c.driverName = "go_sqlite"
			c.dsn = ":memory:"
			c.enableMigrate = true
			// 提供非空的 getMigrateModels，避免潜在的 nil 调用
			c.getMigrateModels = func() []interface{} {
				return []interface{}{&Person{}}
			}
		}),
	}

	c, err := NewClient(opts...)
	if err != nil {
		t.Fatalf("NewClient error: %v", err)
	}
	if c == nil || c.DB == nil {
		t.Fatalf("expected DB created, got nil")
	}

	// 检查表是否存在
	has := c.Migrator().HasTable(&Person{})
	if !has {
		t.Fatalf("expected table for Person created by AutoMigrate")
	}
}
