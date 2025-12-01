package sorting

import (
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

// 简单模型用于构建 SQL
type User struct {
	ID        int
	Name      string
	Age       int
	CreatedAt int64
	Score     int
}

func openDryRunDB(t *testing.T) *gorm.DB {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		DryRun: true,
	})
	if err != nil {
		t.Fatalf("failed to open dry-run db: %v", err)
	}
	return db
}

func sqlOfScope(t *testing.T, scope func(*gorm.DB) *gorm.DB) string {
	db := openDryRunDB(t)
	var users []User
	tx := db.Session(&gorm.Session{DryRun: true}).Model(&User{}).Scopes(scope).Find(&users)
	if tx.Error != nil {
		t.Fatalf("unexpected error executing dummy query: %v", tx.Error)
	}
	return tx.Statement.SQL.String()
}

func TestQueryStringSorting_BuildScope_Empty(t *testing.T) {
	qss := NewQueryStringSorting()

	// 空 orderBys 应不产生 ORDER BY
	scope := qss.BuildScope(nil)
	sql := sqlOfScope(t, scope)
	if strings.Contains(strings.ToUpper(sql), "ORDER BY") {
		t.Fatalf("did not expect ORDER BY for empty orderBys, got SQL: %s", sql)
	}
}

func TestQueryStringSorting_BuildScope_Orderings(t *testing.T) {
	qss := NewQueryStringSorting()

	orderBys := []string{"name", "-age", "", "-", "created_at"}
	scope := qss.BuildScope(orderBys)
	sql := sqlOfScope(t, scope)

	up := strings.ToUpper(sql)

	if !strings.Contains(up, "ORDER BY") {
		t.Fatalf("expected ORDER BY in SQL, got: %s", sql)
	}
	// 宽松匹配字段名与方向
	if !strings.Contains(up, "NAME") {
		t.Fatalf("expected ordering by name, got: %s", sql)
	}
	if !strings.Contains(up, "AGE") || !strings.Contains(up, "DESC") {
		t.Fatalf("expected ordering by age DESC, got: %s", sql)
	}
	if !strings.Contains(up, "CREATED_AT") {
		t.Fatalf("expected ordering by created_at, got: %s", sql)
	}
}

func TestQueryStringSorting_BuildScopeWithDefaultField(t *testing.T) {
	qss := NewQueryStringSorting()

	// 当 orderBys 为空时，应使用默认字段和方向
	scope := qss.BuildScopeWithDefaultField(nil, "created_at", true)
	sql := sqlOfScope(t, scope)
	up := strings.ToUpper(sql)
	if !strings.Contains(up, "ORDER BY") || !strings.Contains(up, "CREATED_AT") || !strings.Contains(up, "DESC") {
		t.Fatalf("expected ORDER BY created_at DESC, got: %s", sql)
	}

	// 当提供 orderBys 时，应优先使用 orderBys 而非默认字段
	scope2 := qss.BuildScopeWithDefaultField([]string{"-score"}, "created_at", true)
	sql2 := sqlOfScope(t, scope2)
	up2 := strings.ToUpper(sql2)
	if strings.Contains(up2, "CREATED_AT") {
		t.Fatalf("did not expect default field to be used when orderBys provided, got: %s", sql2)
	}
	if !strings.Contains(up2, "SCORE") || !strings.Contains(up2, "DESC") {
		t.Fatalf("expected ORDER BY score DESC, got: %s", sql2)
	}
}
