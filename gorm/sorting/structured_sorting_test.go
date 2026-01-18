package sorting

import (
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
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

func TestStructuredSorting_BuildScope_Empty(t *testing.T) {
	ss := NewStructuredSorting()

	scope := ss.BuildScope(nil)
	sql := sqlOfScope(t, scope)
	if strings.Contains(strings.ToUpper(sql), "ORDER BY") {
		t.Fatalf("did not expect ORDER BY for empty orders, got SQL: %s", sql)
	}
}

func TestStructuredSorting_BuildScope_Orderings(t *testing.T) {
	ss := NewStructuredSorting()

	orders := []*paginationV1.Sorting{
		{Field: "name", Direction: paginationV1.Sorting_ASC},
		{Field: "age", Direction: paginationV1.Sorting_DESC},
		nil,
		{Field: "", Direction: paginationV1.Sorting_ASC},
		{Field: "created_at", Direction: paginationV1.Sorting_ASC},
	}

	scope := ss.BuildScope(orders)
	sql := sqlOfScope(t, scope)
	up := strings.ToUpper(sql)

	if !strings.Contains(up, "ORDER BY") {
		t.Fatalf("expected ORDER BY in SQL, got: %s", sql)
	}
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

func TestStructuredSorting_BuildScopeWithDefaultField(t *testing.T) {
	ss := NewStructuredSorting()

	// 当 orders 为空时应使用默认字段和方向
	scope := ss.BuildScopeWithDefaultField(nil, "created_at", true)
	sql := sqlOfScope(t, scope)
	up := strings.ToUpper(sql)
	if !strings.Contains(up, "ORDER BY") || !strings.Contains(up, "CREATED_AT") || !strings.Contains(up, "DESC") {
		t.Fatalf("expected ORDER BY created_at DESC, got: %s", sql)
	}

	// 提供 orders 时应优先使用 orders 而非默认字段
	scope2 := ss.BuildScopeWithDefaultField([]*paginationV1.Sorting{{Field: "score", Direction: paginationV1.Sorting_DESC}}, "created_at", true)
	sql2 := sqlOfScope(t, scope2)
	up2 := strings.ToUpper(sql2)
	if strings.Contains(up2, "CREATED_AT") {
		t.Fatalf("did not expect default field to be used when orders provided, got: %s", sql2)
	}
	if !strings.Contains(up2, "SCORE") || !strings.Contains(up2, "DESC") {
		t.Fatalf("expected ORDER BY score DESC, got: %s", sql2)
	}
}
