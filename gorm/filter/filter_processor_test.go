package filter

import (
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

// 简单的测试模型
type User struct {
	ID          uint `gorm:"primarykey"`
	Name        string
	Title       string
	Status      string
	CreatedAt   time.Time
	DeletedAt   gorm.DeletedAt
	Preferences string `gorm:"type:json"`
}

func openTestDB(t *testing.T) *gorm.DB {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.AutoMigrate(&User{}); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}
	return db
}

func sqlFor(t *testing.T, db *gorm.DB, apply func(*gorm.DB) *gorm.DB) string {
	tx := db.Session(&gorm.Session{DryRun: true})
	tx = apply(tx.Model(&User{}))
	// 发起一次查询以触发 SQL 构建（DryRun 模式不会实际执行）
	var out []User
	if err := tx.Find(&out).Error; err != nil {
		// DryRun 下一般不会出错，如果出错则记录
		t.Fatalf("build sql error: %v", err)
	}
	if tx.Statement == nil || tx.Statement.SQL.String() == "" {
		return ""
	}
	return tx.Statement.SQL.String()
}

func TestProcessor_BasicOperators(t *testing.T) {
	db := openTestDB(t)
	proc := NewProcessor()
	// Equal
	sql := sqlFor(t, db, func(tx *gorm.DB) *gorm.DB { return proc.Equal(tx, "name", "tom") })
	if sql == "" || !strings.Contains(strings.ToLower(sql), "name") || !strings.Contains(sql, "=") {
		t.Fatalf("Equal sql unexpected: %q", sql)
	}

	// NotEqual
	sql = sqlFor(t, db, func(tx *gorm.DB) *gorm.DB { return proc.NotEqual(tx, "name", "tom") })
	if sql == "" || !strings.Contains(strings.ToLower(sql), "name") || !strings.Contains(strings.ToLower(sql), "not") && !strings.Contains(sql, "!=") && !strings.Contains(sql, " NOT ") {
		// 允许不同方言生成不同形式，主要确保非空且包含 name
		t.Fatalf("NotEqual sql unexpected: %q", sql)
	}

	// IN with JSON array
	sql = sqlFor(t, db, func(tx *gorm.DB) *gorm.DB { return proc.In(tx, "name", `["a","b"]`, nil) })
	if sql == "" || !strings.Contains(strings.ToLower(sql), " in ") {
		t.Fatalf("In(sql json) unexpected: %q", sql)
	}

	// Range / Between
	sql = sqlFor(t, db, func(tx *gorm.DB) *gorm.DB { return proc.Range(tx, "created_at", `["2020-01-01","2021-01-01"]`, nil) })
	if sql == "" || !(strings.Contains(sql, ">=") && strings.Contains(sql, "<=")) {
		t.Fatalf("Range sql unexpected: %q", sql)
	}

	// IsNull / IsNotNull
	sql = sqlFor(t, db, func(tx *gorm.DB) *gorm.DB { return proc.IsNull(tx, "deleted_at") })
	if sql == "" || !strings.Contains(strings.ToLower(sql), "is null") {
		t.Fatalf("IsNull sql unexpected: %q", sql)
	}
	sql = sqlFor(t, db, func(tx *gorm.DB) *gorm.DB { return proc.IsNotNull(tx, "deleted_at") })
	if sql == "" || !strings.Contains(strings.ToLower(sql), "is not null") {
		t.Fatalf("IsNotNull sql unexpected: %q", sql)
	}
}

func TestProcessor_StringOperatorsAndRegex(t *testing.T) {
	db := openTestDB(t)
	proc := NewProcessor()

	// Contains
	sql := sqlFor(t, db, func(tx *gorm.DB) *gorm.DB { return proc.Contains(tx, "title", "go") })
	if sql == "" || !strings.Contains(strings.ToLower(sql), "like") {
		t.Fatalf("Contains sql unexpected: %q", sql)
	}

	// StartsWith
	sql = sqlFor(t, db, func(tx *gorm.DB) *gorm.DB { return proc.StartsWith(tx, "title", "Go") })
	if sql == "" || !strings.Contains(strings.ToLower(sql), "like") {
		t.Fatalf("StartsWith sql unexpected: %q", sql)
	}

	// EndsWith
	sql = sqlFor(t, db, func(tx *gorm.DB) *gorm.DB { return proc.EndsWith(tx, "title", "Lang") })
	if sql == "" || !strings.Contains(strings.ToLower(sql), "like") {
		t.Fatalf("EndsWith sql unexpected: %q", sql)
	}

	// Exact
	sql = sqlFor(t, db, func(tx *gorm.DB) *gorm.DB { return proc.Exact(tx, "status", "active") })
	if sql == "" || !strings.Contains(strings.ToLower(sql), "=") {
		t.Fatalf("Exact sql unexpected: %q", sql)
	}

	// Regex (sqlite uses REGEXP branch in implementation)
	sql = sqlFor(t, db, func(tx *gorm.DB) *gorm.DB { return proc.Regex(tx, "title", `^An?`) })
	if sql == "" {
		t.Fatalf("Regex sql empty")
	}
	// 确保包含正则相关运算符（REGEXP 或 ~ 等）
	if !strings.Contains(strings.ToLower(sql), "regexp") && !strings.Contains(sql, "~") {
		t.Fatalf("Regex sql unexpected: %q", sql)
	}
}

func TestProcessor_ProcessDispatcher(t *testing.T) {
	db := openTestDB(t)
	proc := NewProcessor()

	cases := []struct {
		op      paginationV1.Operator
		field   string
		value   string
		substrs []string
	}{
		{paginationV1.Operator_EQ, "name", "tom", []string{"name", "="}},
		{paginationV1.Operator_IN, "name", `["a","b"]`, []string{" in "}},
		{paginationV1.Operator_BETWEEN, "created_at", `["2020-01-01","2021-01-01"]`, []string{">=", "<="}},
		{paginationV1.Operator_IS_NULL, "deleted_at", "", []string{"is null"}},
		{paginationV1.Operator_SEARCH, "title", "query", []string{"like"}},
	}

	for _, c := range cases {
		sql := sqlFor(t, db, func(tx *gorm.DB) *gorm.DB {
			return proc.Process(tx, c.op, c.field, c.value, nil)
		})
		if sql == "" {
			t.Fatalf("Process(op=%v) produced empty sql", c.op)
		}
		lsql := strings.ToLower(sql)
		for _, s := range c.substrs {
			if !strings.Contains(lsql, strings.ToLower(s)) {
				t.Fatalf("Process(op=%v) sql %q missing %q", c.op, sql, s)
			}
		}
	}
}

func TestProcessor_DatePartAndJsonbHelpers(t *testing.T) {
	db := openTestDB(t)
	proc := NewProcessor()

	// DatePart 返回非空 SQL 片段（DryRun 下检查生成的 SQL）
	sql := sqlFor(t, db, func(tx *gorm.DB) *gorm.DB { return proc.DatePart(tx, "year", "created_at") })
	if sql == "" {
		t.Fatalf("DatePart produced empty sql")
	}
	if !strings.Contains(strings.ToLower(sql), "extract") && !strings.Contains(strings.ToLower(sql), "is not null") {
		t.Fatalf("DatePart sql unexpected: %q", sql)
	}

	// Jsonb 返回非空表达式
	expr := proc.JsonbField(db.Session(&gorm.Session{DryRun: true}), "daily_email", "preferences")
	if expr == "" {
		t.Fatalf("JsonbField returned empty string")
	}

	// Jsonb WHERE 子句可构建
	sql = sqlFor(t, db, func(tx *gorm.DB) *gorm.DB { return proc.Jsonb(tx, "daily_email", "preferences") })
	if sql == "" {
		t.Fatalf("Jsonb produced empty sql")
	}
}

func TestNewProcessor_UsesJSONCodec(t *testing.T) {
	// 确保 NewProcessor 能够正常创建并带有 json codec（避免 panic）
	proc := NewProcessor()
	if proc == nil || proc.codec == nil {
		t.Fatalf("NewProcessor returned nil or missing codec")
	}
	// codec 能解析基本 JSON
	var arr []interface{}
	if err := proc.codec.Unmarshal([]byte(`["a","b"]`), &arr); err != nil {
		t.Fatalf("codec.Unmarshal failed: %v", err)
	}
}
