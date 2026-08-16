package pagination

import (
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/tx7do/go-crud/pagination"
	"github.com/tx7do/go-crud/pagination/paginator"
)

type tokenRow struct {
	ID   int64 `gorm:"primarykey"`
	Name string
}

func (tokenRow) TableName() string { return "token_paginator_rows" }

func openTokenPaginatorDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file::memory:"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.AutoMigrate(&tokenRow{}); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}
	rows := make([]tokenRow, 0, 150)
	for i := int64(1); i <= 150; i++ {
		rows = append(rows, tokenRow{ID: i, Name: "row"})
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	return db
}

// TestBuildDB_SignedTokenFiltersByID 验证签名 token 解出的 lastID 真正作用于查询：
// 只返回 id > lastID 的行，且条数等于请求的 pageSize。
func TestBuildDB_SignedTokenFiltersByID(t *testing.T) {
	db := openTokenPaginatorDB(t)
	pagination.SetTokenSecret([]byte("unit-test-secret"))
	defer pagination.SetTokenSecret(nil)

	tok := pagination.EncodeAndSign(10, pagination.TokenSecret())

	var rows []tokenRow
	err := NewTokenPaginator().BuildDB(tok, 20)(db.Model(&tokenRow{})).Find(&rows).Error
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(rows) != 20 {
		t.Fatalf("expected 20 rows, got %d", len(rows))
	}
	if rows[0].ID != 11 {
		t.Fatalf("expected first row id=11, got %d", rows[0].ID)
	}
}

// TestBuildDB_InvalidTokenFallsBackToFirstPage 验证验签失败的 token 退化为
// 仅按 pageSize 取第一页，不注入 WHERE。
func TestBuildDB_InvalidTokenFallsBackToFirstPage(t *testing.T) {
	db := openTokenPaginatorDB(t)
	pagination.SetTokenSecret([]byte("unit-test-secret"))
	defer pagination.SetTokenSecret(nil)

	var rows []tokenRow
	err := NewTokenPaginator().BuildDB("forged-token", 20)(db.Model(&tokenRow{})).Find(&rows).Error
	if err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(rows) != 20 {
		t.Fatalf("expected 20 rows, got %d", len(rows))
	}
	if rows[0].ID != 1 {
		t.Fatalf("expected first row id=1, got %d", rows[0].ID)
	}
}

// TestBuildDB_PageSizeRespectedAndCapped 验证修复后 pageSize 真正生效
// （此前 WithPage 在 token 模式下是 no-op，恒为默认 10），并受 MaxLimit 上限约束。
func TestBuildDB_PageSizeRespectedAndCapped(t *testing.T) {
	db := openTokenPaginatorDB(t)

	// pageSize=30 应返回 30 行（修复前恒为 10）
	var rows []tokenRow
	if err := NewTokenPaginator().BuildDB("", 30)(db.Model(&tokenRow{})).Find(&rows).Error; err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(rows) != 30 {
		t.Fatalf("expected 30 rows for pageSize=30, got %d", len(rows))
	}

	// 超大 pageSize 被截断到 MaxLimit
	orig := paginator.MaxLimit
	defer func() { paginator.MaxLimit = orig }()
	paginator.MaxLimit = 100

	var rows2 []tokenRow
	if err := NewTokenPaginator().BuildDB("", 4000000000)(db.Model(&tokenRow{})).Find(&rows2).Error; err != nil {
		t.Fatalf("query error: %v", err)
	}
	if len(rows2) != 100 {
		t.Fatalf("expected capped 100 rows, got %d", len(rows2))
	}
}
