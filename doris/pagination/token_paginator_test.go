package pagination

import (
	"strings"
	"testing"

	"github.com/tx7do/go-crud/doris/query"
	"github.com/tx7do/go-crud/pagination"
	"github.com/tx7do/go-crud/pagination/paginator"
)

func withTestSecret(t *testing.T, secret string) {
	t.Helper()
	pagination.SetTokenSecret([]byte(secret))
	t.Cleanup(func() { pagination.SetTokenSecret(nil) })
}

// TestTokenPaginator_SignedTokenDecoded 验证签名 token 被解码为 lastID 并生成
// "id > ?" 条件与请求的 LIMIT。
func TestTokenPaginator_SignedTokenDecoded(t *testing.T) {
	withTestSecret(t, "unit-test-secret")

	tok := pagination.EncodeAndSign(100, pagination.TokenSecret())
	b := query.NewQueryBuilder("t1", nil)
	NewTokenPaginator().BuildClause(b, tok, 25)

	sql, args := b.Build()
	if !strings.Contains(sql, "id > ?") {
		t.Errorf("expected 'id > ?' in SQL, got %q", sql)
	}
	if len(args) == 0 || args[0] != int64(100) {
		t.Errorf("expected bound arg 100, got %v", args)
	}
	if !strings.Contains(sql, "LIMIT 25") {
		t.Errorf("expected 'LIMIT 25' in SQL, got %q", sql)
	}
}

// TestTokenPaginator_InvalidTokenOnlyLimit 验证无法通过验签的 token 只应用 LIMIT，
// 不注入任何 WHERE 条件。
func TestTokenPaginator_InvalidTokenOnlyLimit(t *testing.T) {
	withTestSecret(t, "unit-test-secret")

	b := query.NewQueryBuilder("t1", nil)
	NewTokenPaginator().BuildClause(b, "forged-token", 10)

	sql, args := b.Build()
	if strings.Contains(sql, "id >") {
		t.Errorf("invalid token must not produce WHERE, got %q", sql)
	}
	if len(args) != 0 {
		t.Errorf("invalid token must not bind args, got %v", args)
	}
	if !strings.Contains(sql, "LIMIT 10") {
		t.Errorf("expected 'LIMIT 10' in SQL, got %q", sql)
	}
}

// TestTokenPaginator_LegacyCompatAndRejection 验证旧式未签名 token 的兼容策略：
// 未设置密钥时兼容解码；设置密钥后拒绝（迁移期防伪造）。
func TestTokenPaginator_LegacyCompatAndRejection(t *testing.T) {
	legacy := pagination.EncodeAndSign(5, nil)

	// 无密钥：兼容解码
	b := query.NewQueryBuilder("t1", nil)
	NewTokenPaginator().BuildClause(b, legacy, 10)
	sql, args := b.Build()
	if !strings.Contains(sql, "id > ?") || len(args) == 0 || args[0] != int64(5) {
		t.Errorf("legacy token should decode without secret, SQL=%q args=%v", sql, args)
	}

	// 有密钥：拒绝
	withTestSecret(t, "unit-test-secret")
	b2 := query.NewQueryBuilder("t1", nil)
	NewTokenPaginator().BuildClause(b2, legacy, 10)
	sql2, args2 := b2.Build()
	if strings.Contains(sql2, "id >") || len(args2) != 0 {
		t.Errorf("legacy token must be rejected when secret is set, SQL=%q args=%v", sql2, args2)
	}
}

// TestTokenPaginator_PageSizeCapped 验证 pageSize 受 paginator.MaxLimit 上限约束。
func TestTokenPaginator_PageSizeCapped(t *testing.T) {
	orig := paginator.MaxLimit
	defer func() { paginator.MaxLimit = orig }()
	paginator.MaxLimit = 100

	b := query.NewQueryBuilder("t1", nil)
	NewTokenPaginator().BuildClause(b, "", 4000000000)
	sql, _ := b.Build()
	if !strings.Contains(sql, "LIMIT 100") {
		t.Errorf("pageSize should be capped at MaxLimit, got %q", sql)
	}
}
