package pagination

import (
	"testing"

	bsonV2 "go.mongodb.org/mongo-driver/v2/bson"

	"github.com/tx7do/go-crud/mongodb/query"
	"github.com/tx7do/go-crud/pagination"
)

// TestTokenPaginator_SignedTokenDecoded 验证签名 token 被解码为 lastID，
// 生成 {"id": {"$gt": lastID}} 过滤并设置请求的 limit。
func TestTokenPaginator_SignedTokenDecoded(t *testing.T) {
	pagination.SetTokenSecret([]byte("unit-test-secret"))
	defer pagination.SetTokenSecret(nil)

	tok := pagination.EncodeAndSign(100, pagination.TokenSecret())
	b := query.NewQueryBuilder()
	NewTokenPaginator().BuildClause(b, tok, 25)

	filter, opts := b.Build()
	gt, ok := filter["id"].(bsonV2.M)
	if !ok {
		t.Fatalf("expected filter['id'] to be bson.M, got %T", filter["id"])
	}
	if gt["$gt"] != int64(100) {
		t.Errorf("expected $gt=100, got %v", gt["$gt"])
	}
	if opts == nil || opts.Limit == nil || *opts.Limit != int64(25) {
		t.Errorf("expected limit 25, got %+v", opts)
	}
}

// TestTokenPaginator_InvalidTokenOnlyLimit 验证无法通过验签的 token 只设置 limit，
// 不注入过滤条件。
func TestTokenPaginator_InvalidTokenOnlyLimit(t *testing.T) {
	pagination.SetTokenSecret([]byte("unit-test-secret"))
	defer pagination.SetTokenSecret(nil)

	b := query.NewQueryBuilder()
	NewTokenPaginator().BuildClause(b, "forged-token", 10)

	filter, opts := b.Build()
	if len(filter) != 0 {
		t.Errorf("invalid token must not set filter, got %v", filter)
	}
	if opts == nil || opts.Limit == nil || *opts.Limit != int64(10) {
		t.Errorf("expected limit 10, got %+v", opts)
	}
}

// TestTokenPaginator_EmptyTokenOnlyLimit 验证空 token（首页）只设置 limit。
func TestTokenPaginator_EmptyTokenOnlyLimit(t *testing.T) {
	b := query.NewQueryBuilder()
	NewTokenPaginator().BuildClause(b, "", 15)

	filter, opts := b.Build()
	if len(filter) != 0 {
		t.Errorf("empty token must not set filter, got %v", filter)
	}
	if opts == nil || opts.Limit == nil || *opts.Limit != int64(15) {
		t.Errorf("expected limit 15, got %+v", opts)
	}
}
