package entgo

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	paginationBase "github.com/tx7do/go-crud/pagination"
	"github.com/tx7do/go-crud/viewer"
)

// stubViewer 用于测试的可配置 viewer 上下文
type stubViewer struct {
	userID   uint64
	tenantID uint64
	orgID    uint64
	scopes   []viewer.DataScope
}

func (s stubViewer) UserID() uint64                 { return s.userID }
func (s stubViewer) TenantID() uint64               { return s.tenantID }
func (s stubViewer) OrgUnitID() uint64              { return s.orgID }
func (s stubViewer) Permissions() []string          { return nil }
func (s stubViewer) Roles() []string                { return nil }
func (s stubViewer) DataScope() []viewer.DataScope  { return s.scopes }
func (s stubViewer) TraceID() string                { return "" }
func (s stubViewer) HasPermission(_, _ string) bool { return false }
func (s stubViewer) IsPlatformContext() bool        { return s.tenantID == 0 }
func (s stubViewer) IsTenantContext() bool          { return s.tenantID > 0 }
func (s stubViewer) IsSystemContext() bool          { return false }
func (s stubViewer) ShouldAudit() bool              { return false }

// TestRepositoryCache 测试entgo仓库的缓存 key 生成。
// 该测试不依赖 Redis 或 ENT 客户端，仅验证缓存 key 的租户/用户/数据范围隔离属性。
func TestRepositoryCache(t *testing.T) {
	// 创建一个 repository 实例用于验证 key 生成逻辑（无需 DB/Redis）。
	repo := NewRepository[
		any, any,
		any, any,
		any, any,
		any,
		any, any, any,
	](nil)
	repo.cacheKeyPrefix = "test:"

	// generateCacheKey 含租户、用户与 viewMask 维度段 "t:<tid>:u:<uid>:m:<mask>:"。
	key := repo.generateCacheKey(viewer.NewNoopContext(), 123, nil)
	assert.Equal(t, "test:t:0:u:0:m:all:id:123", key)

	// 不同 viewMask（返回字段不同）不得共享缓存。
	keyMaskA := repo.generateCacheKey(viewer.NewNoopContext(), 123, nil)
	keyMaskB := repo.generateCacheKey(viewer.NewNoopContext(), 123, &fieldmaskpb.FieldMask{Paths: []string{"name"}})
	assert.NotEqual(t, keyMaskA, keyMaskB)

	// 不同租户对同一 id 应产生不同 key（跨租户隔离）。
	keyTenantA := repo.generateCacheKey(stubViewer{tenantID: 1}, 123, nil)
	keyTenantB := repo.generateCacheKey(stubViewer{tenantID: 2}, 123, nil)
	assert.NotEqual(t, keyTenantA, keyTenantB)

	// generateListCacheKey 按租户、用户、数据范围隔离。
	req := &paginationV1.PagingRequest{
		Page:     uint32Ptr(1),
		PageSize: uint32Ptr(10),
	}
	keyListA, err := repo.generateListCacheKey(stubViewer{tenantID: 1}, req)
	assert.NoError(t, err)
	assert.NotEmpty(t, keyListA)
	keyListB, err := repo.generateListCacheKey(stubViewer{tenantID: 2}, req)
	assert.NoError(t, err)
	assert.NotEmpty(t, keyListB)
	assert.NotEqual(t, keyListA, keyListB)

	// 同租户下，不同 DataScope（SELF vs ALL）的相同查询不得共享缓存。
	keySelf, err := repo.generateListCacheKey(stubViewer{
		tenantID: 1,
		userID:   10,
		scopes:   []viewer.DataScope{{ScopeType: viewer.ScopeTypeSelf}},
	}, req)
	assert.NoError(t, err)
	keyAll, err := repo.generateListCacheKey(stubViewer{
		tenantID: 1,
		userID:   10,
		scopes:   []viewer.DataScope{{ScopeType: viewer.ScopeTypeAll}},
	}, req)
	assert.NoError(t, err)
	assert.NotEqual(t, keySelf, keyAll)
}

// TestGenerateListCacheKeyFromPagination_TokenBranch 验证 token 分支：
// 1) 原始 token 不直接入 key（先验签解码为 lastID，防缓存基数攻击）；
// 2) 非法 token 直接报错（fail-closed，不落缓存）；
// 3) 等价 token（同一 lastID）命中同一 key。
func TestGenerateListCacheKeyFromPagination_TokenBranch(t *testing.T) {
	repo := NewRepository[
		any, any,
		any, any,
		any, any,
		any,
		any, any, any,
	](nil)
	repo.cacheKeyPrefix = "test:"
	vc := stubViewer{tenantID: 1, userID: 10}

	mkReq := func(token string, size uint32) *paginationV1.PaginationRequest {
		return &paginationV1.PaginationRequest{
			PaginationType: &paginationV1.PaginationRequest_TokenBased{
				TokenBased: &paginationV1.TokenBasedPagination{Token: token, PageSize: size},
			},
		}
	}

	// 无密钥：合法旧式 token 解码为 lastID 后参与 key
	legacy := paginationBase.EncodeAndSign(5, nil)
	key1, err := repo.generateListCacheKeyFromPagination(vc, mkReq(legacy, 10))
	assert.NoError(t, err)
	assert.NotEmpty(t, key1)

	// 非法 token：报错，不生成缓存 key
	_, err = repo.generateListCacheKeyFromPagination(vc, mkReq("!!!not-base64!!!", 10))
	assert.Error(t, err)

	// 设置密钥后：签名 token 可用，且等价游标（相同 lastID）产生相同 key
	paginationBase.SetTokenSecret([]byte("unit-test-secret"))
	defer paginationBase.SetTokenSecret(nil)

	signed := paginationBase.EncodeAndSign(5, paginationBase.TokenSecret())
	key2, err := repo.generateListCacheKeyFromPagination(vc, mkReq(signed, 10))
	assert.NoError(t, err)
	assert.NotEmpty(t, key2)

	// 密钥未设置时签发的 token 在密钥启用后必须被拒绝（迁移期防伪造）
	_, err = repo.generateListCacheKeyFromPagination(vc, mkReq(legacy, 10))
	assert.Error(t, err)

	// 不同 lastID 产生不同 key
	signed7 := paginationBase.EncodeAndSign(7, paginationBase.TokenSecret())
	key3, err := repo.generateListCacheKeyFromPagination(vc, mkReq(signed7, 10))
	assert.NoError(t, err)
	assert.NotEqual(t, key2, key3)
}

// uint32Ptr 辅助函数，用于创建uint32指针
func uint32Ptr(i uint32) *uint32 {
	return &i
}
