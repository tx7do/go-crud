package entgo

import (
	"testing"

	"github.com/stretchr/testify/assert"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

// TestRepositoryCache 测试entgo仓库的缓存 key 生成。
// 该测试不依赖 Redis 或 ENT 客户端，仅验证缓存 key 的租户隔离属性。
func TestRepositoryCache(t *testing.T) {
	// 创建一个 repository 实例用于验证 key 生成逻辑（无需 DB/Redis）。
	repo := NewRepository[
		interface{}, interface{},
		interface{}, interface{},
		interface{}, interface{},
		interface{},
		interface{}, interface{}, interface{},
	](nil)
	repo.cacheKeyPrefix = "test:"

	// generateCacheKey 现含租户维度段 "t:<tenantID>:"。
	key := repo.generateCacheKey(0, 123)
	assert.Equal(t, "test:t:0:id:123", key)

	// 不同租户对同一 id 应产生不同 key（跨租户隔离）。
	keyTenantA := repo.generateCacheKey(1, 123)
	keyTenantB := repo.generateCacheKey(2, 123)
	assert.NotEqual(t, keyTenantA, keyTenantB)

	// generateListCacheKey 同样按租户隔离。
	req := &paginationV1.PagingRequest{
		Page:     uint32Ptr(1),
		PageSize: uint32Ptr(10),
	}
	keyListA, err := repo.generateListCacheKey(1, req)
	assert.NoError(t, err)
	assert.NotEmpty(t, keyListA)
	keyListB, err := repo.generateListCacheKey(2, req)
	assert.NoError(t, err)
	assert.NotEmpty(t, keyListB)
	assert.NotEqual(t, keyListA, keyListB)
}

// uint32Ptr 辅助函数，用于创建uint32指针
func uint32Ptr(i uint32) *uint32 {
	return &i
}
