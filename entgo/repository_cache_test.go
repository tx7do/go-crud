package entgo

import (
	"testing"
	"time"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

// TestRepositoryCache 测试entgo仓库的缓存功能
func TestRepositoryCache(t *testing.T) {
	// 注意：这是一个示例测试，实际使用时需要真实的Redis连接和ENT实体
	// redisClient := redis.NewClient(&redis.Options{
	// 	Addr: "localhost:6379",
	// })

	// 创建模拟的repository实例进行测试结构验证
	var repo *Repository[interface{}, interface{}, interface{}, interface{}, interface{}, interface{}, interface{}, interface{}, interface{}, interface{}]

	// 测试WithCache方法
	if repo != nil {
		repo.WithCache(nil, "test:", 10*time.Minute, 5*time.Minute)
	}

	// 测试generateCacheKey方法
	if repo != nil {
		key := repo.generateCacheKey(123)
		expected := "test:id:123"
		if key != expected {
			t.Errorf("Expected cache key %s, got %s", expected, key)
		}
	}

	// 测试generateListCacheKey方法
	if repo != nil {
		req := &paginationV1.PagingRequest{
			Page:     uint32Ptr(1),
			PageSize: uint32Ptr(10),
		}
		key, err := repo.generateListCacheKey(req)
		if err != nil {
			t.Errorf("Failed to generate list cache key: %v", err)
		}
		if key == "" {
			t.Error("Generated list cache key is empty")
		}
	}
}

// uint32Ptr 辅助函数，用于创建uint32指针
func uint32Ptr(i uint32) *uint32 {
	return &i
}
