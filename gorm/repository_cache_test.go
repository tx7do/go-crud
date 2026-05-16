package gorm

import (
	"context"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-utils/mapper"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// 测试用 User DTO（模拟 protobuf 生成的结构）
type CacheTestUser struct {
	Id   uint64 `json:"id"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

func (u *CacheTestUser) GetId() int64 {
	return int64(u.Id)
}

// 打开测试数据库
func openTestDBForCache(t *testing.T) *gorm.DB {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.AutoMigrate(&testUserEntity{}); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}
	return db
}

// 种子数据
func seedUsersForCache(t *testing.T, db *gorm.DB, users ...testUserEntity) {
	for _, u := range users {
		if err := db.Create(&u).Error; err != nil {
			t.Fatalf("seed user failed: %v", err)
		}
	}
}

// TestRepository_WithCache 测试缓存配置
func TestRepository_WithCache(t *testing.T) {
	m := mapper.NewCopierMapper[CacheTestUser, testUserEntity]()
	repo := NewRepository[CacheTestUser, testUserEntity](m)

	// 配置缓存
	repo.WithCache(nil, "test:", 10*time.Minute, 5*time.Minute)

	assert.NotNil(t, repo.cacheSupportSingle)
	assert.NotNil(t, repo.cacheSupportList)
	assert.Equal(t, "test:", repo.cacheKeyPrefix)
	assert.Equal(t, 10*time.Minute, repo.cacheTTL)
	assert.Equal(t, 5*time.Minute, repo.cacheListTTL)

	// 测试 WithCacheFromRedis
	repo2 := NewRepository[CacheTestUser, testUserEntity](m)
	repo2.WithCacheFromRedis(nil, "test2:", 15*time.Minute)
	assert.Equal(t, 15*time.Minute, repo2.cacheTTL)
	assert.Equal(t, 5*time.Minute, repo2.cacheListTTL) // 15/3 = 5
}

// TestRepository_generateCacheKey 测试单条缓存键生成
func TestRepository_generateCacheKey(t *testing.T) {
	m := mapper.NewCopierMapper[CacheTestUser, testUserEntity]()
	repo := NewRepository[CacheTestUser, testUserEntity](m)
	repo.cacheKeyPrefix = "user:"

	key := repo.generateCacheKey(123)
	assert.Equal(t, "user:id:123", key)

	key2 := repo.generateCacheKey("abc")
	assert.Equal(t, "user:id:abc", key2)
}

// TestRepository_generateListCacheKey 测试列表缓存键生成
func TestRepository_generateListCacheKey(t *testing.T) {
	m := mapper.NewCopierMapper[CacheTestUser, testUserEntity]()
	repo := NewRepository[CacheTestUser, testUserEntity](m)
	repo.cacheKeyPrefix = "user:"

	// 测试 nil 请求
	_, err := repo.generateListCacheKey(nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "paging request is nil")

	// 测试基本分页参数
	page := uint32(1)
	pageSize := uint32(10)
	req := &paginationV1.PagingRequest{
		Page:     &page,
		PageSize: &pageSize,
	}

	key, err := repo.generateListCacheKey(req)
	assert.NoError(t, err)
	assert.NotEmpty(t, key)
	assert.Contains(t, key, "user:list:")

	// 相同参数应生成相同的 key
	key2, err := repo.generateListCacheKey(req)
	assert.NoError(t, err)
	assert.Equal(t, key, key2)

	// 不同参数应生成不同的 key
	page2 := uint32(2)
	req2 := &paginationV1.PagingRequest{
		Page:     &page2,
		PageSize: &pageSize,
	}
	key3, err := repo.generateListCacheKey(req2)
	assert.NoError(t, err)
	assert.NotEqual(t, key, key3)
}

// TestRepository_extractIDFromDTO 测试从 DTO 提取 ID
func TestRepository_extractIDFromDTO(t *testing.T) {
	m := mapper.NewCopierMapper[CacheTestUser, testUserEntity]()
	repo := NewRepository[CacheTestUser, testUserEntity](m)

	// nil DTO
	id := repo.extractIDFromDTO(nil)
	assert.Nil(t, id)

	// 有 GetId 方法的 DTO
	user := &CacheTestUser{Id: 123, Name: "test"}
	id = repo.extractIDFromDTO(user)
	assert.NotNil(t, id)
	assert.Equal(t, int64(123), id)
}

// TestRepository_extractIDFromDB 测试从 GORM DB 提取 ID
func TestRepository_extractIDFromDB(t *testing.T) {
	db := openTestDBForCache(t)
	m := mapper.NewCopierMapper[CacheTestUser, testUserEntity]()
	repo := NewRepository[CacheTestUser, testUserEntity](m)

	// 测试 nil statement
	id := repo.extractIDFromDB(db)
	assert.Nil(t, id)

	// 测试有 where 条件的 DB
	whereDB := db.Where("id = ?", 123)
	id = repo.extractIDFromDB(whereDB)
	assert.NotNil(t, id)
	assert.Equal(t, int64(123), id)
}

// TestRepository_GetByIDWithCache 测试带缓存的按ID查询
func TestRepository_GetByIDWithCache(t *testing.T) {
	db := openTestDBForCache(t)
	ctx := context.Background()

	seedUsersForCache(t, db,
		testUserEntity{Name: "alice", Age: 20},
		testUserEntity{Name: "bob", Age: 30},
	)

	m := mapper.NewCopierMapper[CacheTestUser, testUserEntity]()
	repo := NewRepository[CacheTestUser, testUserEntity](m)

	// 未配置缓存时，应降级到普通查询
	user, err := repo.GetByIDWithCache(ctx, db, 1, nil)
	assert.NoError(t, err)
	assert.NotNil(t, user)
	assert.Equal(t, "alice", user.Name)

	// 配置缓存后
	repo.WithCache(nil, "user:", 10*time.Minute, 5*time.Minute)

	// 第一次查询（缓存未命中）
	user1, err := repo.GetByIDWithCache(ctx, db, 1, nil)
	assert.NoError(t, err)
	assert.NotNil(t, user1)

	// 第二次查询（应命中缓存）
	user2, err := repo.GetByIDWithCache(ctx, db, 1, nil)
	assert.NoError(t, err)
	assert.NotNil(t, user2)
	assert.Equal(t, user1.Id, user2.Id)
}

// TestRepository_ListWithPagingCache 测试带缓存的分页列表查询
func TestRepository_ListWithPagingCache(t *testing.T) {
	db := openTestDBForCache(t)
	ctx := context.Background()

	seedUsersForCache(t, db,
		testUserEntity{Name: "alice", Age: 20},
		testUserEntity{Name: "bob", Age: 30},
		testUserEntity{Name: "carol", Age: 40},
	)

	m := mapper.NewCopierMapper[CacheTestUser, testUserEntity]()
	repo := NewRepository[CacheTestUser, testUserEntity](m)

	page := uint32(1)
	pageSize := uint32(10)
	req := &paginationV1.PagingRequest{
		Page:     &page,
		PageSize: &pageSize,
	}

	// 未配置缓存时，应降级到普通查询
	result, err := repo.ListWithPagingCache(ctx, db, req)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, uint64(3), result.Total)
	assert.Len(t, result.Items, 3)

	// 配置缓存后
	repo.WithCache(nil, "user:", 10*time.Minute, 5*time.Minute)

	// 第一次查询（缓存未命中）
	result1, err := repo.ListWithPagingCache(ctx, db, req)
	assert.NoError(t, err)
	assert.NotNil(t, result1)

	// 第二次查询（应命中缓存）
	result2, err := repo.ListWithPagingCache(ctx, db, req)
	assert.NoError(t, err)
	assert.NotNil(t, result2)
	assert.Equal(t, result1.Total, result2.Total)
	assert.Len(t, result2.Items, len(result1.Items))
}

// TestRepository_CreateWithCache 测试创建记录并失效缓存
func TestRepository_CreateWithCache(t *testing.T) {
	db := openTestDBForCache(t)
	ctx := context.Background()

	m := mapper.NewCopierMapper[CacheTestUser, testUserEntity]()
	repo := NewRepository[CacheTestUser, testUserEntity](m)
	repo.WithCache(nil, "user:", 10*time.Minute, 5*time.Minute)

	// 创建新用户
	newUser := &CacheTestUser{
		Name: "newuser",
		Age:  25,
	}

	created, err := repo.CreateWithCache(ctx, db, newUser, nil)
	assert.NoError(t, err)
	assert.NotNil(t, created)
	assert.Equal(t, "newuser", created.Name)
	assert.Greater(t, created.Id, uint64(0))
}

// TestRepository_UpdateWithCache 测试更新记录并失效缓存
func TestRepository_UpdateWithCache(t *testing.T) {
	db := openTestDBForCache(t)
	ctx := context.Background()

	seedUsersForCache(t, db,
		testUserEntity{Name: "alice", Age: 20},
	)

	m := mapper.NewCopierMapper[CacheTestUser, testUserEntity]()
	repo := NewRepository[CacheTestUser, testUserEntity](m)
	repo.WithCache(nil, "user:", 10*time.Minute, 5*time.Minute)

	// 先查询出用户
	user, err := repo.Get(ctx, db, nil)
	assert.NoError(t, err)
	assert.NotNil(t, user)

	// 更新用户
	user.Age = 25
	updated, err := repo.UpdateWithCache(ctx, db, user, nil)
	assert.NoError(t, err)
	assert.NotNil(t, updated)
	assert.Equal(t, 25, updated.Age)
}

// TestRepository_DeleteWithCache 测试删除记录并失效缓存
func TestRepository_DeleteWithCache(t *testing.T) {
	db := openTestDBForCache(t)
	ctx := context.Background()

	seedUsersForCache(t, db,
		testUserEntity{Name: "alice", Age: 20},
	)

	m := mapper.NewCopierMapper[CacheTestUser, testUserEntity]()
	repo := NewRepository[CacheTestUser, testUserEntity](m)
	repo.WithCache(nil, "user:", 10*time.Minute, 5*time.Minute)

	// 删除用户
	affected, err := repo.DeleteWithCache(ctx, db, 1, false)
	assert.NoError(t, err)
	assert.Greater(t, affected, int64(0))
}

// TestRepository_invalidateCache 测试缓存失效
func TestRepository_invalidateCache(t *testing.T) {
	ctx := context.Background()

	m := mapper.NewCopierMapper[CacheTestUser, testUserEntity]()
	repo := NewRepository[CacheTestUser, testUserEntity](m)

	// 未配置缓存时，不应报错
	repo.invalidateCache(ctx, 123)

	// 配置缓存后
	repo.WithCache(nil, "user:", 10*time.Minute, 5*time.Minute)

	// 失效缓存
	repo.invalidateCache(ctx, 123)
	// 不报错即为成功
}

// TestRepository_GetWithCache 测试带缓存的单条查询
func TestRepository_GetWithCache(t *testing.T) {
	db := openTestDBForCache(t)
	ctx := context.Background()

	seedUsersForCache(t, db,
		testUserEntity{Name: "alice", Age: 20},
	)

	m := mapper.NewCopierMapper[CacheTestUser, testUserEntity]()
	repo := NewRepository[CacheTestUser, testUserEntity](m)

	// 未配置缓存时，应降级到普通查询
	user, err := repo.GetWithCache(ctx, db.Where("id = ?", 1), nil)
	assert.NoError(t, err)
	assert.NotNil(t, user)
	assert.Equal(t, "alice", user.Name)

	// 配置缓存后
	repo.WithCache(nil, "user:", 10*time.Minute, 5*time.Minute)

	// 查询（由于 extractIDFromDB 的限制，可能降级）
	user2, err := repo.GetWithCache(ctx, db.Where("id = ?", 1), nil)
	assert.NoError(t, err)
	assert.NotNil(t, user2)
}

// TestRepository_CacheFallback 测试缓存失败时的降级
func TestRepository_CacheFallback(t *testing.T) {
	db := openTestDBForCache(t)
	ctx := context.Background()

	seedUsersForCache(t, db,
		testUserEntity{Name: "alice", Age: 20},
	)

	m := mapper.NewCopierMapper[CacheTestUser, testUserEntity]()
	repo := NewRepository[CacheTestUser, testUserEntity](m)

	// 不配置缓存
	user, err := repo.GetByIDWithCache(ctx, db, 1, nil)
	assert.NoError(t, err)
	assert.NotNil(t, user)

	listResult, err := repo.ListWithPagingCache(ctx, db, &paginationV1.PagingRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, listResult)
}

// TestRepository_CacheWithFieldMask 测试带字段掩码的缓存
func TestRepository_CacheWithFieldMask(t *testing.T) {
	db := openTestDBForCache(t)
	ctx := context.Background()

	seedUsersForCache(t, db,
		testUserEntity{Name: "alice", Age: 20},
		testUserEntity{Name: "bob", Age: 30},
	)

	m := mapper.NewCopierMapper[CacheTestUser, testUserEntity]()
	repo := NewRepository[CacheTestUser, testUserEntity](m)
	repo.WithCache(nil, "user:", 10*time.Minute, 5*time.Minute)

	// 带字段掩码的查询
	fieldMask := &fieldmaskpb.FieldMask{Paths: []string{"name"}}
	page := uint32(1)
	pageSize := uint32(10)
	req := &paginationV1.PagingRequest{
		Page:      &page,
		PageSize:  &pageSize,
		FieldMask: fieldMask,
	}

	result, err := repo.ListWithPagingCache(ctx, db, req)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result.Items, 2)
}

// TestRepository_CacheConcurrentAccess 测试并发访问缓存
func TestRepository_CacheConcurrentAccess(t *testing.T) {
	db := openTestDBForCache(t)
	ctx := context.Background()

	seedUsersForCache(t, db,
		testUserEntity{Name: "alice", Age: 20},
	)

	m := mapper.NewCopierMapper[CacheTestUser, testUserEntity]()
	repo := NewRepository[CacheTestUser, testUserEntity](m)
	repo.WithCache(nil, "user:", 10*time.Minute, 5*time.Minute)

	// 并发查询同一个用户
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func() {
			user, err := repo.GetByIDWithCache(ctx, db, 1, nil)
			assert.NoError(t, err)
			assert.NotNil(t, user)
			done <- true
		}()
	}

	// 等待所有 goroutine 完成
	for i := 0; i < 10; i++ {
		<-done
	}
}

// BenchmarkRepository_GetByIDWithCache 性能测试：带缓存的查询
func BenchmarkRepository_GetByIDWithCache(b *testing.B) {
	db := openTestDBForCache(&testing.T{})
	ctx := context.Background()

	seedUsersForCache(&testing.T{}, db,
		testUserEntity{Name: "alice", Age: 20},
	)

	m := mapper.NewCopierMapper[CacheTestUser, testUserEntity]()
	repo := NewRepository[CacheTestUser, testUserEntity](m)
	repo.WithCache(nil, "user:", 10*time.Minute, 5*time.Minute)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = repo.GetByIDWithCache(ctx, db, 1, nil)
	}
}

// BenchmarkRepository_ListWithPagingCache 性能测试：带缓存的列表查询
func BenchmarkRepository_ListWithPagingCache(b *testing.B) {
	db := openTestDBForCache(&testing.T{})
	ctx := context.Background()

	seedUsersForCache(&testing.T{}, db,
		testUserEntity{Name: "alice", Age: 20},
		testUserEntity{Name: "bob", Age: 30},
		testUserEntity{Name: "carol", Age: 40},
	)

	m := mapper.NewCopierMapper[CacheTestUser, testUserEntity]()
	repo := NewRepository[CacheTestUser, testUserEntity](m)
	repo.WithCache(nil, "user:", 10*time.Minute, 5*time.Minute)

	page := uint32(1)
	pageSize := uint32(10)
	req := &paginationV1.PagingRequest{
		Page:     &page,
		PageSize: &pageSize,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = repo.ListWithPagingCache(ctx, db, req)
	}
}
