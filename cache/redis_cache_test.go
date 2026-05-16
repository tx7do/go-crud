package cache

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

// 辅助函数：创建测试用的 RedisCache + miniredis 实例
func newTestRedisCache[T any](t *testing.T) (*RedisCache[T], *miniredis.Miniredis) {
	t.Helper() // 标记为 helper，报错时显示调用者行号
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	return NewRedisCache[T](client, time.Minute), s
}

// TestRedisCache_SetGet: 基础读写测试
func TestRedisCache_SetGet(t *testing.T) {
	type testType struct {
		A string
		B int
	}
	cache, s := newTestRedisCache[testType](t)
	defer s.Close()

	val := testType{A: "foo", B: 42}

	if err := cache.Set(t.Context(), "k1", &val); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	got, err := cache.Get(t.Context(), "k1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}

	if got == nil {
		t.Fatal("expected non-nil result")
	}
	if *got != val {
		t.Fatalf("expected %v, got %v", val, *got)
	}
}

// TestRedisCache_GetNotFound: 键不存在时返回 (nil, nil)，不是 error
func TestRedisCache_GetNotFound(t *testing.T) {
	cache, s := newTestRedisCache[string](t)
	defer s.Close()

	val, err := cache.Get(t.Context(), "notfound")
	assert.EqualError(t, err, ErrCacheMiss.Error(), "expected ErrCacheMiss for not found")
	assert.Nil(t, val, "expected nil value for not found")
}

// TestRedisCache_SetMarshalError: 测试不可序列化类型的错误处理
func TestRedisCache_SetMarshalError(t *testing.T) {
	type badType struct{ C chan int } // chan 无法被 json.Marshal
	cache, s := newTestRedisCache[badType](t)
	defer s.Close()

	ch := make(chan int)
	err := cache.Set(t.Context(), "bad", &badType{C: ch})

	if err == nil {
		t.Fatal("expected marshal error for chan type")
	}
}

// TestRedisCache_GetUnmarshalError: 测试非法 JSON 的反序列化错误
func TestRedisCache_GetUnmarshalError(t *testing.T) {
	cache, s := newTestRedisCache[struct{ A int }](t)
	defer s.Close()

	// 直接写入非法 JSON 到 miniredis
	_ = s.Set("badjson", "not-a-json")

	val, err := cache.Get(t.Context(), "badjson")

	if err == nil {
		t.Fatal("expected unmarshal error for invalid JSON")
	}
	if val != nil {
		t.Fatalf("expected nil value on unmarshal error, got: %v", *val)
	}
}

// TestRedisCache_Del: 删除后读取应返回 (nil, nil)
func TestRedisCache_Del(t *testing.T) {
	cache, s := newTestRedisCache[string](t)
	defer s.Close()

	val := "test-value"
	if err := cache.Set(t.Context(), "k", &val); err != nil {
		t.Fatalf("set failed: %v", err)
	}

	if err := cache.Del(t.Context(), "k"); err != nil {
		t.Fatalf("del failed: %v", err)
	}

	got, err := cache.Get(t.Context(), "k")
	assert.EqualError(t, err, ErrCacheMiss.Error(), "expected ErrCacheMiss after del")
	if got != nil {
		t.Fatalf("expected nil after del, got: %v", *got)
	}
}

// TestRedisCache_SetGetNil 验证 nil 值的存储与读取（空结果缓存）
func TestRedisCache_SetGetNil(t *testing.T) {
	type User struct {
		ID   int64
		Name string
	}
	cache, s := newTestRedisCache[User](t)
	defer s.Close()

	err := cache.Set(t.Context(), "user:empty", nil)
	assert.NoError(t, err)

	val, err := cache.Get(t.Context(), "user:empty")
	assert.NoError(t, err)
	assert.Nil(t, val, "nil value should be retrieved as nil pointer")
}

// TestRedisCache_BasicTypes 验证泛型支持基本类型
func TestRedisCache_BasicTypes(t *testing.T) {
	tests := []struct {
		name string
		val  int
		key  string
	}{
		{"int", 42, "k:int"},
		{"zero", 0, "k:zero"},
		{"negative", -1, "k:neg"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache, s := newTestRedisCache[int](t)
			defer s.Close()

			err := cache.Set(t.Context(), tt.key, &tt.val)
			assert.NoError(t, err)

			got, err := cache.Get(t.Context(), tt.key)
			assert.NoError(t, err)
			assert.NotNil(t, got)
			assert.Equal(t, tt.val, *got)
		})
	}
}
