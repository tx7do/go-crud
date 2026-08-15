package cache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type mockRedisCache[T any] struct {
	data     map[string]*T
	mu       sync.Mutex
	getCount map[string]int
	setCount map[string]int
	failGet  bool
}

func newMockRedisCache[T any]() *mockRedisCache[T] {
	return &mockRedisCache[T]{
		data:     make(map[string]*T),
		getCount: make(map[string]int),
		setCount: make(map[string]int),
	}
}

func (m *mockRedisCache[T]) Get(ctx context.Context, key string) (*T, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getCount[key]++

	if m.failGet {
		return nil, errors.New("redis error") // 系统错误才返回 error
	}

	v, ok := m.data[key]
	if !ok {
		return nil, ErrCacheMiss // 键不存在返回 (nil, nil)，不是错误
	}
	return v, nil
}

func (m *mockRedisCache[T]) Set(ctx context.Context, key string, value *T) error {
	return m.SetWithTTL(ctx, key, value, 0)
}

func (m *mockRedisCache[T]) SetWithTTL(ctx context.Context, key string, value *T, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setCount[key]++
	m.data[key] = value
	return nil
}

func (m *mockRedisCache[T]) Del(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

// TestCacheSupport_GetOrLoad_CacheHit: 缓存命中，不触发 loader
func TestCacheSupport_GetOrLoad_CacheHit(t *testing.T) {
	type testType string
	cache := newMockRedisCache[testType]()

	val := testType("v1")
	cache.data["k1"] = &val

	s := &CacheSupport[testType]{
		Cache:        cache,
		SingleFlight: NewSingleFlight[testType](),
		TTL:          time.Minute,
		metrics:      &nopMetrics{},
	}

	ctx := context.Background()
	result, err := s.GetOrLoad(ctx, "k1", func(ctx context.Context) (*testType, error) {
		t.Fatal("loader should not be called on cache hit")
		return nil, nil
	})

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, testType("v1"), *result)
}

// TestCacheSupport_GetOrLoad_CacheMiss: 缓存未命中，触发 loader 并回写
func TestCacheSupport_GetOrLoad_CacheMiss(t *testing.T) {
	type testType string
	cache := newMockRedisCache[testType]()

	s := &CacheSupport[testType]{
		Cache:        cache,
		SingleFlight: NewSingleFlight[testType](),
		TTL:          time.Minute,
		metrics:      &nopMetrics{},
	}

	loaderCalled := false

	result, err := s.GetOrLoad(t.Context(), "k2", func(ctx context.Context) (*testType, error) {
		loaderCalled = true
		val := testType("v2")
		return &val, nil // loader 返回指针
	})

	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, testType("v2"), *result)
	assert.True(t, loaderCalled, "loader should be called on cache miss")

	assert.NotNil(t, cache.data["k2"])
	assert.Equal(t, testType("v2"), *cache.data["k2"])
}

// TestCacheSupport_GetOrLoad_LoaderError: loader 错误时，根据 CacheEmpty 策略处理
func TestCacheSupport_GetOrLoad_LoaderError(t *testing.T) {
	type testType string
	cache := newMockRedisCache[testType]()

	s := &CacheSupport[testType]{
		Cache:        cache,
		SingleFlight: NewSingleFlight[testType](),
		TTL:          time.Minute,
		metrics:      &nopMetrics{},
	}

	ctx := context.Background()
	errTest := errors.New("loader error")

	result, err := s.GetOrLoad(ctx, "k3", func(ctx context.Context) (*testType, error) {
		return nil, errTest
	})

	assert.ErrorIs(t, err, errTest)
	assert.Nil(t, result)

	emptyVal, err := cache.Get(ctx, "k3")
	assert.NoError(t, err)
	assert.Nil(t, emptyVal)
}

// TestCacheSupport_GetOrLoad_ConcurrentSingleFlight: 并发请求合并，loader 只执行一次
func TestCacheSupport_GetOrLoad_ConcurrentSingleFlight(t *testing.T) {
	type testType string
	cache := newMockRedisCache[testType]()

	s := &CacheSupport[testType]{
		Cache:        cache,
		SingleFlight: NewSingleFlight[testType](),
		TTL:          time.Minute,
		metrics:      &nopMetrics{},
	}

	ctx := context.Background()
	var loaderCount int
	var mu sync.Mutex

	wg := sync.WaitGroup{}
	wg.Add(10)
	results := make([]*testType, 10)
	errs := make([]error, 10)

	for i := 0; i < 10; i++ {
		go func(idx int) {
			defer wg.Done()
			val, err := s.GetOrLoad(ctx, "k4", func(ctx context.Context) (*testType, error) {
				mu.Lock()
				loaderCount++
				mu.Unlock()
				time.Sleep(20 * time.Millisecond)
				v := testType("v4")
				return &v, nil
			})
			results[idx] = val
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	assert.Equal(t, 1, loaderCount, "loader should be called exactly once")

	for i := range errs {
		assert.NoError(t, errs[i], "unexpected error at index %d", i)
		assert.NotNil(t, results[i], "result should not be nil at index %d", i)
		if results[i] != nil {
			assert.Equal(t, testType("v4"), *results[i], "unexpected value at index %d", i)
		}
	}
}

func TestCacheSupport_GetOrLoad_CacheEmptyOption(t *testing.T) {
	type testType string
	cache := newMockRedisCache[testType]()

	s := &CacheSupport[testType]{
		Cache:        cache,
		SingleFlight: NewSingleFlight[testType](),
		TTL:          time.Minute,
		metrics:      &nopMetrics{},
	}

	ctx := context.Background()

	// 1. 首次请求：loader 返回空值（业务上「查无数据」）
	loaderCallCount := 0
	result1, err := s.GetOrLoad(ctx, "empty", func(ctx context.Context) (*testType, error) {
		loaderCallCount++
		return nil, nil
	})

	assert.NoError(t, err)
	assert.Nil(t, result1)
	assert.Equal(t, 1, loaderCallCount)

	// 2. 短时间内再次请求：应命中缓存的空值，不触发 loader
	result2, err := s.GetOrLoad(ctx, "empty", func(ctx context.Context) (*testType, error) {
		loaderCallCount++ // 不应执行
		return nil, nil
	})

	assert.NoError(t, err)
	assert.Nil(t, result2)
	assert.Equal(t, 1, loaderCallCount, "loader should not be called on cached empty value")
}

func TestCacheSupport_GetOrLoad_NoCacheOption(t *testing.T) {
	type testType string
	cache := newMockRedisCache[testType]()

	s := &CacheSupport[testType]{
		Cache:        cache,
		SingleFlight: NewSingleFlight[testType](),
		TTL:          time.Minute,
		metrics:      &nopMetrics{},
	}

	ctx := context.Background()
	callCount := 0

	_, _ = s.GetOrLoad(ctx, "k", func(ctx context.Context) (*testType, error) {
		callCount++
		v := testType("v")
		return &v, nil
	}, WithNoCache())

	_, _ = s.GetOrLoad(ctx, "k", func(ctx context.Context) (*testType, error) {
		callCount++
		v := testType("v")
		return &v, nil
	}, WithNoCache())

	assert.Equal(t, 2, callCount)
	assert.Empty(t, cache.data)
}
