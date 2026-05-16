package cache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type mockRedisCache[T any] struct {
	data     map[string]T
	mu       sync.Mutex
	getCount map[string]int
	setCount map[string]int
	failGet  bool
}

func newMockRedisCache[T any]() *mockRedisCache[T] {
	return &mockRedisCache[T]{
		data:     make(map[string]T),
		getCount: make(map[string]int),
		setCount: make(map[string]int),
	}
}

func (m *mockRedisCache[T]) Get(ctx context.Context, key string) (T, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getCount[key]++
	if m.failGet {
		var zero T
		return zero, errors.New("redis error")
	}
	v, ok := m.data[key]
	if !ok {
		var zero T
		return zero, errors.New("not found")
	}
	return v, nil
}

func (m *mockRedisCache[T]) Set(ctx context.Context, key string, value T) error {
	return m.SetWithTTL(ctx, key, value, 0)
}

func (m *mockRedisCache[T]) SetWithTTL(ctx context.Context, key string, value T, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setCount[key]++
	m.data[key] = value
	return nil
}

func TestCacheSupport_GetOrLoad_CacheHit(t *testing.T) {
	type testType string
	cache := newMockRedisCache[testType]()
	cache.data["k1"] = "v1"
	s := &CacheSupport[testType]{
		Cache:        cache,
		SingleFlight: NewSingleFlight[testType](),
	}
	ctx := context.Background()
	val, err := s.GetOrLoad(ctx, "k1", func() (testType, error) {
		t.Fatal("loader should not be called on cache hit")
		return "", nil
	})
	if err != nil || val != "v1" {
		t.Fatalf("unexpected result: %v, %v", val, err)
	}
}

func TestCacheSupport_GetOrLoad_CacheMiss(t *testing.T) {
	type testType string
	cache := newMockRedisCache[testType]()
	s := &CacheSupport[testType]{
		Cache:        cache,
		SingleFlight: NewSingleFlight[testType](),
	}
	ctx := context.Background()
	loaderCalled := false
	val, err := s.GetOrLoad(ctx, "k2", func() (testType, error) {
		loaderCalled = true
		return "v2", nil
	})
	if err != nil || val != "v2" {
		t.Fatalf("unexpected result: %v, %v", val, err)
	}
	if !loaderCalled {
		t.Fatal("loader should be called on cache miss")
	}
	if cache.data["k2"] != "v2" {
		t.Fatal("value should be set in cache")
	}
}

func TestCacheSupport_GetOrLoad_LoaderError(t *testing.T) {
	type testType string
	cache := newMockRedisCache[testType]()
	s := &CacheSupport[testType]{
		Cache:        cache,
		SingleFlight: NewSingleFlight[testType](),
	}
	ctx := context.Background()
	_, err := s.GetOrLoad(ctx, "k3", func() (testType, error) {
		return "", errors.New("loader error")
	})
	if err == nil || err.Error() != "loader error" {
		t.Fatalf("expected loader error, got: %v", err)
	}
}

func TestCacheSupport_GetOrLoad_ConcurrentSingleFlight(t *testing.T) {
	type testType string
	cache := newMockRedisCache[testType]()
	s := &CacheSupport[testType]{
		Cache:        cache,
		SingleFlight: NewSingleFlight[testType](),
	}
	ctx := context.Background()
	var loaderCount int
	wg := sync.WaitGroup{}
	wg.Add(10)
	results := make([]testType, 10)
	errs := make([]error, 10)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			defer wg.Done()
			val, err := s.GetOrLoad(ctx, "k4", func() (testType, error) {
				loaderCount++
				time.Sleep(20 * time.Millisecond)
				return "v4", nil
			})
			results[idx] = val
			errs[idx] = err
		}(i)
	}
	wg.Wait()
	if loaderCount != 1 {
		t.Fatalf("loader should be called once, got %d", loaderCount)
	}
	for i, err := range errs {
		if err != nil {
			t.Fatalf("unexpected error at %d: %v", i, err)
		}
		if results[i] != "v4" {
			t.Fatalf("unexpected result at %d: %v", i, results[i])
		}
	}
}
