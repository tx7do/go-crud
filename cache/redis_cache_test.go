package cache

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedisCache[T any](t *testing.T) (*RedisCache[T], *miniredis.Miniredis) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("failed to start miniredis: %v", err)
	}
	client := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	return NewRedisCache[T](client, time.Minute), s
}

func TestRedisCache_SetGet(t *testing.T) {
	type testType struct {
		A string
		B int
	}
	cache, s := newTestRedisCache[testType](t)
	defer s.Close()
	val := testType{"foo", 42}
	if err := cache.Set(t.Context(), "k1", val); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	got, err := cache.Get(t.Context(), "k1")
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if got != val {
		t.Fatalf("expected %v, got %v", val, got)
	}
}

func TestRedisCache_GetNotFound(t *testing.T) {
	cache, s := newTestRedisCache[string](t)
	defer s.Close()
	_, err := cache.Get(t.Context(), "notfound")
	if err == nil {
		t.Fatal("expected error for not found")
	}
}

func TestRedisCache_SetMarshalError(t *testing.T) {
	type badType struct{ C chan int }
	cache, s := newTestRedisCache[badType](t)
	defer s.Close()
	ch := make(chan int)
	err := cache.Set(t.Context(), "bad", badType{C: ch})
	if err == nil {
		t.Fatal("expected marshal error")
	}
}

func TestRedisCache_GetUnmarshalError(t *testing.T) {
	cache, s := newTestRedisCache[struct{ A int }](t)
	defer s.Close()
	s.Set("badjson", "not-a-json")
	_, err := cache.Get(t.Context(), "badjson")
	if err == nil {
		t.Fatal("expected unmarshal error")
	}
}

func TestRedisCache_Del(t *testing.T) {
	cache, s := newTestRedisCache[string](t)
	defer s.Close()
	if err := cache.Set(t.Context(), "k", "v"); err != nil {
		t.Fatalf("set failed: %v", err)
	}
	if err := cache.Del(t.Context(), "k"); err != nil {
		t.Fatalf("del failed: %v", err)
	}
	_, err := cache.Get(t.Context(), "k")
	if err == nil {
		t.Fatal("expected error after del")
	}
}
