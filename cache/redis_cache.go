package cache

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisCache[T any] struct {
	client *redis.Client
	ttl    time.Duration
}

func NewRedisCache[T any](client *redis.Client, ttl time.Duration) *RedisCache[T] {
	return &RedisCache[T]{
		client: client,
		ttl:    ttl,
	}
}

func (r *RedisCache[T]) Get(ctx context.Context, key string) (*T, error) {
	str, err := r.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrCacheMiss
		}
		return nil, err
	}
	if str == "" {
		return nil, nil // 空字符串表示「缓存空值」
	}

	var val T
	if err = json.Unmarshal([]byte(str), &val); err != nil {
		return nil, err
	}

	return &val, nil
}

func (r *RedisCache[T]) Set(ctx context.Context, key string, val *T) error {
	return r.SetWithTTL(ctx, key, val, r.ttl)
}

func (r *RedisCache[T]) SetWithTTL(ctx context.Context, key string, val *T, ttl time.Duration) error {
	if val == nil {
		return r.client.Set(ctx, key, "", ttl).Err() // 存储空字符串表示「缓存空值」
	}

	data, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return r.client.Set(ctx, key, data, ttl).Err()
}

func (r *RedisCache[T]) Del(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}
