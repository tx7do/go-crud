package cache

import (
	"context"
	"encoding/json"
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

func (r *RedisCache[T]) Get(ctx context.Context, key string) (T, error) {
	var zero T

	str, err := r.client.Get(ctx, key).Result()
	if err != nil {
		return zero, err
	}

	if err = json.Unmarshal([]byte(str), &zero); err != nil {
		return zero, err
	}

	return zero, nil
}

func (r *RedisCache[T]) Set(ctx context.Context, key string, val T) error {
	return r.SetWithTTL(ctx, key, val, r.ttl)
}

func (r *RedisCache[T]) SetWithTTL(ctx context.Context, key string, val T, ttl time.Duration) error {
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}
	return r.client.Set(ctx, key, data, ttl).Err()
}

func (r *RedisCache[T]) Del(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}
