package cache

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisCacheInterface[T any] interface {
	Get(ctx context.Context, key string) (T, error)
	Set(ctx context.Context, key string, value T) error
	SetWithTTL(ctx context.Context, key string, value T, ttl time.Duration) error
}

type CacheSupport[T any] struct {
	Cache        RedisCacheInterface[T]
	SingleFlight *SingleFlight[T]
	TTL          time.Duration
}

func NewCacheSupport[T any](redisClient *redis.Client, ttl time.Duration) *CacheSupport[T] {
	return &CacheSupport[T]{
		Cache:        NewRedisCache[T](redisClient, ttl),
		SingleFlight: NewSingleFlight[T](),
		TTL:          ttl,
	}
}

func (c *CacheSupport[T]) GetOrLoad(
	ctx context.Context,
	key string,
	loader func() (T, error),
	opts ...Option,
) (T, error) {
	var zero T

	config := Options{
		TTL:        c.TTL,
		CacheEmpty: true,
	}
	for _, opt := range opts {
		opt(&config)
	}

	if config.NoCache {
		return loader()
	}

	val, err := c.Cache.Get(ctx, key)
	if err == nil {
		return val, nil
	}

	result, err := c.SingleFlight.Do(key, func() (T, error) {
		dbData, dbErr := loader()

		if dbErr != nil {
			if config.CacheEmpty {
				_ = c.Cache.SetWithTTL(ctx, key, zero, 5*time.Second)
			}
			return zero, dbErr
		}

		_ = c.Cache.SetWithTTL(ctx, key, dbData, config.TTL)
		return dbData, nil
	})

	if err != nil {
		return zero, err
	}

	return result, nil
}
