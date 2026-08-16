package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisCacheInterface[T any] interface {
	Get(ctx context.Context, key string) (*T, error)
	Set(ctx context.Context, key string, value *T) error
	SetWithTTL(ctx context.Context, key string, value *T, ttl time.Duration) error
	Del(ctx context.Context, key string) error
}

type LoaderFunc[T any] func(ctx context.Context) (*T, error)

// MaxKeyLen 是缓存 key 的长度上限。key 含 viewer 身份维度（tenant/user/
// orgUnit/dataScope）+ filter/sort/page hash + 记录 ID，正常远小于此值。
// 拒绝过长 key 防止调用方用伪造的超长 ID/viewMask 膨胀键空间（F-4 的
// 应用层缓解；真正的键数上界依赖 Redis 侧 maxmemory + eviction 策略，
// 须由部署侧配置）。
const MaxKeyLen = 512

type CacheSupport[T any] struct {
	Cache        RedisCacheInterface[T]
	SingleFlight *SingleFlight[T]
	TTL          time.Duration
	metrics      MetricsCollector
}

func NewCacheSupport[T any](redisClient *redis.Client, ttl time.Duration, mc MetricsCollector) *CacheSupport[T] {
	if mc == nil {
		mc = &nopMetrics{}
	}
	return &CacheSupport[T]{
		Cache:        NewRedisCache[T](redisClient, ttl),
		SingleFlight: NewSingleFlight[T](),
		TTL:          ttl,
		metrics:      mc,
	}
}

func (c *CacheSupport[T]) GetOrLoad(
	ctx context.Context,
	key string,
	loader LoaderFunc[T],
	opts ...Option,
) (*T, error) {
	var zero *T = nil

	// F-4：拒绝超长 key（防键空间膨胀 DoS）。
	if len(key) > MaxKeyLen {
		c.metrics.IncRequestsTotal(fmt.Sprintf("%T", zero), "error")
		return zero, ErrKeyTooLong
	}

	entityName := fmt.Sprintf("%T", zero)

	config := Options{
		TTL:        c.TTL,
		CacheEmpty: true,
	}
	for _, opt := range opts {
		opt(&config)
	}

	if config.NoCache && loader != nil {
		return c.trackedLoader(ctx, entityName, loader)
	}

	val, err := c.Cache.Get(ctx, key)
	if err == nil {
		c.metrics.IncRequestsTotal(entityName, "hit")
		return val, nil
	}

	if !errors.Is(err, ErrCacheMiss) {
		c.metrics.IncRequestsTotal(entityName, "miss")
		return zero, err
	}

	c.metrics.IncRequestsTotal(entityName, "miss")

	result, err := c.SingleFlight.Do(key, func() (*T, error) {
		// singleflight 闭包只执行一次但结果共享给所有并发等待者：
		// 1) 使用脱离取消的 ctx——首个调用者取消时不能把所有等待者一起打断，
		//    更不能让取消错误被当作加载结果传播；
		// 2) 加载错误一律不落缓存——此前会把 (nil, err) 负缓存 5 秒，
		//    瞬时 DB 错误被掩盖成"不存在"，影响存在性判断类调用方。
		loadCtx := context.WithoutCancel(ctx)

		var dbData *T
		var dbErr error
		if loader != nil {
			dbData, dbErr = loader(loadCtx)
		}

		if dbErr != nil {
			c.metrics.IncRequestsTotal(entityName, "error")
			return zero, dbErr
		}

		if dbData == nil {
			if config.CacheEmpty {
				// 真正的空结果（loader 成功返回 nil）短 TTL 负缓存，防缓存穿透
				_ = c.Cache.SetWithTTL(loadCtx, key, zero, 5*time.Second)
			}
			return zero, nil
		}

		_ = c.Cache.SetWithTTL(loadCtx, key, dbData, config.TTL)
		return dbData, nil
	})

	if err != nil {
		return zero, err
	}

	return result, nil
}

func (c *CacheSupport[T]) trackedLoader(ctx context.Context, entityName string, loader LoaderFunc[T]) (*T, error) {
	startTime := time.Now()
	defer func() {
		c.metrics.ObserveLoaderDuration(entityName, time.Since(startTime).Seconds())
	}()
	return loader(ctx)
}
