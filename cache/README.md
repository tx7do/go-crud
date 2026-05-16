# Cache Package

通用缓存支持包，提供基于 Redis 的高性能缓存解决方案，内置防击穿（SingleFlight）机制和指标收集功能。

## 特性

- ✅ **泛型支持** - 完全基于 Go 泛型，类型安全
- ✅ **防击穿保护** - 使用 `singleflight` 防止缓存击穿
- ✅ **空值缓存** - 支持缓存空结果，防止缓存穿透
- ✅ **灵活配置** - 支持自定义 TTL、禁用缓存等选项
- ✅ **指标监控** - 内置 MetricsCollector 接口，支持监控缓存命中率
- ✅ **优雅降级** - 缓存失败时自动回源到数据加载器

## 核心组件

### 1. CacheSupport

`CacheSupport[T]` 是缓存的核心封装，提供了完整的缓存读写逻辑。

```go
type CacheSupport[T any] struct {
    Cache        RedisCacheInterface[T]  // Redis 缓存实现
    SingleFlight *SingleFlight[T]        // 防击穿组件
    TTL          time.Duration           // 默认 TTL
}
```

#### 创建 CacheSupport

```go
import (
    "github.com/redis/go-redis/v9"
    "github.com/tx7do/go-crud/cache"
)

redisClient := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
})

// 创建缓存支持，TTL 为 10 分钟
cacheSupport := cache.NewCacheSupport[string](redisClient, 10*time.Minute, nil)
```

#### GetOrLoad 方法

核心的缓存读取方法，实现了 Cache-Aside 模式：

```go
func (c *CacheSupport[T]) GetOrLoad(
    ctx context.Context,
    key string,
    loader LoaderFunc[T],
    opts ...Option,
) (*T, error)
```

**工作流程：**
1. 尝试从缓存获取数据
2. 如果缓存命中，直接返回
3. 如果缓存未命中，使用 `singleflight` 确保只有一个请求加载数据
4. 加载成功后写入缓存并返回
5. 加载失败时根据配置决定是否缓存空值

**使用示例：**

```go
ctx := context.Background()

// 基本用法
user, err := cacheSupport.GetOrLoad(ctx, "user:123", func(ctx context.Context) (*User, error) {
    // 从数据库加载数据
    return db.GetUserByID(ctx, 123)
})

if err != nil {
    log.Printf("Failed to get user: %v", err)
    return nil, err
}

fmt.Printf("User: %+v\n", user)
```

**带选项的用法：**

```go
// 自定义 TTL
user, err := cacheSupport.GetOrLoad(ctx, "user:123", loader, 
    cache.WithTTL(30*time.Minute))

// 禁用缓存（强制刷新）
user, err := cacheSupport.GetOrLoad(ctx, "user:123", loader, 
    cache.WithNoCache())

// 不缓存空结果
user, err := cacheSupport.GetOrLoad(ctx, "user:123", loader, 
    cache.WithCacheEmpty(false))
```

### 2. RedisCache

基于 Redis 的缓存实现，使用 JSON 序列化。

```go
type RedisCache[T any] struct {
    client *redis.Client
    ttl    time.Duration
}
```

#### 主要方法

- `Get(ctx, key)` - 获取缓存值
- `Set(ctx, key, value)` - 设置缓存值（使用默认 TTL）
- `SetWithTTL(ctx, key, value, ttl)` - 设置缓存值（自定义 TTL）
- `Del(ctx, key)` - 删除缓存

**使用示例：**

```go
redisCache := cache.NewRedisCache[User](redisClient, 10*time.Minute)

// 设置缓存
user := &User{ID: 123, Name: "John"}
err := redisCache.Set(ctx, "user:123", user)

// 获取缓存
cachedUser, err := redisCache.Get(ctx, "user:123")

// 删除缓存
err = redisCache.Del(ctx, "user:123")
```

### 3. SingleFlight

防击穿组件，基于 `golang.org/x/sync/singleflight`。

```go
type SingleFlight[T any] struct {
    sf *singleflight.Group
}
```

**作用：** 当多个并发请求同时请求同一个 key 时，只执行一次加载操作，其他请求等待结果。

**使用示例：**

```go
sf := cache.NewSingleFlight[User]()

result, err := sf.Do("user:123", func() (*User, error) {
    // 这个函数只会被执行一次，即使有多个并发请求
    return db.GetUserByID(ctx, 123)
})
```

### 4. Options

缓存配置选项。

```go
type Options struct {
    TTL        time.Duration // 缓存过期时间
    NoCache    bool          // 是否禁用缓存
    CacheEmpty bool          // 是否缓存空结果
}
```

#### 可用选项

- `WithTTL(ttl)` - 设置自定义 TTL
- `WithNoCache()` - 禁用缓存，直接调用 loader
- `WithCacheEmpty(enable)` - 控制是否缓存空结果

## 高级用法

### 自定义 Metrics Collector

实现 `MetricsCollector` 接口来监控缓存性能：

```go
type MyMetricsCollector struct{}

func (m *MyMetricsCollector) IncRequestsTotal(entity string, status string) {
    // status: "hit", "miss", "error"
    metrics.CacheRequests.WithLabelValues(entity, status).Inc()
}

func (m *MyMetricsCollector) ObserveLoaderDuration(entity string, duration float64) {
    metrics.LoaderDuration.WithLabelValues(entity).Observe(duration)
}

// 使用时传入 collector
cacheSupport := cache.NewCacheSupport[string](
    redisClient, 
    10*time.Minute, 
    &MyMetricsCollector{},
)
```

### 处理空值缓存

当数据库中不存在某条记录时，可以缓存空值以防止缓存穿透：

```go
// 默认会缓存空值（CacheEmpty = true）
result, err := cacheSupport.GetOrLoad(ctx, "user:999", func(ctx context.Context) (*User, error) {
    user, err := db.GetUserByID(ctx, 999)
    if err != nil {
        return nil, err
    }
    if user == nil {
        // 返回 nil 表示数据不存在，会被缓存 5 秒
        return nil, nil
    }
    return user, nil
})

// 如果不希望缓存空值
result, err := cacheSupport.GetOrLoad(ctx, "user:999", loader, 
    cache.WithCacheEmpty(false))
```

### 并发场景下的性能优化

在高并发场景下，`SingleFlight` 可以显著减少数据库压力：

```go
// 模拟 100 个并发请求
var wg sync.WaitGroup
wg.Add(100)

for i := 0; i < 100; i++ {
    go func() {
        defer wg.Done()
        user, err := cacheSupport.GetOrLoad(ctx, "user:123", func(ctx context.Context) (*User, error) {
            // 这个 loader 只会被执行一次
            time.Sleep(100 * time.Millisecond) // 模拟慢查询
            return db.GetUserByID(ctx, 123)
        })
        
        if err != nil {
            log.Printf("Error: %v", err)
            return
        }
        
        fmt.Printf("Got user: %s\n", user.Name)
    }()
}

wg.Wait()
```

## 错误处理

### ErrCacheMiss

缓存未命中时的错误：

```go
val, err := redisCache.Get(ctx, "key")
if errors.Is(err, cache.ErrCacheMiss) {
    // 缓存未命中，需要从数据库加载
    val = loadFromDB()
} else if err != nil {
    // 其他错误（如 Redis 连接失败）
    log.Printf("Redis error: %v", err)
}
```

## 最佳实践

### 1. 合理设置 TTL

```go
// 热点数据：较长 TTL
hotDataCache := cache.NewCacheSupport[Data](redisClient, 30*time.Minute, nil)

// 实时性要求高的数据：较短 TTL
realtimeCache := cache.NewCacheSupport[Data](redisClient, 1*time.Minute, nil)

// 列表数据：中等 TTL
listCache := cache.NewCacheSupport[PagingResult[Data]](redisClient, 5*time.Minute, nil)
```

### 2. 使用有意义的 Key

```go
// 推荐格式：{entity}:{id} 或 {entity}:{query_hash}
key := fmt.Sprintf("user:%d", userID)
key := fmt.Sprintf("users:page:%d:size:%d", page, pageSize)

// 复杂查询使用哈希
hash := md5.Sum([]byte(queryString))
key := fmt.Sprintf("search:%x", hash[:8])
```

### 3. 写操作后失效缓存

```go
func UpdateUser(ctx context.Context, user *User) error {
    // 1. 更新数据库
    err := db.UpdateUser(ctx, user)
    if err != nil {
        return err
    }
    
    // 2. 删除缓存
    cacheKey := fmt.Sprintf("user:%d", user.ID)
    _ = cacheSupport.Cache.Del(ctx, cacheKey)
    
    return nil
}
```

### 4. 监控缓存命中率

```go
type CacheMetrics struct {
    hits   int64
    misses int64
}

func (m *CacheMetrics) IncRequestsTotal(entity string, status string) {
    if status == "hit" {
        atomic.AddInt64(&m.hits, 1)
    } else {
        atomic.AddInt64(&m.misses, 1)
    }
}

func (m *CacheMetrics) HitRate() float64 {
    total := atomic.LoadInt64(&m.hits) + atomic.LoadInt64(&m.misses)
    if total == 0 {
        return 0
    }
    return float64(atomic.LoadInt64(&m.hits)) / float64(total)
}
```

## 完整示例

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "time"

    "github.com/redis/go-redis/v9"
    "github.com/tx7do/go-crud/cache"
)

type User struct {
    ID    int64  `json:"id"`
    Name  string `json:"name"`
    Email string `json:"email"`
}

func main() {
    // 1. 初始化 Redis 客户端
    redisClient := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })

    // 2. 创建缓存支持
    cacheSupport := cache.NewCacheSupport[User](
        redisClient,
        10*time.Minute,
        nil, // 或使用自定义的 MetricsCollector
    )

    ctx := context.Background()

    // 3. 使用缓存获取用户
    userID := int64(123)
    cacheKey := fmt.Sprintf("user:%d", userID)

    user, err := cacheSupport.GetOrLoad(ctx, cacheKey, func(ctx context.Context) (*User, error) {
        // 模拟从数据库加载
        fmt.Println("Loading from database...")
        time.Sleep(100 * time.Millisecond)
        
        return &User{
            ID:    userID,
            Name:  "John Doe",
            Email: "john@example.com",
        }, nil
    })

    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }

    fmt.Printf("User: %+v\n", user)

    // 4. 第二次请求会命中缓存
    user2, err := cacheSupport.GetOrLoad(ctx, cacheKey, func(ctx context.Context) (*User, error) {
        fmt.Println("This should not be printed")
        return nil, nil
    })

    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }

    fmt.Printf("Cached User: %+v\n", user2)

    // 5. 手动删除缓存
    _ = cacheSupport.Cache.Del(ctx, cacheKey)
    fmt.Println("Cache deleted")
}
```

## API 参考

### 类型

- `CacheSupport[T any]` - 缓存支持结构
- `RedisCache[T any]` - Redis 缓存实现
- `SingleFlight[T any]` - 防击穿组件
- `LoaderFunc[T any]` - 数据加载函数类型
- `RedisCacheInterface[T any]` - 缓存接口
- `MetricsCollector` - 指标收集器接口

### 函数

- `NewCacheSupport[T](client, ttl, mc)` - 创建缓存支持
- `NewRedisCache[T](client, ttl)` - 创建 Redis 缓存
- `NewSingleFlight[T]()` - 创建防击穿组件

### 选项

- `WithTTL(ttl)` - 设置 TTL
- `WithNoCache()` - 禁用缓存
- `WithCacheEmpty(enable)` - 控制空值缓存

### 错误

- `ErrCacheMiss` - 缓存未命中错误

## 依赖

- `github.com/redis/go-redis/v9` - Redis 客户端
- `golang.org/x/sync/singleflight` - 防击穿支持
- `encoding/json` - JSON 序列化

## 测试

运行测试：

```bash
go test -v ./cache/...
```

运行基准测试：

```bash
go test -bench=. -benchmem ./cache/...
```

## 许可证

本项目采用 MIT 许可证。
