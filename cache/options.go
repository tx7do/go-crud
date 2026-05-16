package cache

import "time"

type Options struct {
	TTL        time.Duration // 缓存的过期时间，默认为 5 分钟
	NoCache    bool          // 是否禁用缓存，默认为 false，即启用缓存
	CacheEmpty bool          // 是否缓存空结果，默认为 false，即不缓存空结果
}

type Option func(*Options)

func WithTTL(ttl time.Duration) Option {
	return func(o *Options) { o.TTL = ttl }
}

func WithNoCache() Option {
	return func(o *Options) { o.NoCache = true }
}

func WithCacheEmpty(enable bool) Option {
	return func(o *Options) { o.CacheEmpty = enable }
}
