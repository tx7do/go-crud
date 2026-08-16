package cache

import "errors"

var ErrCacheMiss = errors.New("cache miss")

// ErrKeyTooLong 在缓存 key 超过 MaxKeyLen 时返回（F-4：防键空间膨胀 DoS）。
var ErrKeyTooLong = errors.New("cache key too long")
