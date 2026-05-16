module github.com/tx7do/go-crud/cache

go 1.25.0

replace github.com/tx7do/go-crud => ../

require (
	github.com/alicebob/miniredis/v2 v2.38.0
	github.com/redis/go-redis/v9 v9.19.0
	golang.org/x/sync v0.20.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/yuin/gopher-lua v1.1.2 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)
