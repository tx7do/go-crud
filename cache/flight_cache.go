package cache

import "golang.org/x/sync/singleflight"

type SingleFlight[T any] struct {
	sf *singleflight.Group
}

func NewSingleFlight[T any]() *SingleFlight[T] {
	return &SingleFlight[T]{sf: &singleflight.Group{}}
}

func (f *SingleFlight[T]) Do(key string, fn func() (T, error)) (T, error) {
	v, err, _ := f.sf.Do(key, func() (any, error) {
		return fn()
	})

	if err != nil {
		var zero T
		return zero, err
	}
	return v.(T), nil
}
