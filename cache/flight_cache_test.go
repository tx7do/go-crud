package cache

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/tx7do/go-utils/trans"
)

// TestSingleFlight_Do_SingleExecution: 验证并发请求只执行一次
func TestSingleFlight_Do_SingleExecution(t *testing.T) {
	group := NewSingleFlight[int]()
	var count int
	var mu sync.Mutex // 保护 count 并发写入

	wg := sync.WaitGroup{}
	wg.Add(10)
	results := make([]*int, 10)
	errs := make([]error, 10)

	for i := 0; i < 10; i++ {
		go func(idx int) {
			defer wg.Done()
			res, err := group.Do("key", func() (*int, error) {
				time.Sleep(50 * time.Millisecond)
				mu.Lock()
				count++
				mu.Unlock()
				return trans.Ptr(42), nil
			})
			results[idx] = res
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	assert.Equal(t, 1, count, "loader should be called exactly once")

	for i := range errs {
		assert.NoError(t, errs[i], "unexpected error at index %d", i)
		assert.NotNil(t, results[i], "result should not be nil at index %d", i)
		if results[i] != nil {
			assert.Equal(t, 42, *results[i], "unexpected value at index %d", i)
		}
	}
}

// TestSingleFlight_Do_DifferentKeys: 验证不同 key 独立执行
func TestSingleFlight_Do_DifferentKeys(t *testing.T) {
	group := NewSingleFlight[int]()
	var mu sync.Mutex
	calls := make(map[string]int)
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		res, _ := group.Do("a", func() (*int, error) {
			mu.Lock()
			defer mu.Unlock()
			calls["a"]++
			return trans.Ptr(1), nil
		})
		_ = res
	}()

	go func() {
		defer wg.Done()
		res, _ := group.Do("b", func() (*int, error) {
			mu.Lock()
			defer mu.Unlock()
			calls["b"]++
			return trans.Ptr(2), nil
		})
		_ = res
	}()

	wg.Wait()

	assert.Equal(t, 1, calls["a"], "key 'a' should be called once")
	assert.Equal(t, 1, calls["b"], "key 'b' should be called once")
}

// TestSingleFlight_Do_ErrorPropagation: 验证错误正确传播
func TestSingleFlight_Do_ErrorPropagation(t *testing.T) {
	group := NewSingleFlight[int]()
	errTest := errors.New("test error")

	wg := sync.WaitGroup{}
	wg.Add(2)
	errs := make([]error, 2)
	results := make([]*int, 2)

	for i := 0; i < 2; i++ {
		go func(idx int) {
			defer wg.Done()
			res, err := group.Do("err", func() (*int, error) {
				time.Sleep(10 * time.Millisecond)
				return nil, errTest
			})
			errs[idx] = err
			results[idx] = res
		}(i)
	}
	wg.Wait()

	for i := range errs {
		assert.ErrorIs(t, errs[i], errTest, "expected test error at index %d", i)
		assert.Nil(t, results[i], "result should be nil on error at index %d", i)
	}
}

// TestSingleFlight_Do_ReturnNilValue  验证空值缓存场景
func TestSingleFlight_Do_ReturnNilValue(t *testing.T) {
	group := NewSingleFlight[string]()

	res, err := group.Do("empty", func() (*string, error) {
		return nil, nil
	})

	assert.NoError(t, err)
	assert.Nil(t, res, "nil value should be returned for empty result")
}

// TestSingleFlight_Do_ConcurrentError 验证并发错误时只执行一次
func TestSingleFlight_Do_ConcurrentError(t *testing.T) {
	group := NewSingleFlight[int]()
	var count int
	var mu sync.Mutex

	wg := sync.WaitGroup{}
	wg.Add(5)
	errs := make([]error, 5)

	for i := 0; i < 5; i++ {
		go func(idx int) {
			defer wg.Done()
			_, err := group.Do("fail", func() (*int, error) {
				time.Sleep(20 * time.Millisecond)
				mu.Lock()
				count++
				mu.Unlock()
				return nil, errors.New("db timeout")
			})
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	assert.Equal(t, 1, count, "loader should be called once even on error")
	for _, err := range errs {
		assert.Error(t, err, "all goroutines should receive the error")
	}
}
