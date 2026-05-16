package cache

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestSingleFlight_Do_SingleExecution(t *testing.T) {
	group := NewSingleFlight[int]()
	var count int
	wg := sync.WaitGroup{}
	wg.Add(10)
	results := make([]int, 10)
	errs := make([]error, 10)

	for i := 0; i < 10; i++ {
		go func(idx int) {
			defer wg.Done()
			res, err := group.Do("key", func() (int, error) {
				time.Sleep(50 * time.Millisecond)
				count++
				return 42, nil
			})
			results[idx] = res
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	if count != 1 {
		t.Errorf("expected fn to be called once, got %d", count)
	}
	for i, err := range errs {
		if err != nil {
			t.Errorf("unexpected error at %d: %v", i, err)
		}
		if results[i] != 42 {
			t.Errorf("unexpected result at %d: %v", i, results[i])
		}
	}
}

func TestSingleFlight_Do_DifferentKeys(t *testing.T) {
	group := NewSingleFlight[int]()
	var mu sync.Mutex
	calls := make(map[string]int)
	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		_, _ = group.Do("a", func() (int, error) {
			mu.Lock()
			calls["a"]++
			mu.Unlock()
			return 1, nil
		})
	}()
	go func() {
		defer wg.Done()
		_, _ = group.Do("b", func() (int, error) {
			mu.Lock()
			calls["b"]++
			mu.Unlock()
			return 2, nil
		})
	}()
	wg.Wait()

	if calls["a"] != 1 || calls["b"] != 1 {
		t.Errorf("expected each key to be called once, got: %+v", calls)
	}
}

func TestSingleFlight_Do_ErrorPropagation(t *testing.T) {
	group := NewSingleFlight[int]()
	errTest := errors.New("test error")
	wg := sync.WaitGroup{}
	wg.Add(2)
	errs := make([]error, 2)

	for i := 0; i < 2; i++ {
		go func(idx int) {
			defer wg.Done()
			_, err := group.Do("err", func() (int, error) {
				time.Sleep(10 * time.Millisecond)
				return 0, errTest
			})
			errs[idx] = err
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err == nil || err.Error() != "test error" {
			t.Errorf("expected error at %d, got %v", i, err)
		}
	}
}
