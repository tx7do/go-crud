package paginator

import (
	"testing"

	"github.com/tx7do/go-crud/pagination"
)

func TestClampLimit(t *testing.T) {
	orig := MaxLimit
	defer func() { MaxLimit = orig }()

	MaxLimit = 100
	cases := []struct {
		in, want int
	}{
		{0, 1},
		{-5, 1},
		{1, 1},
		{50, 50},
		{100, 100},
		{101, 100},
		{4000000000, 100},
	}
	for _, c := range cases {
		if got := clampLimit(c.in); got != c.want {
			t.Errorf("clampLimit(%d) = %d, want %d", c.in, got, c.want)
		}
	}

	MaxLimit = 0 // 关闭上限
	if got := clampLimit(1000000); got != 1000000 {
		t.Errorf("clampLimit with MaxLimit=0 should be no-op, got %d", got)
	}
}

func TestPagePaginatorSizeCapped(t *testing.T) {
	orig := MaxLimit
	defer func() { MaxLimit = orig }()
	MaxLimit = 100

	p := NewPagePaginator(1, 4000000000)
	if p.Size() != 100 {
		t.Errorf("NewPagePaginator size should be capped at 100, got %d", p.Size())
	}
	if p.Limit() != 100 {
		t.Errorf("limit should be capped at 100, got %d", p.Limit())
	}

	p = p.WithSize(5000)
	if p.Size() != 100 {
		t.Errorf("WithSize should be capped at 100, got %d", p.Size())
	}
}

func TestOffsetPaginatorLimitCapped(t *testing.T) {
	orig := MaxLimit
	defer func() { MaxLimit = orig }()
	MaxLimit = 100

	p := NewOffsetPaginator(0, 999999)
	if p.Limit() != 100 {
		t.Errorf("offset paginator limit should be capped, got %d", p.Limit())
	}
	p = p.WithLimit(999999)
	if p.Limit() != 100 {
		t.Errorf("WithLimit should be capped, got %d", p.Limit())
	}
}

func TestTokenPaginatorSizeCapped(t *testing.T) {
	orig := MaxLimit
	defer func() { MaxLimit = orig }()
	MaxLimit = 100

	p := NewTokenPaginator("", 999999)
	if p.Size() != 100 {
		t.Errorf("token paginator size should be capped, got %d", p.Size())
	}
	p = p.WithSize(999999)
	if p.Size() != 100 {
		t.Errorf("WithSize should be capped, got %d", p.Size())
	}
}

func TestPaginatorsStillSatisfyInterface(t *testing.T) {
	var _ pagination.Paginator = NewPagePaginator(1, 10)
	var _ pagination.Paginator = NewOffsetPaginator(0, 10)
	var _ pagination.Paginator = NewTokenPaginator("", 10)
}
