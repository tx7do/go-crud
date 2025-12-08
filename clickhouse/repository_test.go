package clickhouse

import (
	"context"
	"errors"
	"testing"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/stretchr/testify/assert"
	"github.com/tx7do/go-utils/mapper"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

// 为测试定义简单实体类型（没有 deleted_at 字段）
type NoDeleted struct {
	ID int `db:"id"`
}

// 保留已有针对 ListWithPaging 的错误分支测试，并添加更多用例
func TestRepository_ErrorBranches(t *testing.T) {
	ctx := context.Background()

	// 原始请求类型（用于 ListWithPaging）
	req := &paginationV1.PagingRequest{}

	client := createTestClient()
	assert.NotNil(t, client)

	logger := log.NewHelper(log.DefaultLogger)
	noDelMapper := mapper.NewCopierMapper[NoDeleted, NoDeleted]()

	t.Run("ListWithPaging_client is nil", func(t *testing.T) {
		repo := NewRepository[NoDeleted, NoDeleted](nil, noDelMapper, "tmp", logger)

		_, err := repo.ListWithPaging(ctx, req)
		if err == nil {
			t.Fatalf("expected error when client is nil, got nil")
		}
		if !errors.Is(err, errors.New("clickhouse client is nil")) && err.Error() != "clickhouse client is nil" {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ListWithPaging_table is empty", func(t *testing.T) {
		repo := NewRepository[NoDeleted, NoDeleted](client, noDelMapper, "", logger)

		_, err := repo.ListWithPaging(ctx, req)
		if err == nil {
			t.Fatalf("expected error when table is empty, got nil")
		}
		if !errors.Is(err, errors.New("table is empty")) && err.Error() != "table is empty" {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("Create dto is nil", func(t *testing.T) {
		repo := NewRepository[NoDeleted, NoDeleted](client, noDelMapper, "tmp", logger)

		_, err := repo.Create(ctx, nil, nil)
		if err == nil {
			t.Fatalf("expected error when dto is nil, got nil")
		}
		if !errors.Is(err, errors.New("dto is nil")) && err.Error() != "dto is nil" {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("CreateX dto is nil", func(t *testing.T) {
		repo := NewRepository[NoDeleted, NoDeleted](client, noDelMapper, "tmp", logger)

		_, err := repo.CreateX(ctx, nil, nil)
		if err == nil {
			t.Fatalf("expected error when dto is nil for CreateX, got nil")
		}
		if !errors.Is(err, errors.New("dto is nil")) && err.Error() != "dto is nil" {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("BatchCreate empty dtos returns nil", func(t *testing.T) {
		repo := NewRepository[NoDeleted, NoDeleted](client, noDelMapper, "tmp", logger)

		res, err := repo.BatchCreate(ctx, []*NoDeleted{}, nil)
		if err != nil {
			t.Fatalf("expected no error for empty dtos, got: %v", err)
		}
		if res != nil {
			t.Fatalf("expected nil result slice for empty dtos, got: %v", res)
		}
	})

	t.Run("SoftDelete unsupported when no deleted_at field", func(t *testing.T) {
		repo := NewRepository[NoDeleted, NoDeleted](client, noDelMapper, "tmp", logger)

		_, err := repo.SoftDelete(ctx)
		if err == nil {
			t.Fatalf("expected error for soft delete unsupported, got nil")
		}
		expected := "soft delete not supported: deleted_at field not found on entity"
		if err.Error() != expected {
			t.Fatalf("unexpected error: %v, want: %s", err, expected)
		}
	})
}
