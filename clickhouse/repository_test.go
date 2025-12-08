package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/stretchr/testify/assert"
	"github.com/tx7do/go-utils/mapper"
	"github.com/tx7do/go-utils/trans"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

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

func TestRepository_Candle_CRUD(t *testing.T) {
	ctx := context.Background()

	client := createTestClient()
	assert.NotNil(t, client)

	// 建表（假定 helper 已存在并创建 name 为 "candles" 的表）
	createCandlesTable(client)

	logger := log.NewHelper(log.DefaultLogger)
	candleMapper := mapper.NewCopierMapper[Candle, Candle]()

	repo := NewRepository[Candle, Candle](client, candleMapper, "candles", logger)
	assert.NotNil(t, repo)

	// 插入一条
	now := time.Now().UTC().Truncate(time.Millisecond)
	dto := &Candle{
		Timestamp: trans.Ptr(now),
		Symbol:    trans.Ptr("TEST"),
		Open:      trans.Ptr(1.1),
		High:      trans.Ptr(2.2),
		Low:       trans.Ptr(0.9),
		Close:     trans.Ptr(1.5),
		Volume:    trans.Ptr(100.0),
	}
	created, err := repo.Create(ctx, dto, nil)
	assert.NoError(t, err)
	assert.NotNil(t, created)

	// Exists 应为 true
	exists, err := repo.Exists(ctx, "symbol = ?", "TEST")
	assert.NoError(t, err)
	assert.True(t, exists)

	// 批量插入
	batch := []*Candle{
		{
			Timestamp: trans.Ptr(time.Now().UTC().Add(1 * time.Minute).Truncate(time.Millisecond)),
			Symbol:    trans.Ptr("B1"),
			Open:      trans.Ptr(10.0),
			High:      trans.Ptr(11.0),
			Low:       trans.Ptr(9.0),
			Close:     trans.Ptr(10.5),
			Volume:    trans.Ptr(500.0),
		},
		{
			Timestamp: trans.Ptr(time.Now().UTC().Add(2 * time.Minute).Truncate(time.Millisecond)),
			Symbol:    trans.Ptr("B2"),
			Open:      trans.Ptr(20.0),
			High:      trans.Ptr(21.0),
			Low:       trans.Ptr(19.0),
			Close:     trans.Ptr(20.5),
			Volume:    trans.Ptr(600.0),
		},
	}
	createdBatch, err := repo.BatchCreate(ctx, batch, nil)
	assert.NoError(t, err)
	assert.Len(t, createdBatch, 2)

	// 列表查询（使用 PagingRequest 的简单空请求）
	pagingReq := &paginationV1.PagingRequest{}
	res, err := repo.ListWithPaging(ctx, pagingReq)
	assert.NoError(t, err)
	// 至少包含我们插入的 3 条记录
	assert.GreaterOrEqual(t, int(res.Total), 3)
	assert.GreaterOrEqual(t, len(res.Items), 1)

	// 软删除（依赖 deleted_at 字段存在）
	//softRes, err := repo.SoftDelete(ctx)
	//assert.NoError(t, err)
	//assert.Equal(t, int64(1), softRes)

	// 硬删除（truncate）
	delRes, err := repo.Delete(ctx, true)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), delRes)

	// 删除后 Exists 应为 false
	existsAfter, err := repo.Exists(ctx, "symbol = ?", "TEST")
	assert.NoError(t, err)
	assert.False(t, existsAfter)
}

func TestRepository_Candle_ListWithPaging(t *testing.T) {
	ctx := context.Background()

	client := createTestClient()
	assert.NotNil(t, client)

	// 确保存在 candles 表并清空
	createCandlesTable(client)

	logger := log.NewHelper(log.DefaultLogger)
	candleMapper := mapper.NewCopierMapper[Candle, Candle]()
	repo := NewRepository[Candle, Candle](client, candleMapper, "candles", logger)
	assert.NotNil(t, repo)

	// 硬删除（truncate）
	delRes, err := repo.Delete(ctx, true)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), delRes)

	// 批量插入
	batch := []*Candle{
		{
			Timestamp: trans.Ptr(time.Now().UTC().Add(1 * time.Minute).Truncate(time.Millisecond)),
			Symbol:    trans.Ptr("B1"),
			Open:      trans.Ptr(10.0),
			High:      trans.Ptr(11.0),
			Low:       trans.Ptr(9.0),
			Close:     trans.Ptr(10.5),
			Volume:    trans.Ptr(500.0),
		},
		{
			Timestamp: trans.Ptr(time.Now().UTC().Add(2 * time.Minute).Truncate(time.Millisecond)),
			Symbol:    trans.Ptr("B2"),
			Open:      trans.Ptr(20.0),
			High:      trans.Ptr(21.0),
			Low:       trans.Ptr(19.0),
			Close:     trans.Ptr(20.5),
			Volume:    trans.Ptr(600.0),
		},
	}
	createdBatch, err := repo.BatchCreate(ctx, batch, nil)
	assert.NoError(t, err)
	assert.Len(t, createdBatch, 2)

	// 使用 FieldMask 仅选择 symbol 和 close，并按 timestamp 降序（最新在前）
	req := &paginationV1.PagingRequest{
		FieldMask: &fieldmaskpb.FieldMask{Paths: []string{"symbol", "close"}},
		OrderBy:   []string{"timestamp DESC"},
		// 不显式分页参数 -> 不会强制限制（ListWithPaging 的实现在无分页参数时返回全部）
	}

	res, err := repo.ListWithPaging(ctx, req)
	assert.NoError(t, err)
	// 至少包含我们插入的 2 条
	assert.GreaterOrEqual(t, int(res.Total), 2)
	assert.GreaterOrEqual(t, len(res.Items), 2)

	first := res.Items[0]
	assert.NotNil(t, first)
	assert.Equal(t, "B1", *first.Symbol)

	assert.Nil(t, first.Open)
	if first.Close == nil {
		t.Fatalf("expected Close to be selected by FieldMask, got nil")
	}
	assert.Equal(t, 10.5, *first.Close)
}
