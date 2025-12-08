package mongodb

import (
	"context"
	"testing"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/stretchr/testify/assert"
	"github.com/tx7do/go-utils/mapper"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	mongoV2 "go.mongodb.org/mongo-driver/v2/mongo"
)

// 简单实体类型用于测试
type NoDeleted struct {
	ID int `db:"id"`
}

func TestRepository_ErrorBranches(t *testing.T) {
	ctx := context.Background()
	logger := log.NewHelper(log.DefaultLogger)
	noDelMapper := mapper.NewCopierMapper[NoDeleted, NoDeleted]()

	// 1. ListWithPaging: db 为 nil -> 错误
	repoNilDB := NewRepository[NoDeleted, NoDeleted](nil, "tmp", noDelMapper, logger)
	_, _, err := repoNilDB.ListWithPaging(ctx, &paginationV1.PagingRequest{})
	assert.Error(t, err)
	assert.Equal(t, "mongodb database is nil", err.Error())

	// 2. ListWithPaging: collection 为空 -> 错误
	repoEmptyColl := NewRepository[NoDeleted, NoDeleted](&mongoV2.Database{}, "", noDelMapper, logger)
	_, _, err = repoEmptyColl.ListWithPaging(ctx, &paginationV1.PagingRequest{})
	assert.Error(t, err)
	assert.Equal(t, "collection is empty", err.Error())

	// 3. Create: dto 为 nil -> 错误（在 dto 校验处返回）
	repo := NewRepository[NoDeleted, NoDeleted](&mongoV2.Database{}, "test", noDelMapper, logger)
	_, err = repo.Create(ctx, nil)
	assert.Error(t, err)
	assert.Equal(t, "dto is nil", err.Error())

	// 4. BatchCreate: 空切片应直接返回 nil, nil（无需访问 DB）
	out, err := repo.BatchCreate(ctx, []*NoDeleted{})
	assert.NoError(t, err)
	assert.Nil(t, out)

	// 5. Update: qb 为 nil -> 错误（在参数校验处返回）
	_, err = repo.Update(ctx, nil, map[string]interface{}{"a": 1})
	assert.Error(t, err)
	assert.Equal(t, "query builder is nil for update", err.Error())

	// 6. Delete: qb 为 nil -> 错误（在参数校验处返回）
	_, err = repo.Delete(ctx, nil)
	assert.Error(t, err)
	assert.Equal(t, "query builder is nil for delete", err.Error())
}
