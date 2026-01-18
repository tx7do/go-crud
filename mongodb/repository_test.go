package mongodb

import (
	"context"
	"testing"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/stretchr/testify/assert"
	"github.com/tx7do/go-crud/mongodb/query"
	"github.com/tx7do/go-utils/mapper"
	"github.com/tx7do/go-utils/trans"
	bsonV2 "go.mongodb.org/mongo-driver/v2/bson"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

// 简单实体类型用于测试
type NoDeleted struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
}

func TestRepository_ErrorBranches(t *testing.T) {
	ctx := context.Background()
	logger := log.NewHelper(log.DefaultLogger)
	noDelMapper := mapper.NewCopierMapper[NoDeleted, NoDeleted]()

	client := createTestClient()

	// 1. ListWithPaging: db 为 nil -> 错误
	repoNilDB := NewRepository[NoDeleted, NoDeleted](nil, "tmp", noDelMapper, logger)
	_, _, err := repoNilDB.ListWithPaging(ctx, &paginationV1.PagingRequest{})
	assert.Error(t, err)
	assert.Equal(t, "mongodb database is nil", err.Error())

	// 2. ListWithPaging: collection 为空 -> 错误
	repoEmptyColl := NewRepository[NoDeleted, NoDeleted](client, "", noDelMapper, logger)
	_, _, err = repoEmptyColl.ListWithPaging(ctx, &paginationV1.PagingRequest{})
	assert.Error(t, err)
	assert.Equal(t, "collection is empty", err.Error())

	// 3. Create: dto 为 nil -> 错误（在 dto 校验处返回）
	repo := NewRepository[NoDeleted, NoDeleted](client, "test", noDelMapper, logger)
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

// TestRepository_UpdateSuccess 测试 Update 成功更新记录
func TestRepository_UpdateSuccess(t *testing.T) {
	ctx := context.Background()
	logger := log.NewHelper(log.DefaultLogger)
	noDelMapper := mapper.NewCopierMapper[NoDeleted, NoDeleted]()

	client := createTestClient()
	repo := NewRepository[NoDeleted, NoDeleted](client, "test_update", noDelMapper, logger)

	// 插入初始记录
	in := &NoDeleted{ID: 1}
	created, err := repo.Create(ctx, in)
	assert.NoError(t, err)
	assert.NotNil(t, created)
	assert.Equal(t, 1, created.ID)

	// 构建查询条件并执行更新（将 id 从 1 更新为 2）
	qb := query.NewQueryBuilder()
	qb.Where(bsonV2.M{"id": 1})
	updateDoc := bsonV2.M{"$set": bsonV2.M{"id": 2}}

	updated, err := repo.Update(ctx, qb, updateDoc)
	assert.NoError(t, err)
	assert.NotNil(t, updated)
	assert.Equal(t, 2, updated.ID)
}

func TestRepository_CRUDAndList(t *testing.T) {
	ctx := context.Background()
	logger := log.NewHelper(log.DefaultLogger)
	noDelMapper := mapper.NewCopierMapper[NoDeleted, NoDeleted]()

	client := createTestClient()
	repo := NewRepository[NoDeleted, NoDeleted](client, "test_crud_list", noDelMapper, logger)

	// 清空集合
	_, _ = repo.Delete(ctx, query.NewQueryBuilder())

	// Create
	in1 := &NoDeleted{ID: 1}
	in2 := &NoDeleted{ID: 2}
	in3 := &NoDeleted{ID: 3}
	_, err := repo.Create(ctx, in1)
	assert.NoError(t, err)
	_, err = repo.Create(ctx, in2)
	assert.NoError(t, err)
	_, err = repo.Create(ctx, in3)
	assert.NoError(t, err)

	// List all
	all, _, err := repo.ListWithPaging(ctx, &paginationV1.PagingRequest{})
	assert.NoError(t, err)
	assert.Len(t, all, 3)
	ids := map[int]bool{}
	for _, d := range all {
		ids[d.ID] = true
	}
	assert.True(t, ids[1] && ids[2] && ids[3])

	// Get one
	qb := query.NewQueryBuilder()
	qb.Where(bsonV2.M{"id": 2})
	got, err := repo.Get(ctx, qb)
	assert.NoError(t, err)
	assert.Equal(t, 2, got.ID)

	// Update id 2 -> 20
	qb = query.NewQueryBuilder()
	qb.Where(bsonV2.M{"id": 2})
	updated, err := repo.Update(ctx, qb, bsonV2.M{"$set": bsonV2.M{"id": 20}})
	assert.NoError(t, err)
	assert.Equal(t, 20, updated.ID)

	// Delete the updated
	qb = query.NewQueryBuilder()
	qb.Where(bsonV2.M{"id": 20})
	delCount, err := repo.Delete(ctx, qb)
	assert.NoError(t, err)
	assert.EqualValues(t, 1, delCount)

	// Final list expect 2 items (1 and 3)
	final, _, err := repo.ListWithPaging(ctx, &paginationV1.PagingRequest{})
	assert.NoError(t, err)
	assert.Len(t, final, 2)
	finalIDs := map[int]bool{}
	for _, d := range final {
		finalIDs[d.ID] = true
	}
	assert.True(t, finalIDs[1] && finalIDs[3])
}

func TestRepository_ListWithPaging_ConditionQuery(t *testing.T) {
	ctx := context.Background()
	logger := log.NewHelper(log.DefaultLogger)
	noDelMapper := mapper.NewCopierMapper[NoDeleted, NoDeleted]()

	client := createTestClient()
	repo := NewRepository[NoDeleted, NoDeleted](client, "test_list_query", noDelMapper, logger)

	// 清空集合
	_, _ = repo.Delete(ctx, query.NewQueryBuilder())

	// 插入数据
	_, err := repo.Create(ctx, &NoDeleted{ID: 1, Name: "Item1"})
	assert.NoError(t, err)
	_, err = repo.Create(ctx, &NoDeleted{ID: 2, Name: "Item2"})
	assert.NoError(t, err)
	_, err = repo.Create(ctx, &NoDeleted{ID: 3, Name: "Item3"})
	assert.NoError(t, err)

	// 按条件查询 id:2（根据项目的 queryStringFilter 语法，若语法不同请调整 Query 字符串）
	req := &paginationV1.PagingRequest{
		FilteringType: &paginationV1.PagingRequest_Query{
			Query: "{\"id\": 2}",
		},
		NoPaging: trans.Ptr(true),
	}

	results, total, err := repo.ListWithPaging(ctx, req)
	assert.NoError(t, err)
	assert.EqualValues(t, int64(1), total)
	assert.Len(t, results, 1)
	assert.Equal(t, 2, results[0].ID)
}
