package milvus

import (
	"testing"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tx7do/go-crud/milvus/mixin"
)

// decodeEntity 还原目标实体（含拍平的 mixin）。
type decodeEntity struct {
	ID    int64
	Title string
	Score float32
	mixin.TenantID
}

func TestEntityFromColumns_FillsFlattenedFields(t *testing.T) {
	// 构造与 SDK 查询响应同形的列集（VarChar → ColumnVarChar）。
	cols := client.ResultSet{
		entity.NewColumnInt64("ID", []int64{42}),
		entity.NewColumnVarChar("Title", []string{"hello"}),
		entity.NewColumnFloat("Score", []float32{1.5}),
		entity.NewColumnInt64("tenant_id", []int64{7}),
	}

	var e decodeEntity
	require.NoError(t, entityFromColumns(cols, 0, &e))
	assert.Equal(t, int64(42), e.ID)
	assert.Equal(t, "hello", e.Title)
	assert.Equal(t, float32(1.5), e.Score)
	// mixin（匿名嵌入）的 tenant_id 一并还原。
	require.NotNil(t, e.GetTenantID())
	assert.Equal(t, uint32(7), *e.GetTenantID())
}

func TestEntityFromColumns_MissingColumnsLeaveZero(t *testing.T) {
	// 列缺失 / 类型不符 → 字段保持零值，不报错。
	cols := client.ResultSet{
		entity.NewColumnInt64("unrelated", []int64{1}),
	}
	var e decodeEntity
	require.NoError(t, entityFromColumns(cols, 0, &e))
	assert.Equal(t, int64(0), e.ID)
	assert.Equal(t, "", e.Title)
	assert.Nil(t, e.GetTenantID())
}
