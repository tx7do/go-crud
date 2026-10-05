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

// decodeMatrixEntity 覆盖列 → 字段还原的全部标量类型组合。
type decodeMatrixEntity struct {
	B   bool
	I8  int8
	I16 int16
	I32 int32
	I64 int64
	F32 float32
	F64 float64
	S   string
	Emb []float32
	mixin.TenantID
}

func TestEntityFromColumns_TypeMatrix(t *testing.T) {
	// 类型一致的列全部还原。
	cols := client.ResultSet{
		entity.NewColumnBool("B", []bool{true}),
		entity.NewColumnInt8("I8", []int8{1}),
		entity.NewColumnInt16("I16", []int16{2}),
		entity.NewColumnInt32("I32", []int32{3}),
		entity.NewColumnInt64("I64", []int64{4}),
		entity.NewColumnFloat("F32", []float32{5.5}),
		entity.NewColumnDouble("F64", []float64{6.5}),
		entity.NewColumnVarChar("S", []string{"s"}),
		entity.NewColumnInt64("tenant_id", []int64{7}),
	}
	var e decodeMatrixEntity
	require.NoError(t, entityFromColumns(cols, 0, &e))
	assert.True(t, e.B)
	assert.Equal(t, int8(1), e.I8)
	assert.Equal(t, int16(2), e.I16)
	assert.Equal(t, int32(3), e.I32)
	assert.Equal(t, int64(4), e.I64)
	assert.Equal(t, float32(5.5), e.F32)
	assert.Equal(t, 6.5, e.F64)
	assert.Equal(t, "s", e.S)
	require.NotNil(t, e.GetTenantID())
	assert.Equal(t, uint32(7), *e.GetTenantID())

	// 向量字段不回读（无对应列，且解码器跳过向量映射）。
	assert.Nil(t, e.Emb)
}

func TestEntityFromColumns_KindMismatchAndRowBounds(t *testing.T) {
	// 列值类型与字段类型不符 → 字段保持零值。
	mismatched := client.ResultSet{
		entity.NewColumnBool("I8", []bool{true}),
		entity.NewColumnInt64("B", []int64{1}),
		entity.NewColumnInt64("S", []int64{2}),
		entity.NewColumnVarChar("F32", []string{"x"}),
	}
	var e decodeMatrixEntity
	require.NoError(t, entityFromColumns(mismatched, 0, &e))
	assert.False(t, e.B)
	assert.Zero(t, e.I8)
	assert.Equal(t, "", e.S)
	assert.Zero(t, e.F32)

	// 行索引越界 → 全部取值失败 → 字段保持零值。
	var e2 decodeMatrixEntity
	require.NoError(t, entityFromColumns(mismatched, 9, &e2))
	assert.False(t, e2.B)

	// nil 目标 → ErrColumnConversion。
	assert.ErrorIs(t, entityFromColumns(mismatched, 0, (*decodeMatrixEntity)(nil)), ErrColumnConversion)
}
