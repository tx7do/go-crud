package milvus

import (
	"reflect"
	"testing"

	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tx7do/go-crud/milvus/mixin"
	"github.com/tx7do/go-crud/vector"
)

// schemaEntity 覆盖 schema 构建的各分支。
type schemaEntity struct {
	ID      int64     // 主键（数值）
	Title   string    // VarChar
	Active  bool      // Bool
	Score   float32   // Float
	Emb     []float32 // FloatVector
	SkipMe  string    `milvus:"-"`
	Renamed string    `milvus:"name:alias_name"`
	mixin.TenantID
}

func findField(t *testing.T, schema *entity.Schema, name string) *entity.Field {
	t.Helper()
	for _, f := range schema.Fields {
		if f.Name == name {
			return f
		}
	}
	return nil
}

func TestBuildSchema_FieldMapping(t *testing.T) {
	schema, err := buildSchema[schemaEntity]("coll", 4)
	require.NoError(t, err)
	assert.Equal(t, "coll", schema.CollectionName)

	// 主键：ID → Int64 PK，非自增。
	pk := findField(t, schema, "ID")
	require.NotNil(t, pk)
	assert.True(t, pk.PrimaryKey)
	assert.False(t, pk.AutoID)
	assert.Equal(t, entity.FieldTypeInt64, pk.DataType)
	assert.Equal(t, pk, schema.PKField())

	// tenant_id（mixin 拍平）→ Int64 + partition key。
	tenant := findField(t, schema, "tenant_id")
	require.NotNil(t, tenant)
	assert.Equal(t, entity.FieldTypeInt64, tenant.DataType)
	assert.True(t, tenant.IsPartitionKey)

	// 标量映射。
	assert.Equal(t, entity.FieldTypeVarChar, findField(t, schema, "Title").DataType)
	assert.Equal(t, "65535", findField(t, schema, "Title").TypeParams[entity.TypeParamMaxLength])
	assert.Equal(t, entity.FieldTypeBool, findField(t, schema, "Active").DataType)
	assert.Equal(t, entity.FieldTypeFloat, findField(t, schema, "Score").DataType)

	// 向量字段 → FloatVector + dim（取自 dims 参数）。
	emb := findField(t, schema, "Emb")
	require.NotNil(t, emb)
	assert.Equal(t, entity.FieldTypeFloatVector, emb.DataType)
	assert.Equal(t, "4", emb.TypeParams[entity.TypeParamDim])

	// "-" 标签跳过；标签名优先于字段名。
	assert.Nil(t, findField(t, schema, "SkipMe"))
	assert.NotNil(t, findField(t, schema, "alias_name"))
	assert.Nil(t, findField(t, schema, "Renamed"))
}

func TestBuildSchema_Rejections(t *testing.T) {
	type unsignedEntity struct {
		ID  int64
		U   uint32
		Emb []float32
	}
	_, err := buildSchema[unsignedEntity]("c", 4)
	assert.ErrorIs(t, err, ErrSchemaBuildFailed)

	type namedVecEntity struct {
		ID  int64
		Emb vector.Float32Vector
	}
	_, err = buildSchema[namedVecEntity]("c", 4)
	assert.ErrorIs(t, err, ErrSchemaBuildFailed)

	type noVecEntity struct {
		ID int64
	}
	_, err = buildSchema[noVecEntity]("c", 4)
	assert.ErrorIs(t, err, ErrSchemaBuildFailed)

	type noPkEntity struct {
		Emb []float32
	}
	_, err = buildSchema[noPkEntity]("c", 4)
	assert.ErrorIs(t, err, ErrInvalidPointID)

	type badPkEntity struct {
		ID  int32
		Emb []float32
	}
	_, err = buildSchema[badPkEntity]("c", 4)
	assert.ErrorIs(t, err, ErrInvalidPointID)

	// varchar 主键合法。
	type uuidPkEntity struct {
		UUID string
		Emb  []float32
	}
	schema, err := buildSchema[uuidPkEntity]("c", 4)
	require.NoError(t, err)
	require.NotNil(t, schema.PKField())
	assert.Equal(t, entity.FieldTypeVarChar, schema.PKField().DataType)

	// 非法参数。
	_, err = buildSchema[schemaEntity]("", 4)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = buildSchema[schemaEntity]("c", 0)
	assert.ErrorIs(t, err, ErrInvalidRequest)
}

func TestVectorDimsOf(t *testing.T) {
	var e schemaEntity
	specs, err := collectFieldSpecs(reflect.TypeOf(&e).Elem())
	require.NoError(t, err)
	withVec := &schemaEntity{Emb: []float32{1, 2, 3}}
	assert.Equal(t, 3, vectorDimsOf(specs, withVec))
	assert.Equal(t, 0, vectorDimsOf(specs, &schemaEntity{}))
	assert.Equal(t, 0, vectorDimsOf(specs, (*schemaEntity)(nil)))
}
