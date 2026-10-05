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

	// tenant 字段非 int64 → 拒绝。
	type badTenantEntity struct {
		ID     int64
		Tenant string `milvus:"name:tenant_id"`
		Emb    []float32
	}
	_, err = buildSchema[badTenantEntity]("c", 4)
	assert.ErrorIs(t, err, ErrSchemaBuildFailed)

	// 无 Milvus 对应类型且非无符号的字段（结构体）→ 跳过不进 schema。
	type structFieldEntity struct {
		ID  int64
		Foo struct{ X int }
		Emb []float32
	}
	schema, err = buildSchema[structFieldEntity]("c", 4)
	require.NoError(t, err)
	assert.Nil(t, findField(t, schema, "Foo"))

	// 非法参数。
	_, err = buildSchema[schemaEntity]("", 4)
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = buildSchema[schemaEntity]("c", 0)
	assert.ErrorIs(t, err, ErrInvalidRequest)
}

// TestIsUnsignedKind 无符号类别判定矩阵。
func TestIsUnsignedKind(t *testing.T) {
	assert.True(t, isUnsignedKind(reflect.Uint))
	assert.True(t, isUnsignedKind(reflect.Uint8))
	assert.True(t, isUnsignedKind(reflect.Uint16))
	assert.True(t, isUnsignedKind(reflect.Uint32))
	assert.True(t, isUnsignedKind(reflect.Uint64))
	assert.True(t, isUnsignedKind(reflect.Uintptr))
	assert.False(t, isUnsignedKind(reflect.Int))
	assert.False(t, isUnsignedKind(reflect.UnsafePointer))
	assert.False(t, isUnsignedKind(reflect.String))
	assert.False(t, isUnsignedKind(reflect.Struct))
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

// TestMilvusScoreToScore 分数换算：相似度度量透传，L2 距离 → 1/(1+d)。
func TestMilvusScoreToScore(t *testing.T) {
	assert.Equal(t, 0.25, milvusScoreToScore(vector.MetricCosine, 0.25))
	assert.Equal(t, -3.0, milvusScoreToScore(vector.MetricDotProduct, -3))
	assert.Equal(t, 0.25, milvusScoreToScore("", 0.25))
	assert.Equal(t, 1.0, milvusScoreToScore(vector.MetricEuclidean, 0))
	assert.InDelta(t, 1.0/3.0, milvusScoreToScore(vector.MetricEuclidean, 2), 1e-9)
}

// TestResolveVectorFieldSpecs 向量字段解析：匹配放行、未匹配/多字段/缺字段拒绝。
func TestResolveVectorFieldSpecs(t *testing.T) {
	// 单一向量字段：显式匹配；未指定 → 默认该字段。
	specs, err := specsOf[schemaEntity]()
	require.NoError(t, err)
	name, err := resolveVectorFieldSpecs(specs, "Emb")
	require.NoError(t, err)
	assert.Equal(t, "Emb", name)
	name, err = resolveVectorFieldSpecs(specs, "")
	require.NoError(t, err)
	assert.Equal(t, "Emb", name)
	// 未匹配的字段名拒绝。
	_, err = resolveVectorFieldSpecs(specs, "nope")
	assert.ErrorIs(t, err, ErrInvalidVectorQuery)

	// 双向量字段：必须显式指定其一。
	type twoVecEntity struct {
		ID   int64
		Emb  []float32
		Emb2 []float32
	}
	specs2, err := specsOf[twoVecEntity]()
	require.NoError(t, err)
	_, err = resolveVectorFieldSpecs(specs2, "")
	assert.ErrorIs(t, err, ErrInvalidVectorQuery)
	name, err = resolveVectorFieldSpecs(specs2, "Emb2")
	require.NoError(t, err)
	assert.Equal(t, "Emb2", name)

	// 无向量字段：拒绝。
	type noVec2Entity struct {
		ID int64
	}
	specs3, err := specsOf[noVec2Entity]()
	require.NoError(t, err)
	_, err = resolveVectorFieldSpecs(specs3, "")
	assert.ErrorIs(t, err, ErrInvalidVectorQuery)
}

// TestPkColumnBuilders 主键列构造：类型匹配、类型不匹配拒绝、
// VarChar 值含引号/反斜杠拒绝（官方 PKs2Expr 无转义，防表达式逃逸）。
func TestPkColumnBuilders(t *testing.T) {
	intSpecs, err := specsOf[schemaEntity]()
	require.NoError(t, err)
	intPk, err := pkSpecOf(intSpecs)
	require.NoError(t, err)

	type varcharPkEntity struct {
		UUID string
		Emb  []float32
	}
	strSpecs, err := specsOf[varcharPkEntity]()
	require.NoError(t, err)
	strPk, err := pkSpecOf(strSpecs)
	require.NoError(t, err)

	// 数值主键：int64 主键构造；VarChar 主键拒绝。
	col, err := numericPkColumn(intPk, []uint64{1, 2})
	require.NoError(t, err)
	assert.Equal(t, "ID", col.Name())
	ic, ok := col.(*entity.ColumnInt64)
	require.True(t, ok)
	assert.Equal(t, []int64{1, 2}, ic.Data())
	_, err = numericPkColumn(strPk, []uint64{1})
	assert.ErrorIs(t, err, ErrInvalidPointID)

	// VarChar 主键：正常值构造；引号/反斜杠拒绝；int64 主键拒绝。
	col, err = varcharPkColumn(strPk, []string{"ok"})
	require.NoError(t, err)
	assert.Equal(t, "UUID", col.Name())
	sc, ok := col.(*entity.ColumnVarChar)
	require.True(t, ok)
	assert.Equal(t, []string{"ok"}, sc.Data())
	_, err = varcharPkColumn(strPk, []string{"a\"b"})
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = varcharPkColumn(strPk, []string{"a\\b"})
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = varcharPkColumn(intPk, []string{"a"})
	assert.ErrorIs(t, err, ErrInvalidPointID)
}

// TestPkSpecOf_Errors 主键解析：多主键与无主键拒绝。
func TestPkSpecOf_Errors(t *testing.T) {
	type multiPkEntity struct {
		ID   int64
		UUID string
		Emb  []float32
	}
	specs, err := specsOf[multiPkEntity]()
	require.NoError(t, err)
	_, err = pkSpecOf(specs)
	assert.ErrorIs(t, err, ErrInvalidPointID)

	type noPkEntity struct {
		Emb []float32
	}
	specs, err = specsOf[noPkEntity]()
	require.NoError(t, err)
	_, err = pkSpecOf(specs)
	assert.ErrorIs(t, err, ErrInvalidPointID)
}
