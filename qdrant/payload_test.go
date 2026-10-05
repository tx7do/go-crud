package qdrant

import (
	"testing"

	qdrant "github.com/qdrant/go-client/qdrant"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tx7do/go-crud/qdrant/mixin"
	"github.com/tx7do/go-crud/vector"
)

// 载荷转换测试（纯函数，可离线断言）。
//
// 通道约定：向量字段（[]float32 / vector.Float32Vector）不进载荷，
// 经 Vectors 通道写入；匿名嵌入 mixin 拍平（tenant_id 落入载荷）；
// json 标签 "-" 跳过；标签名优先于字段名。

// convEntity 覆盖载荷通道的各分支。
type convEntity struct {
	ID      uint64               `json:"id"`
	Title   string               `json:"title"`
	Rank    int32                `json:"rank"`
	Emb     vector.Float32Vector `json:"emb"`
	RawEmb  []float32            `json:"raw_emb"`
	Hidden  string               `json:"-"`
	Skipped struct{ X int }      `json:"skipped"` // 非嵌入结构体字段不进载荷
	mixin.TenantID
}

func TestPayloadFromEntity_Channels(t *testing.T) {
	e := &convEntity{
		ID:    7,
		Title: "t",
		Rank:  3,
		Emb:   vector.Float32Vector{1, 2},
	}
	e.SetTenantID(11)

	m, err := payloadFromEntity(e)
	require.NoError(t, err)

	// 普通字段按 json 标签名进入载荷。
	assert.Equal(t, uint64(7), m["id"])
	assert.Equal(t, "t", m["title"])
	assert.Equal(t, int32(3), m["rank"])

	// 向量字段不进载荷（经 Vectors 通道，见 pointFromEntity）。
	_, hasEmb := m["emb"]
	assert.False(t, hasEmb, "vector.Float32Vector field must not enter payload")
	_, hasRaw := m["raw_emb"]
	assert.False(t, hasRaw, "[]float32 field must not enter payload")

	// "-" 标签跳过；非嵌入结构体字段不进载荷。
	_, has := m["Hidden"]
	assert.False(t, has)
	_, has = m["skipped"]
	assert.False(t, has)

	// 匿名嵌入 mixin 拍平：tenant_id 落入载荷。
	assert.Equal(t, uint32(11), m["tenant_id"])
}

func TestPayloadFromEntity_NilTenantOmitted(t *testing.T) {
	e := &convEntity{ID: 1, Title: "t"}
	m, err := payloadFromEntity(e)
	require.NoError(t, err)
	_, has := m["tenant_id"]
	assert.False(t, has, "nil tenant pointer must be omitted")
}

func TestEntityFromPayload_RoundTrip(t *testing.T) {
	payload, err := qdrant.TryValueMap(map[string]any{
		"title": "hello",
		"rank":  int64(42),
		"tags":  []any{"a", "b"},
	})
	require.NoError(t, err)

	var e convEntity
	require.NoError(t, entityFromPayload(payload, &e))
	assert.Equal(t, "hello", e.Title)
	assert.Equal(t, int32(42), e.Rank)
}

func TestPointFromEntity_NumericIDAndVectors(t *testing.T) {
	e := &convEntity{
		ID:     123,
		Emb:    vector.Float32Vector{0.5, 0.5},
		RawEmb: []float32{0.25, 0.25, 0.25},
	}
	e.SetTenantID(7)

	pt, err := pointFromEntity(e)
	require.NoError(t, err)

	// ID 通道：数值字段 → 数值点 ID。
	require.NotNil(t, pt.Id)
	assert.Equal(t, uint64(123), pt.Id.GetNum())

	// 向量通道：全部向量字段按声明顺序拼接（Emb 两维在前，RawEmb 三维在后）。
	require.NotNil(t, pt.Vectors)
	dense := pt.Vectors.GetVector().GetDense()
	require.NotNil(t, dense)
	assert.Equal(t, []float32{0.5, 0.5, 0.25, 0.25, 0.25}, dense.GetData())

	// 载荷通道：非向量导出字段 + 拍平的 mixin。
	require.NotNil(t, pt.Payload["tenant_id"])
}

func TestPointFromEntity_MissingVector(t *testing.T) {
	e := &convEntity{ID: 1, Title: "no-emb"}
	_, err := pointFromEntity(e)
	assert.ErrorIs(t, err, ErrInvalidRequest)
}

func TestPointFromEntity_MissingOrInvalidID(t *testing.T) {
	type noIDEntity struct {
		Emb []float32 `json:"emb"`
	}
	_, err := pointFromEntity(&noIDEntity{Emb: []float32{1}})
	assert.ErrorIs(t, err, ErrInvalidPointID)

	type negIDEntity struct {
		ID  int64     `json:"id"`
		Emb []float32 `json:"emb"`
	}
	_, err = pointFromEntity(&negIDEntity{ID: -5, Emb: []float32{1}})
	assert.ErrorIs(t, err, ErrInvalidPointID)
}

func TestPayloadFromEntity_NonStruct(t *testing.T) {
	_, err := payloadFromEntity(42)
	assert.ErrorIs(t, err, ErrPayloadConversion)
	_, err = payloadFromEntity(nil)
	assert.ErrorIs(t, err, ErrPayloadConversion)
}
