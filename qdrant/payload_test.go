package qdrant

import (
	"reflect"
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

// nestedConvEntity 覆盖 map / 字符串切片字段的编码与非嵌入结构体字段的跳过。
type nestedConvEntity struct {
	ID    uint64            `json:"id"`
	Meta  map[string]string `json:"meta"`
	Names []string          `json:"names"`
	Inner struct {
		A string `json:"A"`
	} `json:"inner"` // 非嵌入结构体字段：编码跳过，解码可还原
	Emb []float32 `json:"emb"` // 向量字段：不进载荷
}

func TestPayloadFromEntity_MapAndSliceChannels(t *testing.T) {
	e := &nestedConvEntity{
		ID:    1,
		Meta:  map[string]string{"k": "v"},
		Names: []string{"a", "b"},
		Emb:   []float32{1, 2},
	}
	m, err := payloadFromEntity(e)
	require.NoError(t, err)

	// map / 字符串切片按 JSON 语义映射。
	assert.Equal(t, uint64(1), m["id"])
	assert.Equal(t, map[string]any{"k": "v"}, m["meta"])
	assert.Equal(t, []any{"a", "b"}, m["names"])

	// 结构体字段与向量字段不进载荷。
	_, has := m["inner"]
	assert.False(t, has, "struct fields must not enter payload")
	_, has = m["emb"]
	assert.False(t, has, "vector fields must not enter payload")
}

func TestEntityFromPayload_NestedStructures(t *testing.T) {
	payload, err := qdrant.TryValueMap(map[string]any{
		"meta":  map[string]any{"k": "v"}, // 嵌套 StructValue → map
		"names": []any{"a", "b"},          // ListValue → 切片
		"inner": map[string]any{"A": "x"}, // StructValue → 结构体字段
		"f":     1.5,                      // DoubleValue → float64（无对应字段）
		"b":     true,                     // BoolValue → bool（无对应字段）
	})
	require.NoError(t, err)

	var e nestedConvEntity
	require.NoError(t, entityFromPayload(payload, &e))
	assert.Equal(t, map[string]string{"k": "v"}, e.Meta)
	assert.Equal(t, []string{"a", "b"}, e.Names)
	assert.Equal(t, "x", e.Inner.A)
}

func TestEntityFromPayload_EmptyAndMismatch(t *testing.T) {
	// 空载荷 → 直接返回。
	var e nestedConvEntity
	require.NoError(t, entityFromPayload(map[string]*qdrant.Value{}, &e))
	assert.Zero(t, e.ID)

	// 载荷值类型与字段类型不符 → JSON 反序列化失败。
	payload, err := qdrant.TryValueMap(map[string]any{"id": "not-a-number"})
	require.NoError(t, err)
	assert.ErrorIs(t, entityFromPayload(payload, &nestedConvEntity{}), ErrPayloadConversion)
}

func TestValueToNative_NilAndNullKinds(t *testing.T) {
	assert.Nil(t, valueToNative(nil))
	assert.Nil(t, valueToNative(&qdrant.Value{})) // 无 Kind
	assert.Nil(t, valueToNative(&qdrant.Value{Kind: &qdrant.Value_NullValue{}}))
}

// TestValueToPayload_KindMatrix 各基本类型按原样进入载荷；
// 指针解引用；nil 指针与非法值跳过。
func TestValueToPayload_KindMatrix(t *testing.T) {
	cases := []struct {
		rv   reflect.Value
		want any
	}{
		{reflect.ValueOf(true), true},
		{reflect.ValueOf(int(1)), int(1)},
		{reflect.ValueOf(int8(1)), int8(1)},
		{reflect.ValueOf(int16(1)), int16(1)},
		{reflect.ValueOf(int32(1)), int32(1)},
		{reflect.ValueOf(int64(1)), int64(1)},
		{reflect.ValueOf(uint(1)), uint(1)},
		{reflect.ValueOf(uint8(1)), uint8(1)},
		{reflect.ValueOf(uint16(1)), uint16(1)},
		{reflect.ValueOf(uint32(1)), uint32(1)},
		{reflect.ValueOf(uint64(1)), uint64(1)},
		{reflect.ValueOf(float32(1.5)), float32(1.5)},
		{reflect.ValueOf(2.5), 2.5},
		{reflect.ValueOf("s"), "s"},
	}
	for _, c := range cases {
		got, err := valueToPayload(c.rv)
		require.NoError(t, err)
		assert.Equal(t, c.want, got)
	}

	x := 5
	got, err := valueToPayload(reflect.ValueOf(&x))
	require.NoError(t, err)
	assert.Equal(t, 5, got)

	got, err = valueToPayload(reflect.ValueOf((*int)(nil)))
	require.NoError(t, err)
	assert.Nil(t, got)

	got, err = valueToPayload(reflect.Value{})
	require.NoError(t, err)
	assert.Nil(t, got)
}

func TestPayloadFromEntity_MultiLevelPointer(t *testing.T) {
	e := nestedConvEntity{ID: 3}
	p := &e
	pp := &p
	m, err := payloadFromEntity(pp)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), m["id"])

	var nilp *nestedConvEntity
	_, err = payloadFromEntity(&nilp)
	assert.ErrorIs(t, err, ErrPayloadConversion)
}

func TestPointFromEntity_NonStructAndBadIDs(t *testing.T) {
	_, err := pointFromEntity(nil)
	assert.ErrorIs(t, err, ErrPayloadConversion)
	_, err = pointFromEntity(42)
	assert.ErrorIs(t, err, ErrPayloadConversion)

	type nilIDEntity struct {
		ID  *uint64
		Emb []float32
	}
	_, err = pointFromEntity(&nilIDEntity{Emb: []float32{1}})
	assert.ErrorIs(t, err, ErrInvalidPointID)

	type badKindIDEntity struct {
		ID  bool
		Emb []float32
	}
	_, err = pointFromEntity(&badKindIDEntity{ID: true, Emb: []float32{1}})
	assert.ErrorIs(t, err, ErrInvalidPointID)

	// 非空指针 ID 字段：解引用后按数值 ID 处理。
	type ptrIDEntity struct {
		ID  *uint64
		Emb []float32
	}
	id := uint64(9)
	pt, err := pointFromEntity(&ptrIDEntity{ID: &id, Emb: []float32{1}})
	require.NoError(t, err)
	require.NotNil(t, pt.Id)
	assert.Equal(t, uint64(9), pt.Id.GetNum())
}

func TestPointFromEntity_UUIDID(t *testing.T) {
	type uuidPointEntity struct {
		UUID string    `json:"uuid"`
		Emb  []float32 `json:"emb"`
	}
	pt, err := pointFromEntity(&uuidPointEntity{UUID: "abc-123", Emb: []float32{0.5}})
	require.NoError(t, err)

	// ID 通道：字符串 UUID 字段 → UUID 点 ID。
	require.NotNil(t, pt.Id)
	assert.Equal(t, "abc-123", pt.Id.GetUuid())

	// 向量通道照常组装。
	require.NotNil(t, pt.Vectors)
	dense := pt.Vectors.GetVector().GetDense()
	require.NotNil(t, dense)
	assert.Equal(t, []float32{0.5}, dense.GetData())
}
