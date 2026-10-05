package weaviate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tx7do/go-crud/weaviate/mixin"
)

// ─────────────────────────────────────────────────────────────────────────────
// 属性编解码测试（纯函数，可离线断言）。
//
// 通道约定：匿名嵌入 mixin 拍平（tenant_id 落入属性表）；json 标签 "-"
// 跳过；标签名优先于字段名；ID 通道字段（id/uuid）与向量字段不进属性表；
// 编码侧保持整数类型不经过 JSON 浮点化；解码侧 JSON 反序列化按目标字段
// 类型还原（含指针）。
// ─────────────────────────────────────────────────────────────────────────────

// convEntity 属性编解码矩阵实体。
type convEntity struct {
	mixin.TenantID
	Skip   string            `json:"-"`
	Rename string            `json:"renamed"`
	Name   string            `json:"name"`
	Flag   bool              `json:"flag"`
	Age    int64             `json:"age"`
	Score  float64           `json:"score"`
	Tags   []string          `json:"tags"`
	Meta   map[string]string `json:"meta"`
	Inner  struct{ A string }
	Ptr    *string `json:"ptr"`
	UUID   string
	Emb    []float32
}

// TestPropsFromEntity 编码矩阵：拍平 / 标签跳过与重命名 / ID 与向量通道
// 跳过 / 结构体与指针跳过 / 列表与映射映射。
func TestPropsFromEntity(t *testing.T) {
	ptr := "p"
	e := &convEntity{
		Rename: "q",
		Name:   "n",
		Flag:   true,
		Age:    42,
		Score:  2.5,
		Tags:   []string{"x"},
		Meta:   map[string]string{"a": "b"},
		Ptr:    &ptr,
		UUID:   "u-1",
		Emb:    []float32{1, 2},
	}
	e.SetTenantID(7)

	m, err := propsFromEntity(e)
	require.NoError(t, err)

	assert.Equal(t, uint32(7), m["tenant_id"], "mixin tenant must be flattened into properties")
	assert.Equal(t, "q", m["renamed"], "tag name overrides field name")
	assert.Equal(t, "n", m["name"])
	assert.Equal(t, true, m["flag"])
	assert.Equal(t, int64(42), m["age"], "integer type must survive without JSON float widening")
	assert.Equal(t, 2.5, m["score"])
	assert.Equal(t, []any{"x"}, m["tags"], "slices encode element-wise")
	assert.Equal(t, map[string]any{"a": "b"}, m["meta"])
	assert.Equal(t, "p", m["ptr"], "pointers dereference into the property value")

	// 通道外字段与不可编码字段不进属性表。
	assert.NotContains(t, m, "UUID", "id channel must not enter properties")
	assert.NotContains(t, m, "Emb", "vector channel must not enter properties")
	assert.NotContains(t, m, "Inner")
	assert.NotContains(t, m, "Skip")
}

// TestEntityFromProps 解码矩阵：按目标字段类型还原（含指针），ID 通道
// 字段经 setIDFieldValue 回填。
func TestEntityFromProps(t *testing.T) {
	var e convEntity
	props := map[string]any{
		"tenant_id": float64(7), // 服务端 JSON 数值为 float64
		"renamed":   "r",
		"name":      "n",
		"flag":      true,
		"age":       float64(42),
		"score":     2.5,
		"Tags":      []any{"a", "b"},
		"Meta":      map[string]any{"k": "v"},
	}
	require.NoError(t, entityFromProps(props, &e))

	require.NotNil(t, e.TenantID.TenantID)
	assert.Equal(t, uint32(7), *e.TenantID.TenantID)
	assert.Equal(t, "r", e.Rename)
	assert.Equal(t, "n", e.Name)
	assert.True(t, e.Flag)
	assert.Equal(t, int64(42), e.Age)
	assert.Equal(t, 2.5, e.Score)
	assert.Equal(t, []string{"a", "b"}, e.Tags)
	assert.Equal(t, map[string]string{"k": "v"}, e.Meta)

	// ID 通道经 setIDFieldValue 回填（属性表不含它）。
	setIDFieldValue(&e, "u-9")
	assert.Equal(t, "u-9", e.UUID)
}

// TestIDFieldValue ID 通道字段读取：uuid / id（大小写不敏感）、非 string
// 类型字段跳过、缺失返回空。
func TestIDFieldValue(t *testing.T) {
	type idEntity struct {
		UUID string
		Name string
	}
	assert.Equal(t, "u-1", idFieldValue(&idEntity{UUID: "u-1"}))

	type idLowerEntity struct {
		id string // 未导出，不参与
		ID string
	}
	assert.Equal(t, "u-2", idFieldValue(&idLowerEntity{ID: "u-2"}))

	type numericIDEntity struct {
		ID   int64
		UUID string
	}
	assert.Equal(t, "", idFieldValue(&numericIDEntity{ID: 1}), "non-string id field is not the id channel")

	assert.Equal(t, "", idFieldValue(nil))
	assert.Equal(t, "", idFieldValue("not-a-struct"))
}

// TestEntityVectors 向量通道：向量字段拼接、缺失报错。
func TestEntityVectors(t *testing.T) {
	type vecEntity struct {
		A []float32
		B []float32
	}
	v, err := entityVectors(&vecEntity{A: []float32{1}, B: []float32{2, 3}})
	require.NoError(t, err)
	assert.Equal(t, []float32{1, 2, 3}, v)

	type noVecEntity struct {
		Name string
	}
	_, err = entityVectors(&noVecEntity{Name: "x"})
	assert.ErrorIs(t, err, ErrInvalidRequest)

	_, err = entityVectors(nil)
	assert.ErrorIs(t, err, ErrPayloadConversion)
}

// TestEntityFromProps_Empty 空属性表直接返回。
func TestEntityFromProps_Empty(t *testing.T) {
	var e convEntity
	assert.NoError(t, entityFromProps(nil, &e))
	assert.Empty(t, e.Name)
}

// TestPropsFromEntity_NilEntity nil 实体报错。
func TestPropsFromEntity_NilEntity(t *testing.T) {
	var e *convEntity
	_, err := propsFromEntity(e)
	assert.ErrorIs(t, err, ErrPayloadConversion)
}
