package neo4j

import (
	"math"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tx7do/go-crud/neo4j/mixin"
)

// ─────────────────────────────────────────────────────────────────────────────
// 属性编解码测试（纯函数，可离线断言）。
//
// 通道约定：匿名嵌入 mixin 拍平（tenant_id 落入属性表）；neo4j 标签 "-"
// 跳过；标签名优先于字段名；身份通道字段（uuid/element_id）不进属性表，
// element id 由节点结构体字段回填；整数窄化为 int64（无符号范围校验）；
// 读侧精确类型还原——int8/int16/int32/uint*/float32 字段无对应驱动值类型，
// 可写不可读。Neo4j 属性系统只收标量与标量数组，映射/结构体/指针不进
// 属性表。
// ─────────────────────────────────────────────────────────────────────────────

// convEntity 属性编解码矩阵实体（Meta 为映射字段，作"不进属性表"的反例）。
type convEntity struct {
	mixin.TenantID
	Skip     string `neo4j:"-"`
	Renamed  string `neo4j:"name:renamed"`
	Name     string
	Flag     bool
	Age      int64
	Count8   int8
	CountU8  uint8
	CountU64 uint64
	Score32  float32
	Score64  float64
	Tags     []string
	Nums     []int64
	Meta     map[string]int64
	Ptr      *string
	Inner    struct{ A string }
	UUID     string
}

// TestStructToProperties 编码矩阵：拍平 / 标签跳过与重命名 / 整数窄化与
// 无符号范围校验 / 浮点提升 / 列表 / 映射、结构体、指针、身份通道跳过。
func TestStructToProperties(t *testing.T) {
	ptr := "p"
	e := &convEntity{
		Renamed:  "q",
		Name:     "n",
		Flag:     true,
		Age:      42,
		Count8:   3,
		CountU8:  9,
		CountU64: math.MaxInt64 + 1,
		Score32:  1.5,
		Score64:  2.5,
		Tags:     []string{"x"},
		Nums:     []int64{1, 2},
		Meta:     map[string]int64{"a": 1},
		Ptr:      &ptr,
	}
	e.SetTenantID(7)

	m := structToProperties(e)

	assert.Equal(t, int64(7), m["tenant_id"], "mixin tenant must be flattened into properties")
	assert.Equal(t, "q", m["renamed"], "tag name overrides field name")
	assert.Equal(t, "n", m["Name"])
	assert.Equal(t, true, m["Flag"])
	assert.Equal(t, int64(42), m["Age"])
	assert.Equal(t, int64(3), m["Count8"], "int8 narrows to int64")
	assert.Equal(t, int64(9), m["CountU8"], "in-range uint8 narrows to int64")
	assert.Equal(t, 2.5, m["Score64"])
	assert.Equal(t, 1.5, m["Score32"], "float32 widens losslessly to float64")
	assert.Equal(t, []any{"x"}, m["Tags"])
	assert.Equal(t, []any{int64(1), int64(2)}, m["Nums"])

	// 不可编码：映射（Neo4j 属性无映射类型）、超界无符号、结构体、指针、
	// 身份通道字段、标签 "-"。
	assert.NotContains(t, m, "Meta")
	assert.NotContains(t, m, "CountU64")
	assert.NotContains(t, m, "Inner")
	assert.NotContains(t, m, "Ptr")
	assert.NotContains(t, m, "UUID")
	assert.NotContains(t, m, "Skip")
	assert.Len(t, m, 11)
}

// TestPropertiesToEntity 解码矩阵：精确类型回填；类型不符（int8/uint8/
// float32 字段）、nil 值属性、缺失属性一律跳过；element id 仅回填
// string 身份通道字段。
func TestPropertiesToEntity(t *testing.T) {
	var e convEntity
	node := neo4j.Node{
		ElementId: "4:abc:1",
		Props: map[string]any{
			"tenant_id": int64(7),
			"renamed":   "r",
			"Name":      "n",
			"Flag":      true,
			"Age":       int64(42),
			"Count8":    int64(7), // int8 字段无对应驱动值类型 → 不回填
			"CountU8":   int64(7), // uint8 字段同理
			"Score32":   1.5,      // float32 字段同理
			"Score64":   2.5,
			"Tags":      nil, // nil 值属性 → 不回填
		},
	}
	require.NoError(t, propertiesToEntity(node, &e))

	assert.Equal(t, int64(7), e.TenantID.TenantID)
	assert.Equal(t, "r", e.Renamed)
	assert.Equal(t, "n", e.Name)
	assert.True(t, e.Flag)
	assert.Equal(t, int64(42), e.Age)
	assert.Equal(t, 2.5, e.Score64)
	// 精确类型不符的字段保持零值（驱动属性值仅有 int64/float64/string/bool）。
	assert.Equal(t, int8(0), e.Count8)
	assert.Equal(t, uint8(0), e.CountU8)
	assert.Equal(t, float32(0), e.Score32)
	// nil 值属性与缺失属性（Nums/Meta）保持零值。
	assert.Nil(t, e.Tags)
	assert.Nil(t, e.Nums)
	assert.Nil(t, e.Meta)
	assert.Empty(t, e.Skip)
	// element id 回填 string 身份通道字段。
	assert.Equal(t, "4:abc:1", e.UUID)
}

// TestPropertiesToEntity_TypedLists 列表的元素级精确类型回填。
func TestPropertiesToEntity_TypedLists(t *testing.T) {
	type typedEntity struct {
		Tags []string
		Nums []int64
		UUID string
	}
	var e typedEntity
	node := neo4j.Node{
		Props: map[string]any{
			"Tags": []any{"a", "b"},
			"Nums": []any{int64(1), int64(2)},
		},
	}
	require.NoError(t, propertiesToEntity(node, &e))
	assert.Equal(t, []string{"a", "b"}, e.Tags)
	assert.Equal(t, []int64{1, 2}, e.Nums)
	assert.Empty(t, e.UUID)
}

// TestPropertiesToEntity_MixedElementList 混合元素列表不回填（整体跳过）。
func TestPropertiesToEntity_MixedElementList(t *testing.T) {
	type shortListEntity struct {
		Tags []int64
		UUID string
	}
	var e shortListEntity
	node := neo4j.Node{Props: map[string]any{"Tags": []any{int64(1), "x"}}}
	require.NoError(t, propertiesToEntity(node, &e))
	assert.Nil(t, e.Tags, "mixed-element list must not populate the field")
	assert.Empty(t, e.UUID)
}

// TestPropertiesToEntity_MapPropsIgnored 映射形态的属性值不回填任何字段
// （服务端不会产出，防御协议旁路）。
func TestPropertiesToEntity_MapPropsIgnored(t *testing.T) {
	type mapEntity struct {
		M    map[string]int64
		UUID string
	}
	var e mapEntity
	node := neo4j.Node{Props: map[string]any{"M": map[string]any{"k": int64(1)}}}
	require.NoError(t, propertiesToEntity(node, &e))
	assert.Nil(t, e.M, "map-shaped property values must not hydrate")
}

// TestPropertiesToEntity_NilDestination nil 目标报错。
func TestPropertiesToEntity_NilDestination(t *testing.T) {
	var e *convEntity
	err := propertiesToEntity(neo4j.Node{}, e)
	assert.ErrorIs(t, err, ErrPayloadConversion)
}

// TestParseTagSetting 标签设置解析（分段、k:v、空白修剪）。
func TestParseTagSetting(t *testing.T) {
	assert.Equal(t, map[string]string{}, parseTagSetting(""))
	assert.Equal(t, map[string]string{"name": "a", "other": "b"}, parseTagSetting("name: a , other: b"))
	assert.Equal(t, map[string]string{}, parseTagSetting("noseparator"))
}

// TestStructToProperties_NilEntity nil 实体返回空属性表。
func TestStructToProperties_NilEntity(t *testing.T) {
	var e *convEntity
	m := structToProperties(e)
	assert.Empty(t, m)
}

// TestStructToProperties_UnexportedAndUnencodableSlices 未导出字段跳过；
// 元素不可编码的切片整体跳过；空切片合法编码为空列表（无失败元素）。
func TestStructToProperties_UnexportedAndUnencodableSlices(t *testing.T) {
	type inner struct{ A string }
	type oddEntity struct {
		hidden string
		Inner  []inner
		Empty  []int64
		OK     string
	}
	e := &oddEntity{hidden: "h", Inner: []inner{{A: "x"}}, OK: "o"}

	m := structToProperties(e)
	assert.Equal(t, map[string]any{"OK": "o", "Empty": []any{}}, m)
}

// TestSetElementID_NilEntity nil 实体不 panic。
func TestSetElementID_NilEntity(t *testing.T) {
	var e *convEntity
	assert.NotPanics(t, func() { setElementID(e, "4:0:1") })
}

// TestNewClient_OfflineConstruction 默认 URI 可离线构建真实驱动（惰性
// 连接），非法 URI 构建期报错；Close 幂等。
func TestNewClient_OfflineConstruction(t *testing.T) {
	c, err := NewClient()
	require.NoError(t, err)
	require.NotNil(t, c.drv)
	assert.NoError(t, c.Close())

	_, err = NewClient(WithURI("://invalid-uri"))
	assert.Error(t, err, "unparseable URI must fail at construction")
}
