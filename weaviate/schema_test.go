package weaviate

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tx7do/go-crud/vector"
)

// ─────────────────────────────────────────────────────────────────────────────
// class schema 构建测试（纯函数，可离线断言）。
// ─────────────────────────────────────────────────────────────────────────────

// classEntity schema 构建矩阵实体（无符号字段见 TestBuildClass_UnsignedRejected）。
type classEntity struct {
	UUID   string
	Title  string             `json:"title"`
	Age    int64              `json:"age"`
	Flag   bool               `json:"flag"`
	Score  float64            `json:"score"`
	Tags   []string           `json:"tags"`
	Nums   []int64            `json:"nums"`
	Meta   map[string]string  `json:"meta"`
	Inner  struct{ A string } `json:"-"`
	Emb    []float32
	Tenant int64 `json:"tenant_id"`
}

// TestBuildClass 属性类型映射 / ID 与向量通道跳过 / 映射与结构体跳过 /
// tenant_id 可过滤索引 / 度量落 vectorIndexConfig。
func TestBuildClass(t *testing.T) {
	class, err := buildClass[classEntity]("GoCrudDoc", vector.MetricCosine)
	require.NoError(t, err)

	assert.Equal(t, "GoCrudDoc", class.Class)
	assert.Equal(t, "none", class.Vectorizer, "module always supplies its own vectors")

	cfg, ok := class.VectorIndexConfig.(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "cosine", cfg["distance"])

	props := make(map[string]string, len(class.Properties))
	filterable := make(map[string]bool, len(class.Properties))
	for _, p := range class.Properties {
		props[p.Name] = p.DataType[0]
		filterable[p.Name] = p.IndexFilterable != nil && *p.IndexFilterable
	}
	assert.Equal(t, "text", props["title"])
	assert.Equal(t, "int", props["age"])
	assert.Equal(t, "boolean", props["flag"])
	assert.Equal(t, "number", props["score"])
	assert.Equal(t, "text[]", props["tags"])
	assert.Equal(t, "int[]", props["nums"])
	assert.Equal(t, "int", props["tenant_id"])
	assert.True(t, filterable["tenant_id"], "tenant property must be filter-indexed")
	assert.False(t, filterable["title"])

	// 通道外字段与不可编码字段不进 schema。
	assert.NotContains(t, props, "UUID")
	assert.NotContains(t, props, "Emb")
	assert.NotContains(t, props, "Meta")
	assert.NotContains(t, props, "Inner")
}

// TestBuildClass_UnsignedRejected 无符号整数显式报错。
func TestBuildClass_UnsignedRejected(t *testing.T) {
	type unsignedEntity struct {
		Count uint32
	}
	_, err := buildClass[unsignedEntity]("GoCrudDoc", "")
	assert.ErrorIs(t, err, ErrInvalidRequest)

	type unsignedSliceEntity struct {
		Counts []uint8
	}
	_, err = buildClass[unsignedSliceEntity]("GoCrudDoc", "")
	assert.ErrorIs(t, err, ErrInvalidRequest)
}

// TestBuildClass_ClassNameValidation 类名须大写字母开头（weaviate GraphQL
// 约定），非法类名拒绝。
func TestBuildClass_ClassNameValidation(t *testing.T) {
	_, err := buildClass[classEntity]("goCrudDoc", "")
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = buildClass[classEntity]("1doc", "")
	assert.ErrorIs(t, err, ErrInvalidRequest)
	_, err = buildClass[classEntity]("bad`class", "")
	assert.ErrorIs(t, err, ErrInvalidRequest)

	_, err = buildClass[classEntity]("GoCrudDoc_2", "")
	assert.NoError(t, err)
}

// TestBuildClass_PropertyNameValidation 属性名注入面拒绝。
func TestBuildClass_PropertyNameValidation(t *testing.T) {
	type badPropEntity struct {
		Title string `json:"bad)prop"`
	}
	_, err := buildClass[badPropEntity]("GoCrudDoc", "")
	assert.ErrorIs(t, err, ErrInvalidRequest)

	// weaviate GraphQL 读侧把大写开头属性名自动小写化，一律拒绝以保证
	// 写入/查询/返回三处命名一致。
	type upperPropEntity struct {
		Title string `json:"Title"`
	}
	_, err = buildClass[upperPropEntity]("GoCrudDoc", "")
	assert.ErrorIs(t, err, ErrInvalidRequest)
}

// TestBuildClass_Metrics 度量映射与缺省（空串按 cosine）。
func TestBuildClass_Metrics(t *testing.T) {
	for metric, want := range map[vector.DistanceMetric]string{
		vector.MetricCosine:     "cosine",
		vector.MetricDotProduct: "dot",
		vector.MetricEuclidean:  "l2-squared",
		"":                      "cosine",
	} {
		class, err := buildClass[classEntity]("GoCrudDoc", metric)
		require.NoError(t, err)
		cfg := class.VectorIndexConfig.(map[string]any)
		assert.Equal(t, want, cfg["distance"])
	}

	_, err := buildClass[classEntity]("GoCrudDoc", vector.DistanceMetric("hamming"))
	assert.ErrorIs(t, err, ErrInvalidRequest)
}

// TestWeaviateDistanceToScore 距离 → 统一相似度分换算。
func TestWeaviateDistanceToScore(t *testing.T) {
	// cosine：相似度 = 1 - 距离。
	assert.InDelta(t, 0.9, weaviateDistanceToScore(vector.MetricCosine, 0.1), 1e-9)
	assert.InDelta(t, 0.9, weaviateDistanceToScore("", 0.1), 1e-9)
	// dot：weaviate dot 距离 = -点积。
	assert.InDelta(t, 2.5, weaviateDistanceToScore(vector.MetricDotProduct, -2.5), 1e-9)
	// l2-squared：统一欧氏换算 1/(1+d)。
	assert.InDelta(t, 1.0/1.25, weaviateDistanceToScore(vector.MetricEuclidean, 0.25), 1e-9)
}

// TestCollectFieldSpecs 拍平遍历与通道标记。
func TestCollectFieldSpecs(t *testing.T) {
	specs, err := collectFieldSpecs(reflect.TypeOf(&classEntity{}).Elem())
	require.NoError(t, err)

	byName := make(map[string]propSpec, len(specs))
	for _, spec := range specs {
		byName[spec.name] = spec
	}
	assert.True(t, byName["UUID"].isPK)
	assert.True(t, byName["Emb"].isVector)
	assert.True(t, byName["tenant_id"].name == "tenant_id")
	assert.False(t, byName["title"].isPK)
}
