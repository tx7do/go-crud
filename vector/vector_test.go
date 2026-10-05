package vector

import (
	"database/sql/driver"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryValidate(t *testing.T) {
	var nilQuery *Query
	assert.Error(t, nilQuery.Validate())

	valid := &Query{Field: "embedding", Vector: []float32{1, 2, 3}, TopK: 10}
	assert.NoError(t, valid.Validate())

	assert.Error(t, (&Query{Vector: []float32{1}, TopK: 1}).Validate(), "field 为空应报错")
	assert.Error(t, (&Query{Field: "embedding", TopK: 1}).Validate(), "向量为空应报错")
	assert.Error(t, (&Query{Field: "embedding", Vector: []float32{1}, TopK: 0}).Validate(), "TopK=0 应报错")
	assert.Error(t, (&Query{Field: "embedding", Vector: []float32{1}, TopK: -1}).Validate(), "TopK<0 应报错")
	assert.Error(t, (&Query{Field: "embedding", Vector: []float32{1}, TopK: 1, NumCandidates: -5}).Validate(), "候选数<0 应报错")

	assert.Error(t, (&Query{Field: "embedding", Vector: []float32{float32(math.NaN())}, TopK: 1}).Validate(),
		"含 NaN 的向量应报错")
	assert.Error(t, (&Query{Field: "embedding", Vector: []float32{float32(math.Inf(1))}, TopK: 1}).Validate(),
		"含 Inf 的向量应报错")
}

func TestEffectiveNumCandidates(t *testing.T) {
	q := &Query{TopK: 8}
	assert.Equal(t, 8*TopKDefaultNumCandidatesRatio, q.EffectiveNumCandidates())

	q.NumCandidates = 200
	assert.Equal(t, 200, q.EffectiveNumCandidates())
}

func TestFloat32VectorValueAndScan(t *testing.T) {
	vec := Float32Vector{1.5, 2, -3.25}

	val, err := vec.Value()
	require.NoError(t, err)
	assert.Equal(t, "[1.5,2,-3.25]", val)

	// pgvector 文本格式回读
	var got Float32Vector
	require.NoError(t, got.Scan("[1.5,2,-3.25]"))
	assert.Equal(t, vec, got)

	// []byte 来源（postgres 驱动常见）
	var fromBytes Float32Vector
	require.NoError(t, fromBytes.Scan([]byte("[1,2,3]")))
	assert.Equal(t, Float32Vector{1, 2, 3}, fromBytes)

	// 空向量与 NULL
	var empty Float32Vector
	require.NoError(t, empty.Scan(""))
	assert.Empty(t, []float32(empty))

	var nullVec Float32Vector
	require.NoError(t, nullVec.Scan(nil))
	assert.Nil(t, nullVec)

	// 非法输入
	assert.Error(t, (&got).Scan("not-a-vector"))
	assert.Error(t, (&got).Scan(123))
}

func TestParseVector(t *testing.T) {
	got, err := ParseVector(" [ 1, 2.5 ,3 ] ")
	require.NoError(t, err)
	assert.Equal(t, Float32Vector{1, 2.5, 3}, got)

	_, err = ParseVector("[a,b]")
	assert.Error(t, err)
}

func TestFormatVectorLiteral(t *testing.T) {
	assert.Equal(t, "[]", FormatVectorLiteral(nil))
	assert.Equal(t, "[0,1,0.5]", FormatVectorLiteral([]float32{0, 1, 0.5}))
}

func TestDistanceToScore(t *testing.T) {
	// cosine：距离 = 1 - cos ∈ [0,2]，score = cos
	assert.InDelta(t, 1.0, DistanceToScore(MetricCosine, 0), 1e-9)
	assert.InDelta(t, 0.0, DistanceToScore(MetricCosine, 1), 1e-9)
	assert.InDelta(t, -1.0, DistanceToScore(MetricCosine, 2), 1e-9)

	// euclidean：score = 1/(1+d)，单调递减、恒为正
	assert.InDelta(t, 1.0, DistanceToScore(MetricEuclidean, 0), 1e-9)
	assert.InDelta(t, 0.5, DistanceToScore(MetricEuclidean, 1), 1e-9)
	assert.InDelta(t, 1.0/3.0, DistanceToScore(MetricEuclidean, 2), 1e-9)

	// dot：距离为负内积，score = -d = 内积
	assert.InDelta(t, 3.5, DistanceToScore(MetricDotProduct, -3.5), 1e-9)
	assert.InDelta(t, 0.0, DistanceToScore(MetricDotProduct, 0), 1e-9)

	// 未知度量按欧氏距离处理
	assert.InDelta(t, 0.5, DistanceToScore("unknown", 1), 1e-9)
}

func TestDistance(t *testing.T) {
	a := []float32{1, 0, 0}
	b := []float32{0, 1, 0}

	// 余弦：正交 cos=0 距离=1；同向距离=0；反向距离=2
	d, err := Distance(MetricCosine, a, b)
	require.NoError(t, err)
	assert.InDelta(t, 1.0, d, 1e-9)
	d, err = Distance(MetricCosine, a, a)
	require.NoError(t, err)
	assert.InDelta(t, 0.0, d, 1e-9)
	d, err = Distance(MetricCosine, a, []float32{-1, 0, 0})
	require.NoError(t, err)
	assert.InDelta(t, 2.0, d, 1e-9)

	// 未指定度量按余弦处理
	d, err = Distance("", a, b)
	require.NoError(t, err)
	assert.InDelta(t, 1.0, d, 1e-9)

	// 欧氏距离
	d, err = Distance(MetricEuclidean, []float32{0, 0}, []float32{3, 4})
	require.NoError(t, err)
	assert.InDelta(t, 5.0, d, 1e-9)

	// 内积
	d, err = Distance(MetricDotProduct, []float32{1, 2, 3}, []float32{4, 5, 6})
	require.NoError(t, err)
	assert.InDelta(t, 32.0, d, 1e-9)

	// 维度不一致
	_, err = Distance(MetricCosine, a, []float32{1, 2})
	assert.Error(t, err)

	// 未知度量
	_, err = Distance("unknown", a, b)
	assert.Error(t, err)
}

func TestDriverValueInterface(t *testing.T) {
	var _ driver.Valuer = Float32Vector{}
	// sql.Scanner 由指针实现，编译期断言放测试里兜底
	var _ interface{ Scan(src any) error } = &Float32Vector{}
}
