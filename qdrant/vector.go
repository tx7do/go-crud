package qdrant

import (
	"fmt"

	qdrant "github.com/qdrant/go-client/qdrant"

	"github.com/tx7do/go-crud/vector"
)

// ─────────────────────────────────────────────────────────────────────────────
// 向量检索（RAG / 语义检索）— 基于 Qdrant 的 Query API（最近邻查询）。
//
//   - 距离度量在集合创建时固定（见 Client.CreateVectorCollection），
//     Query.Metric 仅用于查询侧的分数语义换算，不会改写集合配置；
//   - Query.Filter 为引擎原生 *qdrant.Filter（must/should/must_not 结构条件），
//     作为 pre-filter 施加（需对过滤字段建立载荷索引以获得性能）；
//   - Query.Field 忽略：本模块经 CreateVectorCollection 创建的集合只含单一
//     匿名向量，QueryPoints.Using 须保持未设置（命名向量不在支持范围内）；
//   - Query.NumCandidates 映射到 SearchParams.HnswEf（未指定不传，走服务端默认；
//     顺带一提：近期版本服务端多数配置下会忽略该值自行调优）；
//   - Query.MinScore 不透传 ScoreThreshold —— 该参数按引擎原生分数方向过滤
//     （欧氏度量下原生分数是距离、越小越好，与统一语义方向相反），故在
//     换算为统一相似度分之后于客户端按 MinScore 过滤（见 SearchByVector）；
//   - Query.MaxDistance 无对应参数，忽略。
//
// 分数语义：Qdrant 对 Cosine / Dot 返回相似度（越大越相似，直接作为
// vector.Hit.Score）；对 Euclid 返回距离（越小越相似，换算 score = 1/(1+d)，
// 见 vector.DistanceToScore）。
// ─────────────────────────────────────────────────────────────────────────────

// buildQueryPoints 构造 Qdrant 最近邻查询请求（纯函数，便于离线测试）。
func buildQueryPoints(collection string, q *vector.Query, filter *qdrant.Filter) (*qdrant.QueryPoints, error) {
	if err := q.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidVectorQuery, err)
	}
	if collection == "" {
		return nil, ErrInvalidRequest
	}

	req := &qdrant.QueryPoints{
		CollectionName: collection,
		Query:          qdrant.NewQueryDense(q.Vector),
		Limit:          ptr(uint64(q.TopK)),
		WithPayload:    qdrant.NewWithPayload(true),
	}
	if filter != nil {
		req.Filter = filter
	}
	if q.NumCandidates > 0 {
		req.Params = &qdrant.SearchParams{HnswEf: ptr(uint64(q.NumCandidates))}
	}
	return req, nil
}

// qdrantScoreToScore 把 Qdrant 原生分数换算为统一语义的相似度分
// （Euclid 为距离，其余为相似度，直接透传）。
func qdrantScoreToScore(metric vector.DistanceMetric, raw float32) float64 {
	if metric == vector.MetricEuclidean {
		return vector.DistanceToScore(vector.MetricEuclidean, float64(raw))
	}
	return float64(raw)
}

func ptr[T any](v T) *T {
	return &v
}
