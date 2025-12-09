package query

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
	bsonV2 "go.mongodb.org/mongo-driver/v2/bson"
	optionsV2 "go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Builder 用于构建 MongoDB 查询
type Builder struct {
	filter bsonV2.M

	findOpts    *optionsV2.FindOptions
	findOneOpts *optionsV2.FindOneOptions

	pipeline []bsonV2.D

	skip     *int64
	limit    *int64
	token    *string
	pageSize *int64
}

func NewQueryBuilder() *Builder {
	return &Builder{
		filter:      bsonV2.M{},
		findOpts:    &optionsV2.FindOptions{},
		findOneOpts: &optionsV2.FindOneOptions{},
	}
}

// Where 将传入的过滤条件合并到 Builder 的 filter 中（覆盖同名字段）
func (qb *Builder) Where(cond bsonV2.M) *Builder {
	if qb.filter == nil {
		qb.filter = bsonV2.M{}
	}
	for k, v := range cond {
		qb.filter[k] = v
	}
	return qb
}

// SetFilter 设置查询过滤条件
func (qb *Builder) SetFilter(filter bsonV2.M) *Builder {
	qb.filter = filter
	return qb
}

// SetOr 设置多个条件的逻辑或
func (qb *Builder) SetOr(conditions []bsonV2.M) *Builder {
	qb.filter[OperatorOr] = conditions
	return qb
}

// SetAnd 设置多个条件的逻辑与
func (qb *Builder) SetAnd(conditions []bsonV2.M) *Builder {
	qb.filter[OperatorAnd] = conditions
	return qb
}

// SetNotEqual 设置字段的不等于条件
func (qb *Builder) SetNotEqual(field string, value interface{}) *Builder {
	qb.filter[field] = bsonV2.M{OperatorNe: value}
	return qb
}

// SetGreaterThan 设置字段的大于条件
func (qb *Builder) SetGreaterThan(field string, value interface{}) *Builder {
	qb.filter[field] = bsonV2.M{OperatorGt: value}
	return qb
}

// SetLessThan 设置字段的小于条件
func (qb *Builder) SetLessThan(field string, value interface{}) *Builder {
	qb.filter[field] = bsonV2.M{OperatorLt: value}
	return qb
}

// SetExists 设置字段是否存在条件
func (qb *Builder) SetExists(field string, exists bool) *Builder {
	qb.filter[field] = bsonV2.M{OperatorExists: exists}
	return qb
}

// SetType 设置字段的类型条件
func (qb *Builder) SetType(field string, typeValue interface{}) *Builder {
	qb.filter[field] = bsonV2.M{OperatorType: typeValue}
	return qb
}

// SetBetween 设置字段的范围查询条件
func (qb *Builder) SetBetween(field string, start, end interface{}) *Builder {
	qb.filter[field] = bsonV2.M{
		OperatorGte: start,
		OperatorLte: end,
	}
	return qb
}

// SetIn 设置字段的包含条件
func (qb *Builder) SetIn(field string, values []interface{}) *Builder {
	qb.filter[field] = bsonV2.M{OperatorIn: values}
	return qb
}

// SetNotIn 设置字段的排除条件
func (qb *Builder) SetNotIn(field string, values []interface{}) *Builder {
	qb.filter[field] = bsonV2.M{OperatorNin: values}
	return qb
}

// SetElemMatch 设置数组字段的匹配条件
func (qb *Builder) SetElemMatch(field string, match bsonV2.M) *Builder {
	qb.filter[field] = bsonV2.M{OperatorElemMatch: match}
	return qb
}

// SetAll 设置字段必须包含所有指定值的条件
func (qb *Builder) SetAll(field string, values []interface{}) *Builder {
	qb.filter[field] = bsonV2.M{OperatorAll: values}
	return qb
}

// SetSize 设置数组字段的大小条件
func (qb *Builder) SetSize(field string, size int) *Builder {
	qb.filter[field] = bsonV2.M{OperatorSize: size}
	return qb
}

// SetCurrentDate 设置字段为当前日期
func (qb *Builder) SetCurrentDate(field string) *Builder {
	qb.filter[field] = bsonV2.M{OperatorCurrentDate: true}
	return qb
}

// SetTextSearch 设置文本搜索条件
func (qb *Builder) SetTextSearch(search string) *Builder {
	qb.filter[OperatorText] = bsonV2.M{OperatorSearch: search}
	return qb
}

// SetMod 设置字段的模运算条件
func (qb *Builder) SetMod(field string, divisor, remainder int) *Builder {
	qb.filter[field] = bsonV2.M{OperatorMod: bsonV2.A{divisor, remainder}}
	return qb
}

// SetRegex 设置正则表达式查询条件
func (qb *Builder) SetRegex(field string, pattern string, options string) *Builder {
	qb.filter[field] = bsonV2.M{OperatorRegex: primitive.Regex{Pattern: pattern, Options: options}}
	return qb
}

// SetGeoWithin 设置地理位置范围查询条件
func (qb *Builder) SetGeoWithin(field string, geometry bsonV2.M) *Builder {
	qb.filter[field] = bsonV2.M{
		OperatorGeoWithin: bsonV2.M{
			OperatorGeometry: geometry,
		},
	}
	return qb
}

// SetGeoIntersects 设置地理位置相交查询条件
func (qb *Builder) SetGeoIntersects(field string, geometry bsonV2.M) *Builder {
	qb.filter[field] = bsonV2.M{
		OperatorGeoIntersects: bsonV2.M{
			OperatorGeometry: geometry,
		},
	}
	return qb
}

// SetNear 设置地理位置附近查询条件
func (qb *Builder) SetNear(field string, point bsonV2.M, maxDistance, minDistance float64) *Builder {
	qb.filter[field] = bsonV2.M{
		OperatorNear: bsonV2.M{
			OperatorGeometry:    point,
			OperatorMaxDistance: maxDistance,
			OperatorMinDistance: minDistance,
		},
	}
	return qb
}

// SetNearSphere 设置球面距离附近查询条件
func (qb *Builder) SetNearSphere(field string, point bsonV2.M, maxDistance, minDistance float64) *Builder {
	qb.filter[field] = bsonV2.M{
		OperatorNearSphere: bsonV2.M{
			OperatorGeometry:    point,
			OperatorMaxDistance: maxDistance,
			OperatorMinDistance: minDistance,
		},
	}
	return qb
}

// SetLimit 设置查询结果的限制数量
func (qb *Builder) SetLimit(limit int64) *Builder {
	if qb.findOpts == nil {
		qb.findOpts = &optionsV2.FindOptions{}
	}
	qb.findOpts.Limit = &limit
	return qb
}

// SetSort 设置查询结果的排序条件
func (qb *Builder) SetSort(sort bsonV2.D) *Builder {
	if qb.findOpts == nil {
		qb.findOpts = &optionsV2.FindOptions{}
	}
	qb.findOpts.Sort = sort
	return qb
}

// SetSortWithPriority 设置查询结果的排序条件，并指定优先级
func (qb *Builder) SetSortWithPriority(sortFields []bsonV2.E) *Builder {
	if qb.findOpts == nil {
		qb.findOpts = &optionsV2.FindOptions{}
	}
	qb.findOpts.Sort = bsonV2.D(sortFields)
	return qb
}

// SetProjection 设置查询结果的字段投影
func (qb *Builder) SetProjection(projection bsonV2.M) *Builder {
	if qb.findOpts == nil {
		qb.findOpts = &optionsV2.FindOptions{}
	}
	qb.findOpts.Projection = projection
	return qb
}

// SetSkip 设置查询结果的跳过数量
func (qb *Builder) SetSkip(skip int64) *Builder {
	if qb.findOpts == nil {
		qb.findOpts = &optionsV2.FindOptions{}
	}
	qb.findOpts.Skip = &skip
	qb.skip = &skip
	return qb
}

// SetSkipLimit 设置 offset/limit（用于仓库中调用）
func (qb *Builder) SetSkipLimit(offset, limit int64) *Builder {
	if qb.findOpts == nil {
		qb.findOpts = &optionsV2.FindOptions{}
	}

	qb.findOpts.Skip = &offset
	qb.findOpts.Limit = &limit

	qb.skip = &offset
	qb.limit = &limit
	return qb
}

// SetPage 设置分页功能，page 从 1 开始，size 为每页的文档数量
func (qb *Builder) SetPage(page, size int64) *Builder {
	if qb.findOpts == nil {
		qb.findOpts = &optionsV2.FindOptions{}
	}
	if page < 1 {
		page = 1
	}
	if size <= 0 {
		size = 10
	}

	offset := (page - 1) * size

	qb.findOpts.Skip = &offset
	qb.findOpts.Limit = &size

	qb.skip = &offset
	qb.limit = &size

	return qb
}

// SetTokenPagination 保存 token 分页信息（供上层解析）
func (qb *Builder) SetTokenPagination(token string, pageSize int64) *Builder {
	qb.token = &token
	qb.pageSize = &pageSize
	return qb
}

// AddStage 添加聚合阶段到管道
func (qb *Builder) AddStage(stage bsonV2.D) *Builder {
	qb.pipeline = append(qb.pipeline, stage)
	return qb
}

// BuildPipeline 返回最终的聚合管道
func (qb *Builder) BuildPipeline() []bsonV2.D {
	if qb.pipeline == nil {
		return nil
	}
	p := make([]bsonV2.D, len(qb.pipeline))
	copy(p, qb.pipeline)
	return p
}

// Build 返回最终的过滤条件和查询选项
func (qb *Builder) Build() (bsonV2.M, *optionsV2.FindOptions) {
	// 复制 filter
	filterCopy := make(bsonV2.M, len(qb.filter))
	for k, v := range qb.filter {
		filterCopy[k] = v
	}

	// 复制 findOpts（浅拷贝结构体内容）
	var optsCopy *optionsV2.FindOptions
	if qb.findOpts != nil {
		tmp := *qb.findOpts
		optsCopy = &tmp
	} else {
		optsCopy = &optionsV2.FindOptions{}
	}

	return filterCopy, optsCopy
}

// BuildFind 构建用于 Find 的 filter 与 options
// 返回值：filter interface{} (可为 bson.M/bson.D/...), options.Lister[options.FindOptions], error
func (qb *Builder) BuildFind() (interface{}, optionsV2.Lister[optionsV2.FindOptions], error) {
	// 如果 findOpts 为空，初始化
	if qb.findOpts == nil {
		qb.findOpts = &optionsV2.FindOptions{}
	}
	// 确保 skip/limit 已应用（如果使用 SetSkipLimit/SetSkip 设置）
	if qb.skip != nil {
		qb.findOpts.Skip = qb.skip
	}
	if qb.limit != nil {
		qb.findOpts.Limit = qb.limit
	}
	// 默认空 filter -> bson.M{}
	if qb.filter == nil {
		// 返回空 filter 且 nil options 列表（便于调用处直接不传 options）
		return bsonV2.M{}, &findOptsLister{opts: qb.findOpts}, nil
	}
	return qb.filter, &findOptsLister{opts: qb.findOpts}, nil
}

// BuildFindOne 构建用于 FindOne 的 filter 与 options
// 返回值：filter interface{} (可为 bson.M/bson.D/...), options.Lister[options.FindOneOptions], error
func (qb *Builder) BuildFindOne() (interface{}, optionsV2.Lister[optionsV2.FindOneOptions], error) {
	if qb.findOneOpts == nil {
		qb.findOneOpts = &optionsV2.FindOneOptions{}
	}
	// 将 skip/limit（若设置）也应用到 findOneOpts（一般 FindOne 只需 projection/sort）
	if qb.skip != nil {
		// findOneOpts.Skip 是 *int64
		qb.findOneOpts.Skip = qb.skip
	}
	// 默认空 filter -> bson.M{}
	if qb.filter == nil {
		return bsonV2.M{}, &findOneOptsLister{opts: qb.findOneOpts}, nil
	}
	return qb.filter, &findOneOptsLister{opts: qb.findOneOpts}, nil
}
