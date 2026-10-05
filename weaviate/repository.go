package weaviate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	strfmt "github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/fault"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/filters"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/graphql"
	"github.com/weaviate/weaviate/entities/models"

	"github.com/tx7do/go-utils/mapper"
	"github.com/tx7do/go-wind/log"

	"github.com/tx7do/go-crud/vector"
	"github.com/tx7do/go-crud/viewer"
)

// ─────────────────────────────────────────────────────────────────────────────
// Weaviate 仓库（泛型）。
//
// Weaviate 是向量数据库：数据面为「对象 = UUID + 属性 + 向量」，检索面为
// kNN（nearVector）。仓库面：
//   - CreateCollection：按实体映射建 class（属性 schema + 度量固定）
//   - Create / BatchCreate：对象写入（属性 + 向量 + UUID 三通道，
//     UUID 由调用方给定或服务端生成并回读）
//   - GetByUUID：按 UUID 直取（客户端租户校验）
//   - Query / Count / Exists：GraphQL Get/Aggregate（租户条件注入）
//   - DeleteByUUIDs / DeleteByFilter：批量删除（同一已注入条件）
//   - SearchByVector：统一契约的向量检索（nearVector，kNN TopK）
//
// 不提供数值 ID 通道（Get/DeleteByIDs 恒 ErrInvalidRequest：对象身份是
// UUID 字符串）；不提供 ListWithPaging（调用方经 Query 自行组合）。
//
// 租户隔离（嵌入 weaviate/mixin.TenantID 的实体自动启用）：
//   - 写入：租户业务视图下强制覆盖 tenant_id（EnforceOnScopedInstance）；
//   - 可注入条件的路径（Query/Count/Exists/DeleteBy*/SearchByVector）：
//     服务端注入 tenant_id 相等条件（InjectTenantFilter）；
//   - 按 UUID 直取的路径（GetByUUID）：客户端属性租户校验
//     （verifyTenantOnProps，纵深防御）。
// ─────────────────────────────────────────────────────────────────────────────

// Repository Weaviate 版仓库（泛型）
type Repository[DTO any, ENTITY any] struct {
	mapper *mapper.CopierMapper[DTO, ENTITY]

	client     *Client
	collection string
}

// NewRepository 创建 Weaviate 仓库实例。
func NewRepository[DTO any, ENTITY any](client *Client, collection string, mapper *mapper.CopierMapper[DTO, ENTITY], logger log.Logger) *Repository[DTO, ENTITY] {
	if logger != nil {
		log.SetLogger(logger)
	}
	return &Repository[DTO, ENTITY]{
		client:     client,
		collection: collection,
		mapper:     mapper,
	}
}

// ready 客户端与类名守卫。
func (r *Repository[DTO, ENTITY]) ready() error {
	if r.client == nil || r.client.cli == nil {
		return ErrClientNotInitialized
	}
	if r.collection == "" {
		return ErrInvalidRequest
	}
	return nil
}

// CreateCollection 按本仓库实体映射创建 class（集合）。
// 属性 schema 见 buildClass；度量写入 vectorIndexConfig.distance
// （建库期固定，查询期不可换）。向量维度无需声明（由数据决定）。
func (r *Repository[DTO, ENTITY]) CreateCollection(ctx context.Context, metric vector.DistanceMetric) error {
	if r.client == nil || r.client.cli == nil {
		return ErrClientNotInitialized
	}
	class, err := buildClass[ENTITY](r.collection, metric)
	if err != nil {
		return err
	}
	if err = r.client.cli.Schema().ClassCreator().WithClass(class).Do(ctx); err != nil {
		return fmt.Errorf("%w: %v", ErrInsertFailed, err)
	}
	return nil
}

// Create 插入一个对象（UUID 由调用方给定，缺省由服务端生成并回读）。
func (r *Repository[DTO, ENTITY]) Create(ctx context.Context, dto *DTO) (*DTO, error) {
	if err := r.ready(); err != nil {
		return nil, err
	}
	if dto == nil {
		return nil, ErrInvalidRequest
	}

	ent := r.mapper.ToEntity(dto)

	// 租户强制：tenant-scoped 实体在租户业务视图下强制覆盖 tenant_id。
	if err := viewer.EnforceOnScopedInstance(ctx, ent); err != nil {
		return nil, err
	}

	props, err := propsFromEntity(ent)
	if err != nil {
		return nil, err
	}
	vecs, err := entityVectors(ent)
	if err != nil {
		return nil, err
	}

	creator := r.client.cli.Data().Creator().
		WithClassName(r.collection).
		WithProperties(props).
		WithVector(vecs)
	if id := idFieldValue(ent); id != "" {
		creator = creator.WithID(id)
	}

	wrapper, err := creator.Do(ctx)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("weaviate create failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrInsertFailed, err)
	}
	if wrapper != nil && wrapper.Object != nil && string(wrapper.Object.ID) != "" {
		setIDFieldValue(ent, string(wrapper.Object.ID))
	}

	return r.mapper.ToDTO(ent), nil
}

// BatchCreate 批量插入对象（单次 batch）。
func (r *Repository[DTO, ENTITY]) BatchCreate(ctx context.Context, dtos []*DTO) ([]*DTO, error) {
	if err := r.ready(); err != nil {
		return nil, err
	}
	if len(dtos) == 0 {
		return nil, nil
	}

	ents := make([]*ENTITY, 0, len(dtos))
	objects := make([]*models.Object, 0, len(dtos))
	for _, d := range dtos {
		if d == nil {
			continue
		}
		e := r.mapper.ToEntity(d)
		// 租户强制：每个实体在租户业务视图下强制覆盖 tenant_id。
		if err := viewer.EnforceOnScopedInstance(ctx, e); err != nil {
			return nil, err
		}
		props, err := propsFromEntity(e)
		if err != nil {
			return nil, err
		}
		vecs, err := entityVectors(e)
		if err != nil {
			return nil, err
		}
		obj := &models.Object{
			Class:      r.collection,
			Properties: props,
			Vector:     vecs,
		}
		if id := idFieldValue(e); id != "" {
			obj.ID = strfmt.UUID(id)
		}
		ents = append(ents, e)
		objects = append(objects, obj)
	}
	if len(ents) == 0 {
		return nil, nil
	}

	rsps, err := r.client.cli.Batch().ObjectsBatcher().WithObjects(objects...).Do(ctx)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("weaviate batch create failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrInsertFailed, err)
	}
	for i := range ents {
		if i >= len(rsps) {
			break
		}
		if rsp := rsps[i]; rsp.Result != nil && rsp.Result.Errors != nil && len(rsp.Result.Errors.Error) > 0 {
			return nil, fmt.Errorf("%w: row %d: %s", ErrInsertFailed, i, rsp.Result.Errors.Error[0].Message)
		}
		if id := string(rsps[i].ID); id != "" {
			setIDFieldValue(ents[i], id)
		}
	}

	out := make([]*DTO, 0, len(ents))
	for _, e := range ents {
		out = append(out, r.mapper.ToDTO(e))
	}
	return out, nil
}

// Get 按数值 ID 取回单对象：不支持（对象身份是 UUID 字符串，见 GetByUUID）。
func (r *Repository[DTO, ENTITY]) Get(ctx context.Context, id uint64) (*DTO, error) {
	return nil, ErrInvalidRequest
}

// GetByUUID 按 UUID 直取单对象。
// 对象 API 无过滤条件，租户校验在客户端做（语义同注入路径）；
// 不匹配与不存在对外同构（ErrPointNotFound），避免存在性泄露。
func (r *Repository[DTO, ENTITY]) GetByUUID(ctx context.Context, uuid string) (*DTO, error) {
	if err := r.ready(); err != nil {
		return nil, err
	}
	if uuid == "" {
		return nil, ErrInvalidRequest
	}

	objects, err := r.client.cli.Data().ObjectsGetter().
		WithClassName(r.collection).
		WithID(uuid).
		Do(ctx)
	if err != nil {
		// 404 与查询失败区分：未找到按契约折叠为 ErrPointNotFound。
		var statusErr *fault.WeaviateClientError
		if errors.As(err, &statusErr) && statusErr.StatusCode == http.StatusNotFound {
			return nil, ErrPointNotFound
		}
		log.Error(context.Background(), fmt.Sprintf("weaviate get failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrQueryFailed, err)
	}
	if len(objects) == 0 {
		return nil, ErrPointNotFound
	}
	obj := objects[0]
	props, _ := obj.Properties.(map[string]any)

	// 租户校验在客户端做（对象 API 无条件参数，纵深防御路径）。
	if verr := verifyTenantOnProps[ENTITY](ctx, props); verr != nil {
		if errors.Is(verr, errTenantMismatch) {
			return nil, ErrPointNotFound
		}
		return nil, verr
	}

	var ent ENTITY
	if err = entityFromProps(props, &ent); err != nil {
		return nil, err
	}
	setIDFieldValue(&ent, string(obj.ID))

	return r.mapper.ToDTO(&ent), nil
}

// Query 按 Where 条件列出对象（tenant-scoped 实体注入租户条件）。
// limit<=0 时使用服务端默认上限；返回的 DTO 携带属性与 UUID（ID 通道字段）。
func (r *Repository[DTO, ENTITY]) Query(ctx context.Context, fb *filters.WhereBuilder, limit int) ([]*DTO, error) {
	if err := r.ready(); err != nil {
		return nil, err
	}

	fb, err := InjectTenantFilter[ENTITY](ctx, fb)
	if err != nil {
		return nil, err
	}

	specs, err := resolveSpecsOf[ENTITY]()
	if err != nil {
		return nil, err
	}
	fields := make([]graphql.Field, 0, len(specs))
	for _, name := range propFieldNames(specs) {
		fields = append(fields, graphql.Field{Name: name})
	}
	fields = append(fields, graphql.Field{
		Name:   "_additional",
		Fields: []graphql.Field{{Name: "id"}},
	})

	gb := r.client.cli.GraphQL().Get().
		WithClassName(r.collection).
		WithFields(fields...)
	// WithWhere(nil) 会生成非法的空 where 子句（"Unexpected empty IN ()"），
	// 无条件时必须整体省略。
	if fb != nil {
		gb = gb.WithWhere(fb)
	}
	if limit > 0 {
		gb = gb.WithLimit(limit)
	}

	resp, err := gb.Do(ctx)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("weaviate query failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrQueryFailed, err)
	}
	if err = graphqlErrorsAs(resp, ErrQueryFailed); err != nil {
		return nil, err
	}

	out := make([]*DTO, 0)
	for _, hit := range graphqlHits(resp, r.collection) {
		m := hitMap(hit)
		if m == nil {
			continue
		}
		props := stripAdditional(m)
		// 纵深防御：服务端条件已注入租户，客户端再校验一次；不匹配静默剔除。
		if verr := verifyTenantOnProps[ENTITY](ctx, props); verr != nil {
			if errors.Is(verr, errTenantMismatch) {
				continue
			}
			return nil, verr
		}
		var ent ENTITY
		if err = entityFromProps(props, &ent); err != nil {
			return nil, err
		}
		setIDFieldValue(&ent, additionalID(m))
		dto := r.mapper.ToDTO(&ent)
		if dto == nil {
			continue
		}
		out = append(out, dto)
	}
	return out, nil
}

// Count 按 Where 条件计数（tenant-scoped 实体注入租户条件；
// 条件为 nil 时计全集合——租户视图下即为该租户的全部对象）。
func (r *Repository[DTO, ENTITY]) Count(ctx context.Context, fb *filters.WhereBuilder) (int64, error) {
	if err := r.ready(); err != nil {
		return 0, err
	}

	fb, err := InjectTenantFilter[ENTITY](ctx, fb)
	if err != nil {
		return 0, err
	}

	ab := r.client.cli.GraphQL().Aggregate().
		WithClassName(r.collection).
		WithFields(graphql.Field{
			Name:   "meta",
			Fields: []graphql.Field{{Name: "count"}},
		})
	// WithWhere(nil) 会生成非法的空 where 子句，无条件时整体省略。
	if fb != nil {
		ab = ab.WithWhere(fb)
	}

	resp, err := ab.Do(ctx)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("weaviate count failed: %v", err))
		return 0, fmt.Errorf("%w: %v", ErrCountFailed, err)
	}
	if err = graphqlErrorsAs(resp, ErrCountFailed); err != nil {
		return 0, err
	}
	return aggregateCount(resp, r.collection), nil
}

// Exists 判断是否存在符合 Where 条件的对象（tenant-scoped 实体注入租户条件）。
func (r *Repository[DTO, ENTITY]) Exists(ctx context.Context, fb *filters.WhereBuilder) (bool, error) {
	n, err := r.Count(ctx, fb)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// DeleteByIDs 按数值 ID 删除：不支持（对象身份是 UUID 字符串，见 Get）。
func (r *Repository[DTO, ENTITY]) DeleteByIDs(ctx context.Context, ids []uint64) (int64, error) {
	return 0, ErrInvalidRequest
}

// DeleteByUUIDs 按 UUID 批量删除（id ContainsAny 条件与租户条件 And 合并；
// 删除计数取批量删除响应的成功数，同一条件保证不越权删）。
func (r *Repository[DTO, ENTITY]) DeleteByUUIDs(ctx context.Context, uuids []string) (int64, error) {
	if err := r.ready(); err != nil {
		return 0, err
	}
	if len(uuids) == 0 {
		return 0, nil
	}

	idsCond := filters.Where().
		WithPath([]string{"id"}).
		WithOperator(filters.ContainsAny).
		WithValueText(uuids...)
	return deleteByFilter(ctx, r, idsCond)
}

// DeleteByFilter 按 Where 条件删除（必须显式给出条件，防误删全集合；
// tenant-scoped 实体服务端注入租户条件）。
func (r *Repository[DTO, ENTITY]) DeleteByFilter(ctx context.Context, fb *filters.WhereBuilder) (int64, error) {
	if err := r.ready(); err != nil {
		return 0, err
	}
	if fb == nil {
		return 0, ErrInvalidRequest
	}
	return deleteByFilter(ctx, r, fb)
}

// deleteByFilter 合并租户条件后执行批量删除（包级泛型函数：Go 的方法
// 不能自带独立类型参数）。
func deleteByFilter[DTO any, ENTITY any](ctx context.Context, r *Repository[DTO, ENTITY], fb *filters.WhereBuilder) (int64, error) {
	fb, err := InjectTenantFilter[ENTITY](ctx, fb)
	if err != nil {
		return 0, err
	}

	rsp, err := r.client.cli.Batch().ObjectsBatchDeleter().
		WithClassName(r.collection).
		WithWhere(fb).
		Do(ctx)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("weaviate delete failed: %v", err))
		return 0, fmt.Errorf("%w: %v", ErrDeleteFailed, err)
	}
	if rsp != nil && rsp.Results != nil {
		return rsp.Results.Successful, nil
	}
	return 0, nil
}

// SearchByVector 统一契约的向量检索（GraphQL nearVector，kNN TopK）。
//
// Where 注入 tenant_id（pre-filter）；分数按度量换算为统一相似度语义
// （cosine 距离 → 1-d，dot 距离 → -d，l2-squared → 1/(1+d)）；MinScore 在
// 统一分数空间客户端过滤（weaviate 的 certainty 语义不同，不透传）。
// 租户不匹配的命中被静默剔除（纵深防御：服务端条件已限定租户）。
func (r *Repository[DTO, ENTITY]) SearchByVector(ctx context.Context, q *vector.Query) (*vector.Result[DTO], error) {
	if err := r.ready(); err != nil {
		return nil, err
	}
	if q == nil || len(q.Vector) == 0 || q.TopK <= 0 {
		return nil, ErrInvalidVectorQuery
	}

	var fb *filters.WhereBuilder
	if f, ok := q.Filter.(*filters.WhereBuilder); ok {
		fb = f
	}
	fb, err := InjectTenantFilter[ENTITY](ctx, fb)
	if err != nil {
		return nil, err
	}

	specs, err := resolveSpecsOf[ENTITY]()
	if err != nil {
		return nil, err
	}
	fields := make([]graphql.Field, 0, len(specs))
	for _, name := range propFieldNames(specs) {
		fields = append(fields, graphql.Field{Name: name})
	}
	fields = append(fields, graphql.Field{
		Name:   "_additional",
		Fields: []graphql.Field{{Name: "id"}, {Name: "distance"}},
	})

	gb := r.client.cli.GraphQL().Get().
		WithClassName(r.collection).
		WithFields(fields...).
		WithNearVector(r.client.cli.GraphQL().NearVectorArgBuilder().WithVector(q.Vector))
	// WithWhere(nil) 会生成非法的空 where 子句，无条件时整体省略。
	if fb != nil {
		gb = gb.WithWhere(fb)
	}
	gb = gb.WithLimit(q.TopK)

	resp, err := gb.Do(ctx)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("weaviate vector search failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrVectorSearchFailed, err)
	}
	if err = graphqlErrorsAs(resp, ErrVectorSearchFailed); err != nil {
		return nil, err
	}

	res := &vector.Result[DTO]{
		Hits:  make([]vector.Hit[DTO], 0),
		Total: 0,
	}
	for _, hit := range graphqlHits(resp, r.collection) {
		m := hitMap(hit)
		if m == nil {
			continue
		}
		props := stripAdditional(m)
		// 纵深防御：服务端条件已注入租户，客户端再校验一次；不匹配静默剔除。
		if verr := verifyTenantOnProps[ENTITY](ctx, props); verr != nil {
			if errors.Is(verr, errTenantMismatch) {
				continue
			}
			return nil, verr
		}
		score := weaviateDistanceToScore(q.Metric, additionalDistance(m))
		if q.MinScore > 0 && score < q.MinScore {
			continue
		}
		var ent ENTITY
		if err = entityFromProps(props, &ent); err != nil {
			return nil, err
		}
		setIDFieldValue(&ent, additionalID(m))
		dto := r.mapper.ToDTO(&ent)
		if dto == nil {
			continue
		}
		res.Hits = append(res.Hits, vector.Hit[DTO]{
			Score: score,
			Value: *dto,
		})
	}
	res.Total = int64(len(res.Hits))
	return res, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// GraphQL 响应解析辅助。
// ─────────────────────────────────────────────────────────────────────────────

// graphqlErrorsAs 折叠 GraphQL 错误列表（哨兵随调用路径语义选择）。
func graphqlErrorsAs(resp *models.GraphQLResponse, fallback error) error {
	if resp == nil || len(resp.Errors) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %s", fallback, resp.Errors[0].Message)
}

// graphqlHits 取 Get 响应中本 class 的命中列表。
func graphqlHits(resp *models.GraphQLResponse, class string) []any {
	if resp == nil || resp.Data == nil {
		return nil
	}
	get, _ := resp.Data["Get"].(map[string]any)
	if get == nil {
		return nil
	}
	hits, _ := get[class].([]any)
	return hits
}

// hitMap 命中转映射（类型不符返回 nil）。
func hitMap(hit any) map[string]any {
	m, _ := hit.(map[string]any)
	return m
}

// stripAdditional 摘除 _additional 列，余下为对象属性。
func stripAdditional(m map[string]any) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		if k == "_additional" {
			continue
		}
		out[k] = v
	}
	return out
}

// additionalID 取命中的对象 UUID。
func additionalID(m map[string]any) string {
	add, _ := m["_additional"].(map[string]any)
	if add == nil {
		return ""
	}
	id, _ := add["id"].(string)
	return id
}

// additionalDistance 取命中的向量距离（缺失返回 0）。
func additionalDistance(m map[string]any) float64 {
	add, _ := m["_additional"].(map[string]any)
	if add == nil {
		return 0
	}
	return jsonNumberToFloat64(add["distance"])
}

// aggregateCount 取 Aggregate 响应的 meta.count。
func aggregateCount(resp *models.GraphQLResponse, class string) int64 {
	if resp == nil || resp.Data == nil {
		return 0
	}
	agg, _ := resp.Data["Aggregate"].(map[string]any)
	if agg == nil {
		return 0
	}
	rows, _ := agg[class].([]any)
	if len(rows) == 0 {
		return 0
	}
	row, _ := rows[0].(map[string]any)
	if row == nil {
		return 0
	}
	meta, _ := row["meta"].(map[string]any)
	if meta == nil {
		return 0
	}
	return int64(jsonNumberToFloat64(meta["count"]))
}

// jsonNumberToFloat64 JSON 数值宽容转换（服务端返回 float64，替身可给出整型）。
func jsonNumberToFloat64(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int64:
		return float64(n)
	case int:
		return float64(n)
	case json.Number:
		f, _ := n.Float64()
		return f
	}
	return 0
}
