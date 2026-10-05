package qdrant

import (
	"context"
	"errors"
	"fmt"

	qdrant "github.com/qdrant/go-client/qdrant"

	"github.com/tx7do/go-utils/mapper"
	"github.com/tx7do/go-wind/log"

	"github.com/tx7do/go-crud/vector"
	"github.com/tx7do/go-crud/viewer"
)

// ─────────────────────────────────────────────────────────────────────────────
// Qdrant 仓库（泛型）。
//
// Qdrant 是纯向量数据库，仓库面为：
//   - Create / BatchCreate：点 Upsert（ID + 向量 + 载荷三通道，见 pointFromEntity）
//   - Get / GetByUUID：按点 ID 取回（协议无 Filter，租户校验在客户端做）
//   - DeleteByIDs / DeleteByUUIDs / DeleteByFilter：删点（租户先过滤后删）
//   - Count / Exists：按 Filter 计数（含租户注入）
//   - SearchByVector：统一契约的向量检索（kNN TopK）
//
// 不提供 ListWithPaging：Qdrant 的 Scroll 分页按点 ID 游标推进，与
// offset/page/token 分页契约不兼容，向量检索本身就是 TopK 语义。
//
// 租户隔离（嵌入 qdrant/mixin.TenantID 的实体自动启用）：
//   - 写入：租户业务视图下强制覆盖 tenant_id（EnforceOnScopedInstance）；
//   - 可注入 Filter 的路径（DeleteByFilter/Count/Exists/SearchByVector）：
//     服务端注入 tenant_id 匹配条件（InjectTenantFilterIntoQdrantFilter）；
//   - 协议无 Filter 的路径（Get*/DeleteByIDs/DeleteByUUIDs）：
//     客户端按取回载荷校验/过滤租户（verifyTenantOnPayload / payloadTenantID）。
// ─────────────────────────────────────────────────────────────────────────────

// Repository Qdrant 版仓库（泛型）
type Repository[DTO any, ENTITY any] struct {
	mapper *mapper.CopierMapper[DTO, ENTITY]

	client     *Client
	collection string
}

// NewRepository 创建 Qdrant 仓库实例。
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

// Create 插入一个点（Upsert 语义：同 ID 覆盖）。
func (r *Repository[DTO, ENTITY]) Create(ctx context.Context, dto *DTO) (*DTO, error) {
	if r.client == nil || r.client.cli == nil {
		return nil, ErrClientNotInitialized
	}
	if r.collection == "" {
		return nil, ErrInvalidRequest
	}
	if dto == nil {
		return nil, ErrInvalidRequest
	}

	ent := r.mapper.ToEntity(dto)

	// 租户强制：tenant-scoped 实体在租户业务视图下强制覆盖 tenant_id。
	if err := viewer.EnforceOnScopedInstance(ctx, ent); err != nil {
		return nil, err
	}

	pt, err := pointFromEntity(ent)
	if err != nil {
		return nil, err
	}

	if _, err = r.client.cli.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: r.collection,
		Points:         []*qdrant.PointStruct{pt},
	}); err != nil {
		log.Error(context.Background(), fmt.Sprintf("qdrant upsert failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrInsertFailed, err)
	}

	return r.mapper.ToDTO(ent), nil
}

// BatchCreate 批量插入点（单次 Upsert）。
func (r *Repository[DTO, ENTITY]) BatchCreate(ctx context.Context, dtos []*DTO) ([]*DTO, error) {
	if r.client == nil || r.client.cli == nil {
		return nil, ErrClientNotInitialized
	}
	if r.collection == "" {
		return nil, ErrInvalidRequest
	}
	if len(dtos) == 0 {
		return nil, nil
	}

	pts := make([]*qdrant.PointStruct, 0, len(dtos))
	ents := make([]*ENTITY, 0, len(dtos))
	for _, d := range dtos {
		e := r.mapper.ToEntity(d)
		// 租户强制：每个实体在租户业务视图下强制覆盖 tenant_id。
		if err := viewer.EnforceOnScopedInstance(ctx, e); err != nil {
			return nil, err
		}
		pt, err := pointFromEntity(e)
		if err != nil {
			return nil, err
		}
		ents = append(ents, e)
		pts = append(pts, pt)
	}

	if _, err := r.client.cli.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: r.collection,
		Points:         pts,
	}); err != nil {
		log.Error(context.Background(), fmt.Sprintf("qdrant upsert batch failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrInsertFailed, err)
	}

	out := make([]*DTO, 0, len(ents))
	for _, e := range ents {
		out = append(out, r.mapper.ToDTO(e))
	}
	return out, nil
}

// Get 按数值点 ID 取回单点。
func (r *Repository[DTO, ENTITY]) Get(ctx context.Context, id uint64) (*DTO, error) {
	return r.get(ctx, qdrant.NewIDNum(id))
}

// GetByUUID 按 UUID 点 ID 取回单点。
func (r *Repository[DTO, ENTITY]) GetByUUID(ctx context.Context, uuid string) (*DTO, error) {
	return r.get(ctx, qdrant.NewIDUUID(uuid))
}

func (r *Repository[DTO, ENTITY]) get(ctx context.Context, pid *qdrant.PointId) (*DTO, error) {
	if r.client == nil || r.client.cli == nil {
		return nil, ErrClientNotInitialized
	}
	if r.collection == "" {
		return nil, ErrInvalidRequest
	}
	if pid == nil {
		return nil, ErrInvalidRequest
	}

	pts, err := r.client.cli.Get(ctx, &qdrant.GetPoints{
		CollectionName: r.collection,
		Ids:            []*qdrant.PointId{pid},
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("qdrant get failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrQueryFailed, err)
	}
	if len(pts) == 0 || pts[0] == nil {
		return nil, ErrPointNotFound
	}

	// GetPoints 协议无 Filter，租户校验在客户端做（语义同注入路径）。
	// 不匹配与不存在对外同构（ErrPointNotFound），避免存在性泄露。
	if verr := verifyTenantOnPayload[ENTITY](ctx, pts[0].Payload); verr != nil {
		if errors.Is(verr, errTenantMismatch) {
			return nil, ErrPointNotFound
		}
		return nil, verr
	}

	var ent ENTITY
	if err = entityFromPayload(pts[0].Payload, &ent); err != nil {
		return nil, err
	}

	dto := r.mapper.ToDTO(&ent)
	return dto, nil
}

// DeleteByIDs 按数值点 ID 删除。
// tenant-scoped 实体在租户业务视图下：先按 ID 取回、剔除他租户点，再删
// （GetPoints 协议无 Filter，无法服务端过滤，客户端先过滤保证不越权删除）。
func (r *Repository[DTO, ENTITY]) DeleteByIDs(ctx context.Context, ids []uint64) (int64, error) {
	pids := make([]*qdrant.PointId, 0, len(ids))
	for _, id := range ids {
		pids = append(pids, qdrant.NewIDNum(id))
	}
	return r.deleteByPointIDs(ctx, pids)
}

// DeleteByUUIDs 按 UUID 点 ID 删除（租户语义同 DeleteByIDs）。
func (r *Repository[DTO, ENTITY]) DeleteByUUIDs(ctx context.Context, uuids []string) (int64, error) {
	pids := make([]*qdrant.PointId, 0, len(uuids))
	for _, u := range uuids {
		pids = append(pids, qdrant.NewIDUUID(u))
	}
	return r.deleteByPointIDs(ctx, pids)
}

func (r *Repository[DTO, ENTITY]) deleteByPointIDs(ctx context.Context, pids []*qdrant.PointId) (int64, error) {
	if r.client == nil || r.client.cli == nil {
		return 0, ErrClientNotInitialized
	}
	if r.collection == "" {
		return 0, ErrInvalidRequest
	}
	if len(pids) == 0 {
		return 0, nil
	}

	deletable := pids
	if viewer.IsTenantScopedType[ENTITY]() {
		dec, err := viewer.EnforceTenant(ctx)
		if err != nil {
			return 0, err
		}
		if dec.Enforce {
			deletable, err = r.tenantFilterPointIDs(ctx, pids, dec.TenantID)
			if err != nil {
				return 0, err
			}
			if len(deletable) == 0 {
				return 0, nil
			}
		}
	}

	if _, err := r.client.cli.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: r.collection,
		Points:         qdrant.NewPointsSelectorIDs(deletable),
	}); err != nil {
		log.Error(context.Background(), fmt.Sprintf("qdrant delete failed: %v", err))
		return 0, fmt.Errorf("%w: %v", ErrDeleteFailed, err)
	}
	return int64(len(deletable)), nil
}

// tenantFilterPointIDs 取回候选点并剔除不属于当前租户的点 ID。
func (r *Repository[DTO, ENTITY]) tenantFilterPointIDs(ctx context.Context, pids []*qdrant.PointId, tenantID uint64) ([]*qdrant.PointId, error) {
	pts, err := r.client.cli.Get(ctx, &qdrant.GetPoints{
		CollectionName: r.collection,
		Ids:            pids,
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("qdrant get for tenant filter failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrQueryFailed, err)
	}
	out := make([]*qdrant.PointId, 0, len(pts))
	for _, pt := range pts {
		if pt == nil {
			continue
		}
		if tid, ok := payloadTenantID(pt.Payload); ok && tid == uint32(tenantID) {
			out = append(out, pt.Id)
		}
	}
	return out, nil
}

// DeleteByFilter 按引擎原生 Filter 删除（必须显式给出 Filter，防误删全集合）。
// tenant-scoped 实体在租户业务视图下服务端注入 tenant_id 匹配条件。
func (r *Repository[DTO, ENTITY]) DeleteByFilter(ctx context.Context, filter *qdrant.Filter) (int64, error) {
	if r.client == nil || r.client.cli == nil {
		return 0, ErrClientNotInitialized
	}
	if r.collection == "" {
		return 0, ErrInvalidRequest
	}
	if filter == nil {
		return 0, ErrInvalidRequest
	}

	filter, err := InjectTenantFilterIntoQdrantFilter[ENTITY](ctx, filter)
	if err != nil {
		return 0, err
	}

	// 引擎不返回删除计数，先按同一（已注入租户）Filter 计数以回报准确值。
	total, err := r.countWithFilter(ctx, filter)
	if err != nil {
		return 0, err
	}

	if _, err = r.client.cli.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: r.collection,
		Points:         qdrant.NewPointsSelectorFilter(filter),
	}); err != nil {
		log.Error(context.Background(), fmt.Sprintf("qdrant delete by filter failed: %v", err))
		return 0, fmt.Errorf("%w: %v", ErrDeleteFailed, err)
	}
	return total, nil
}

// Count 按引擎原生 Filter 计数（tenant-scoped 实体注入 tenant_id 条件；
// Filter 为 nil 时计全集合——租户视图下即为该租户的全部点）。
func (r *Repository[DTO, ENTITY]) Count(ctx context.Context, filter *qdrant.Filter) (int64, error) {
	if r.client == nil || r.client.cli == nil {
		return 0, ErrClientNotInitialized
	}
	if r.collection == "" {
		return 0, ErrInvalidRequest
	}

	filter, err := InjectTenantFilterIntoQdrantFilter[ENTITY](ctx, filter)
	if err != nil {
		return 0, err
	}
	return r.countWithFilter(ctx, filter)
}

func (r *Repository[DTO, ENTITY]) countWithFilter(ctx context.Context, filter *qdrant.Filter) (int64, error) {
	n, err := r.client.cli.Count(ctx, &qdrant.CountPoints{
		CollectionName: r.collection,
		Filter:         filter,
		Exact:          ptr(true),
	})
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("qdrant count failed: %v", err))
		return 0, fmt.Errorf("%w: %v", ErrCountFailed, err)
	}
	return int64(n), nil
}

// Exists 判断是否存在符合 Filter 的点（tenant-scoped 实体注入 tenant_id 条件）。
func (r *Repository[DTO, ENTITY]) Exists(ctx context.Context, filter *qdrant.Filter) (bool, error) {
	n, err := r.Count(ctx, filter)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// SearchByVector 统一契约的向量检索（Qdrant Query API，kNN TopK）。
//
// Filter 注入 tenant_id（pre-filter）；分数按度量换算为统一相似度语义
// （Euclid 距离 → 1/(1+d)，其余原生相似度直接透传）；MinScore 在统一分数
// 空间客户端过滤（ScoreThreshold 的原生方向随度量翻转，不透传）。
// 租户不匹配的命中被静默剔除（纵深防御：服务端 Filter 已限定租户）。
func (r *Repository[DTO, ENTITY]) SearchByVector(ctx context.Context, q *vector.Query) (*vector.Result[DTO], error) {
	if r.client == nil || r.client.cli == nil {
		return nil, ErrClientNotInitialized
	}
	if r.collection == "" {
		return nil, ErrInvalidRequest
	}

	var filter *qdrant.Filter
	if f, ok := q.Filter.(*qdrant.Filter); ok {
		filter = f
	}
	filter, err := InjectTenantFilterIntoQdrantFilter[ENTITY](ctx, filter)
	if err != nil {
		return nil, err
	}

	req, err := buildQueryPoints(r.collection, q, filter)
	if err != nil {
		return nil, err
	}

	pts, err := r.client.cli.Query(ctx, req)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("qdrant vector query failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrVectorSearchFailed, err)
	}

	res := &vector.Result[DTO]{
		Hits:  make([]vector.Hit[DTO], 0, len(pts)),
		Total: 0,
	}
	for _, pt := range pts {
		if pt == nil {
			continue
		}
		// 纵深防御：服务端 Filter 已注入租户，客户端再校验一次载荷租户。
		if verr := verifyTenantOnPayload[ENTITY](ctx, pt.Payload); verr != nil {
			if errors.Is(verr, errTenantMismatch) {
				continue
			}
			return nil, verr
		}
		score := qdrantScoreToScore(q.Metric, pt.Score)
		if q.MinScore > 0 && score < q.MinScore {
			continue
		}
		var ent ENTITY
		if err = entityFromPayload(pt.Payload, &ent); err != nil {
			return nil, err
		}
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
