package milvus

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"

	"github.com/tx7do/go-utils/mapper"
	windErrors "github.com/tx7do/go-wind/errors"
	"github.com/tx7do/go-wind/log"

	"github.com/tx7do/go-crud/vector"
	"github.com/tx7do/go-crud/viewer"
)

// ─────────────────────────────────────────────────────────────────────────────
// Milvus 仓库（泛型）。
//
// Milvus 是纯向量数据库，仓库面为：
//   - CreateCollection：按实体映射构建 schema（tenant 字段标记 partition
//     key）、建立 AUTOINDEX 向量索引并加载集合（度量在索引期固定）；
//   - Create / BatchCreate：实体 → 列（官方 AnyToColumns）→ Upsert
//     （主键由调用方给定，非自增）；
//   - Get / GetByUUID：按主键取回（协议无过滤表达式，租户校验在客户端做）；
//   - QueryByExpr / Count / Exists / DeleteByExpr：按 Milvus 表达式
//     （租户谓词注入进表达式）；
//   - DeleteByIDs / DeleteByUUIDs：按主键删除（租户先过滤后删）；
//   - SearchByVector：统一契约的向量检索（kNN TopK，AUTOINDEX 检索参数）。
//
// 不提供 ListWithPaging：Milvus 无排序/游标分页能力，向量检索本身是
// TopK 语义。
//
// 租户隔离（嵌入 milvus/mixin.TenantID 的实体自动启用）：
//   - 写入：租户业务视图下强制覆盖 tenant_id（EnforceOnScopedInstance）；
//   - 表达式路径（QueryByExpr/DeleteByExpr/Count/Exists/SearchByVector）：
//     服务端注入 tenant_id == tid 谓词（InjectTenantFilterIntoExpr）；
//   - 主键直取路径（Get*/DeleteByIDs/DeleteByUUIDs）：客户端按取回行的
//     tenant_id 列校验/过滤（checkTenantColumn）。
// ─────────────────────────────────────────────────────────────────────────────

// Repository Milvus 版仓库（泛型）
type Repository[DTO any, ENTITY any] struct {
	mapper *mapper.CopierMapper[DTO, ENTITY]

	client     *Client
	collection string
}

// NewRepository 创建 Milvus 仓库实例。
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

// HasCollection 判断本仓库绑定的集合是否存在。
func (r *Repository[DTO, ENTITY]) HasCollection(ctx context.Context) (bool, error) {
	if r.client == nil || r.client.cli == nil {
		return false, ErrClientNotInitialized
	}
	if r.collection == "" {
		return false, ErrInvalidRequest
	}
	return r.client.HasCollection(ctx, r.collection)
}

// DropCollection 删除本仓库绑定的集合。
func (r *Repository[DTO, ENTITY]) DropCollection(ctx context.Context) error {
	if r.client == nil || r.client.cli == nil {
		return ErrClientNotInitialized
	}
	if r.collection == "" {
		return ErrInvalidRequest
	}
	return r.client.DropCollection(ctx, r.collection)
}

// ─── 内部工具 ──────────────────────────────────────────────────────────────

// specsOf 收集本仓库实体类型的字段映射表。
func specsOf[ENTITY any]() ([]fieldSpec, error) {
	var e ENTITY
	return collectFieldSpecs(reflect.TypeOf(&e).Elem())
}

// pkSpec 返回主键字段映射；缺失或多于一个均报错。
func pkSpecOf(specs []fieldSpec) (*fieldSpec, error) {
	var pk *fieldSpec
	for i := range specs {
		if specs[i].isPK {
			if pk != nil {
				return nil, fmt.Errorf("%w: multiple primary key fields", ErrInvalidPointID)
			}
			pk = &specs[i]
		}
	}
	if pk == nil {
		return nil, fmt.Errorf("%w: entity has no primary key field", ErrInvalidPointID)
	}
	return pk, nil
}

// nonVectorSpecNames 非向量字段名（查询输出字段集：主键、tenant_id、标量）。
func nonVectorSpecNames(specs []fieldSpec) []string {
	var out []string
	for _, spec := range specs {
		if !spec.isVector {
			out = append(out, spec.name)
		}
	}
	return out
}

// numericPkColumn 构建数值主键列（查询/删除按主键定位）。
func numericPkColumn(pk *fieldSpec, ids []uint64) (entity.Column, error) {
	if pk.kind != reflect.Int64 {
		return nil, fmt.Errorf("%w: primary key is not int64", ErrInvalidPointID)
	}
	vals := make([]int64, 0, len(ids))
	for _, id := range ids {
		vals = append(vals, int64(id))
	}
	return entity.NewColumnInt64(pk.name, vals), nil
}

// varcharPkColumn 构建 VarChar 主键列。
// 值中含引号/反斜杠拒绝（官方 PKs2Expr 无转义，防止表达式逃逸）。
func varcharPkColumn(pk *fieldSpec, uuids []string) (entity.Column, error) {
	if pk.kind != reflect.String {
		return nil, fmt.Errorf("%w: primary key is not varchar", ErrInvalidPointID)
	}
	for _, u := range uuids {
		if strings.ContainsAny(u, "\"\\") {
			return nil, fmt.Errorf("%w: uuid contains quote/backslash", ErrInvalidRequest)
		}
	}
	return entity.NewColumnVarChar(pk.name, uuids), nil
}

// ─── 集合创建 ──────────────────────────────────────────────────────────────

// CreateCollection 按本仓库实体映射创建集合并建立向量索引：
// schema 构建见 buildSchema；向量字段统一建立 AUTOINDEX 索引（度量取自
// 参数，索引期固定）并同步加载集合。
//
//	@param ctx 上下文
//	@param dims 向量维度（须与实体向量字段长度一致）
//	@param metric 距离度量（cosine / euclidean / dot），未指定按 cosine
func (r *Repository[DTO, ENTITY]) CreateCollection(
	ctx context.Context,
	dims int,
	metric vector.DistanceMetric,
) error {
	if r.client == nil || r.client.cli == nil {
		return ErrClientNotInitialized
	}
	if r.collection == "" {
		return ErrInvalidRequest
	}
	if metric == "" {
		metric = vector.MetricCosine
	}
	mt, ok := milvusMetrics[metric]
	if !ok {
		return fmt.Errorf("%w: unsupported vector metric %q", ErrInvalidRequest, metric)
	}

	schema, err := buildSchema[ENTITY](r.collection, dims)
	if err != nil {
		return err
	}

	// 强一致建集合：DAL 的 Create 语义要求写后立读，默认的 Bounded
	// 一致性（有界旧序）会让紧随写入的读取看到旧状态。
	if err = r.client.cli.CreateCollection(ctx, schema, entity.DefaultShardNumber, client.WithConsistencyLevel(entity.ClStrong)); err != nil {
		return fmt.Errorf("%w: %v", ErrInsertFailed, err)
	}
	for _, vf := range vectorFieldNames(schema) {
		idx, ierr := entity.NewIndexAUTOINDEX(mt)
		if ierr != nil {
			return fmt.Errorf("%w: %v", ErrInsertFailed, ierr)
		}
		if ierr = r.client.cli.CreateIndex(ctx, r.collection, vf, idx, false); ierr != nil {
			return fmt.Errorf("%w: %v", ErrInsertFailed, ierr)
		}
	}
	if err = r.client.cli.LoadCollection(ctx, r.collection, false); err != nil {
		return fmt.Errorf("%w: %v", ErrInsertFailed, err)
	}
	return nil
}

// ─── 写入 ──────────────────────────────────────────────────────────────────

// insertEntities 实体集 → 列 → Upsert。
func (r *Repository[DTO, ENTITY]) insertEntities(ctx context.Context, ents []*ENTITY) error {
	specs, err := specsOf[ENTITY]()
	if err != nil {
		return err
	}
	dims := 0
	for _, e := range ents {
		if d := vectorDimsOf(specs, e); d > dims {
			dims = d
		}
	}
	if dims == 0 {
		return fmt.Errorf("%w: entity has no vector data", ErrInvalidRequest)
	}
	schema, err := buildSchema[ENTITY](r.collection, dims)
	if err != nil {
		return err
	}
	rows := make([]interface{}, 0, len(ents))
	for _, e := range ents {
		rows = append(rows, e)
	}
	cols, err := entity.AnyToColumns(rows, schema)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrColumnConversion, err)
	}
	coerceStringColumnsToVarChar(cols)
	if _, err = r.client.cli.Upsert(ctx, r.collection, "", cols...); err != nil {
		log.Error(context.Background(), fmt.Sprintf("milvus upsert failed: %v", err))
		return fmt.Errorf("%w: %v", ErrInsertFailed, err)
	}
	return nil
}

// coerceStringColumnsToVarChar 把 AnyToColumns 产出的 String 列原地替换为
// 同名同数据的 VarChar 列：SDK v2.4.2 的 AnyToColumns 对 VarChar 字段也
// 构造 NewColumnString（entity/rows.go 的 FieldTypeString/FieldTypeVarChar
// 共用分支），而 2.4.x 服务端要求 VarChar 字段收取 VarChar 列，直插报
// "param column ... has type type:String but collection field definition
// is string"。
func coerceStringColumnsToVarChar(cols []entity.Column) {
	for i, col := range cols {
		if cs, ok := col.(*entity.ColumnString); ok {
			cols[i] = entity.NewColumnVarChar(cs.Name(), cs.Data())
		}
	}
}

// vectorDimsOf 取实体向量字段的最大长度（schema 的维度）。
func vectorDimsOf[ENTITY any](specs []fieldSpec, ent *ENTITY) int {
	if ent == nil {
		return 0
	}
	rv := reflect.ValueOf(ent).Elem()
	dims := 0
	for _, spec := range specs {
		if !spec.isVector {
			continue
		}
		if l := rv.FieldByIndex(spec.path).Len(); l > dims {
			dims = l
		}
	}
	return dims
}

// Create 插入一条记录（Upsert 语义：同主键覆盖）。
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

	if err := r.insertEntities(ctx, []*ENTITY{ent}); err != nil {
		return nil, err
	}
	return r.mapper.ToDTO(ent), nil
}

// BatchCreate 批量插入（单次 Upsert）。
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

	ents := make([]*ENTITY, 0, len(dtos))
	for _, d := range dtos {
		e := r.mapper.ToEntity(d)
		// 租户强制：每个实体在租户业务视图下强制覆盖 tenant_id。
		if err := viewer.EnforceOnScopedInstance(ctx, e); err != nil {
			return nil, err
		}
		ents = append(ents, e)
	}

	if err := r.insertEntities(ctx, ents); err != nil {
		return nil, err
	}

	out := make([]*DTO, 0, len(ents))
	for _, e := range ents {
		out = append(out, r.mapper.ToDTO(e))
	}
	return out, nil
}

// ─── 读取 ──────────────────────────────────────────────────────────────────

// Get 按数值主键取回单条记录。
func (r *Repository[DTO, ENTITY]) Get(ctx context.Context, id uint64) (*DTO, error) {
	return r.getByPk(ctx, func(pk *fieldSpec) (entity.Column, error) {
		return numericPkColumn(pk, []uint64{id})
	})
}

// GetByUUID 按 VarChar 主键取回单条记录。
func (r *Repository[DTO, ENTITY]) GetByUUID(ctx context.Context, uuid string) (*DTO, error) {
	return r.getByPk(ctx, func(pk *fieldSpec) (entity.Column, error) {
		return varcharPkColumn(pk, []string{uuid})
	})
}

func (r *Repository[DTO, ENTITY]) getByPk(ctx context.Context, mkCol func(*fieldSpec) (entity.Column, error)) (*DTO, error) {
	if r.client == nil || r.client.cli == nil {
		return nil, ErrClientNotInitialized
	}
	if r.collection == "" {
		return nil, ErrInvalidRequest
	}

	specs, err := specsOf[ENTITY]()
	if err != nil {
		return nil, err
	}
	pk, err := pkSpecOf(specs)
	if err != nil {
		return nil, err
	}
	pkCol, err := mkCol(pk)
	if err != nil {
		return nil, err
	}

	rs, err := r.client.cli.QueryByPks(ctx, r.collection, []string{}, pkCol, nonVectorSpecNames(specs))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("milvus query by pks failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrQueryFailed, err)
	}
	if rs.Len() == 0 {
		return nil, ErrPointNotFound
	}

	// 按主键直取无过滤表达式，租户校验在客户端做。
	// 不匹配与不存在对外同构（ErrPointNotFound），避免存在性泄露。
	if verr := checkTenantColumn[ENTITY](ctx, rs, 0); verr != nil {
		if windErrors.Is(verr, errTenantMismatch) {
			return nil, ErrPointNotFound
		}
		return nil, verr
	}

	var ent ENTITY
	if err = entityFromColumns(rs, 0, &ent); err != nil {
		return nil, err
	}
	return r.mapper.ToDTO(&ent), nil
}

// QueryByExpr 按 Milvus 表达式查询（tenant-scoped 实体注入 tenant_id 谓词；
// 租户不匹配的行被剔除——服务端谓词已限定，此处为纵深防御）。
func (r *Repository[DTO, ENTITY]) QueryByExpr(ctx context.Context, expr string) ([]*DTO, error) {
	if r.client == nil || r.client.cli == nil {
		return nil, ErrClientNotInitialized
	}
	if r.collection == "" {
		return nil, ErrInvalidRequest
	}

	expr, err := InjectTenantFilterIntoExpr[ENTITY](ctx, expr)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(expr) == "" {
		return nil, ErrInvalidRequest
	}

	specs, err := specsOf[ENTITY]()
	if err != nil {
		return nil, err
	}

	rs, err := r.client.cli.Query(ctx, r.collection, []string{}, expr, nonVectorSpecNames(specs))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("milvus query failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrQueryFailed, err)
	}

	out := make([]*DTO, 0, rs.Len())
	for i := 0; i < rs.Len(); i++ {
		if verr := checkTenantColumn[ENTITY](ctx, rs, i); verr != nil {
			if windErrors.Is(verr, errTenantMismatch) {
				continue
			}
			return nil, verr
		}
		var ent ENTITY
		if err = entityFromColumns(rs, i, &ent); err != nil {
			return nil, err
		}
		if dto := r.mapper.ToDTO(&ent); dto != nil {
			out = append(out, dto)
		}
	}
	return out, nil
}

// ─── 删除 ──────────────────────────────────────────────────────────────────

// DeleteByIDs 按数值主键删除。
// tenant-scoped 实体在租户业务视图下：先按主键取回、剔除他租户行，再删
// （按主键直取无过滤表达式，客户端先过滤保证不越权删除）。
func (r *Repository[DTO, ENTITY]) DeleteByIDs(ctx context.Context, ids []uint64) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	return r.deleteByPk(ctx, func(pk *fieldSpec) (entity.Column, error) {
		return numericPkColumn(pk, ids)
	})
}

// DeleteByUUIDs 按 VarChar 主键删除（租户语义同 DeleteByIDs）。
func (r *Repository[DTO, ENTITY]) DeleteByUUIDs(ctx context.Context, uuids []string) (int64, error) {
	if len(uuids) == 0 {
		return 0, nil
	}
	return r.deleteByPk(ctx, func(pk *fieldSpec) (entity.Column, error) {
		return varcharPkColumn(pk, uuids)
	})
}

func (r *Repository[DTO, ENTITY]) deleteByPk(ctx context.Context, mkCol func(*fieldSpec) (entity.Column, error)) (int64, error) {
	if r.client == nil || r.client.cli == nil {
		return 0, ErrClientNotInitialized
	}
	if r.collection == "" {
		return 0, ErrInvalidRequest
	}

	specs, err := specsOf[ENTITY]()
	if err != nil {
		return 0, err
	}
	pk, err := pkSpecOf(specs)
	if err != nil {
		return 0, err
	}
	pkCol, err := mkCol(pk)
	if err != nil {
		return 0, err
	}

	deletable := pkCol
	if viewer.IsTenantScopedType[ENTITY]() {
		dec, err := viewer.EnforceTenant(ctx)
		if err != nil {
			return 0, err
		}
		if dec.Enforce {
			deletable, err = r.tenantFilterPks(ctx, pk, pkCol)
			if err != nil {
				return 0, err
			}
			if deletable.Len() == 0 {
				return 0, nil
			}
		}
	}

	if err = r.client.cli.DeleteByPks(ctx, r.collection, "", deletable); err != nil {
		log.Error(context.Background(), fmt.Sprintf("milvus delete by pks failed: %v", err))
		return 0, fmt.Errorf("%w: %v", ErrDeleteFailed, err)
	}
	return int64(deletable.Len()), nil
}

// tenantFilterPks 取回候选行并剔除不属于当前租户的主键值
// （租户判定经 checkTenantColumn，按当前上下文决策）。
func (r *Repository[DTO, ENTITY]) tenantFilterPks(ctx context.Context, pk *fieldSpec, pkCol entity.Column) (entity.Column, error) {
	// 仅取主键与 tenant_id 两列做判定。
	rs, err := r.client.cli.QueryByPks(ctx, r.collection, []string{}, pkCol, []string{pk.name, "tenant_id"})
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("milvus query by pks for tenant filter failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrQueryFailed, err)
	}
	if pk.kind == reflect.Int64 {
		var keep []int64
		for i := 0; i < rs.Len(); i++ {
			if verr := checkTenantColumn[ENTITY](ctx, rs, i); verr != nil {
				if windErrors.Is(verr, errTenantMismatch) {
					continue
				}
				return nil, verr
			}
			pkc := rs.GetColumn(pk.name)
			if pkc == nil {
				// 服务端省略了请求的主键列：该行不可判属，跳过。
				continue
			}
			pvv, gerr := pkc.Get(i)
			if gerr != nil {
				continue
			}
			if v, ok := pvv.(int64); ok {
				keep = append(keep, v)
			}
		}
		return entity.NewColumnInt64(pk.name, keep), nil
	}
	var keepStr []string
	for i := 0; i < rs.Len(); i++ {
		if verr := checkTenantColumn[ENTITY](ctx, rs, i); verr != nil {
			if windErrors.Is(verr, errTenantMismatch) {
				continue
			}
			return nil, verr
		}
		pkc := rs.GetColumn(pk.name)
		if pkc == nil {
			// 服务端省略了请求的主键列：该行不可判属，跳过。
			continue
		}
		pvv, gerr := pkc.Get(i)
		if gerr != nil {
			continue
		}
		if v, ok := pvv.(string); ok {
			keepStr = append(keepStr, v)
		}
	}
	return entity.NewColumnVarChar(pk.name, keepStr), nil
}

// DeleteByExpr 按 Milvus 表达式删除（必须显式给出非空表达式或处于租户
// 业务视图下的 tenant-scoped 实体——注入后的租户谓词即非空；否则全表
// 删除需另行显式确认，拒绝空表达式防误删）。
func (r *Repository[DTO, ENTITY]) DeleteByExpr(ctx context.Context, expr string) (int64, error) {
	if r.client == nil || r.client.cli == nil {
		return 0, ErrClientNotInitialized
	}
	if r.collection == "" {
		return 0, ErrInvalidRequest
	}

	expr, err := InjectTenantFilterIntoExpr[ENTITY](ctx, expr)
	if err != nil {
		return 0, err
	}
	if strings.TrimSpace(expr) == "" {
		return 0, ErrInvalidRequest
	}

	// 引擎不返回删除计数，先按同一（已注入租户）表达式计数以回报准确值。
	total, err := r.countExpr(ctx, expr)
	if err != nil {
		return 0, err
	}

	if err = r.client.cli.Delete(ctx, r.collection, "", expr); err != nil {
		log.Error(context.Background(), fmt.Sprintf("milvus delete failed: %v", err))
		return 0, fmt.Errorf("%w: %v", ErrDeleteFailed, err)
	}
	return total, nil
}

// ─── 计数 ──────────────────────────────────────────────────────────────────

// Count 按表达式计数（tenant-scoped 实体注入 tenant_id 谓词）。
// 优先走服务端 count(*) 聚合；不支持时回退按主键列取行计数。
func (r *Repository[DTO, ENTITY]) Count(ctx context.Context, expr string) (int64, error) {
	if r.client == nil || r.client.cli == nil {
		return 0, ErrClientNotInitialized
	}
	if r.collection == "" {
		return 0, ErrInvalidRequest
	}

	expr, err := InjectTenantFilterIntoExpr[ENTITY](ctx, expr)
	if err != nil {
		return 0, err
	}
	if strings.TrimSpace(expr) == "" {
		return 0, ErrInvalidRequest
	}
	return r.countExpr(ctx, expr)
}

func (r *Repository[DTO, ENTITY]) countExpr(ctx context.Context, expr string) (int64, error) {
	// 首选：count(*) 聚合（服务端支持时返回名为 count(*) 的 Int64 列）。
	rs, err := r.client.cli.Query(ctx, r.collection, []string{}, expr, []string{"count(*)"})
	if err == nil {
		if col := rs.GetColumn("count(*)"); col != nil {
			if v, gerr := col.Get(0); gerr == nil {
				if n, ok := v.(int64); ok {
					return n, nil
				}
			}
		}
	} else {
		log.Error(context.Background(), fmt.Sprintf("milvus count(*) query failed, falling back to pk count: %v", err))
	}

	// 回退：按主键列取行计数。
	specs, err := specsOf[ENTITY]()
	if err != nil {
		return 0, err
	}
	pk, err := pkSpecOf(specs)
	if err != nil {
		return 0, err
	}
	rs, err = r.client.cli.Query(ctx, r.collection, []string{}, expr, []string{pk.name})
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("milvus count query failed: %v", err))
		return 0, fmt.Errorf("%w: %v", ErrCountFailed, err)
	}
	return int64(rs.Len()), nil
}

// Exists 判断是否存在符合表达式的记录（tenant-scoped 实体注入 tenant_id 谓词）。
func (r *Repository[DTO, ENTITY]) Exists(ctx context.Context, expr string) (bool, error) {
	n, err := r.Count(ctx, expr)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// ─── 向量检索 ──────────────────────────────────────────────────────────────

// SearchByVector 统一契约的向量检索（Milvus Search，kNN TopK）。
//
// 表达式注入 tenant_id（pre-filter）；度量按统一枚举映射（未指定按 cosine，
// 与集合索引度量一致——索引期固定）；分数按度量换算为统一相似度语义
// （L2 距离 → 1/(1+d)，其余原生相似度直接透传）；MinScore 在统一分数空间
// 客户端过滤。检索参数固定 AUTOINDEX（本模块集合一律以 AUTOINDEX 建索引），
// NumCandidates 不透传（Milvus 无对应参数）。租户不匹配的命中被剔除
// （纵深防御：服务端表达式已限定租户）。
func (r *Repository[DTO, ENTITY]) SearchByVector(ctx context.Context, q *vector.Query) (*vector.Result[DTO], error) {
	if r.client == nil || r.client.cli == nil {
		return nil, ErrClientNotInitialized
	}
	if r.collection == "" {
		return nil, ErrInvalidRequest
	}
	if err := q.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidVectorQuery, err)
	}

	specs, err := specsOf[ENTITY]()
	if err != nil {
		return nil, err
	}
	vectorField, err := resolveVectorFieldSpecs(specs, q.Field)
	if err != nil {
		return nil, err
	}
	metric, ok := milvusMetrics[q.Metric]
	if !ok {
		return nil, fmt.Errorf("%w: unsupported vector metric %q", ErrInvalidVectorQuery, q.Metric)
	}

	var expr string
	if s, isStr := q.Filter.(string); isStr {
		expr = s
	}
	expr, err = InjectTenantFilterIntoExpr[ENTITY](ctx, expr)
	if err != nil {
		return nil, err
	}

	sp, err := entity.NewIndexAUTOINDEXSearchParam(1)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrVectorSearchFailed, err)
	}

	results, err := r.client.cli.Search(
		ctx, r.collection, []string{}, expr, nonVectorSpecNames(specs),
		[]entity.Vector{entity.FloatVector(q.Vector)}, vectorField, metric, q.TopK, sp,
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("milvus vector search failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrVectorSearchFailed, err)
	}

	res := &vector.Result[DTO]{
		Hits:  make([]vector.Hit[DTO], 0, len(results)),
		Total: 0,
	}
	for _, result := range results {
		if result.Err != nil {
			log.Error(context.Background(), fmt.Sprintf("milvus search partial error: %v", result.Err))
			continue
		}
		n := result.Fields.Len()
		for i := 0; i < n; i++ {
			// 纵深防御：服务端表达式已注入租户，客户端再校验一次 tenant_id 列。
			if verr := checkTenantColumn[ENTITY](ctx, result.Fields, i); verr != nil {
				if windErrors.Is(verr, errTenantMismatch) {
					continue
				}
				return nil, verr
			}
			if i >= len(result.Scores) {
				continue
			}
			score := milvusScoreToScore(q.Metric, result.Scores[i])
			if q.MinScore > 0 && score < q.MinScore {
				continue
			}
			var ent ENTITY
			// 主键列在 IDs，其余输出列在 Fields，合并后统一还原。
			cols := make([]entity.Column, 0, len(result.Fields)+1)
			cols = append(cols, result.Fields...)
			cols = append(cols, result.IDs)
			if err = entityFromColumns(cols, i, &ent); err != nil {
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
	}
	res.Total = int64(len(res.Hits))
	return res, nil
}
