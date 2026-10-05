package neo4j

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"

	"github.com/tx7do/go-utils/mapper"
	"github.com/tx7do/go-wind/log"

	"github.com/tx7do/go-crud/viewer"
)

// ─────────────────────────────────────────────────────────────────────────────
// Neo4j 仓库（泛型）。
//
// 本模块把 label 当作行式存储的表使用：节点 = 行，属性 = 列，element id =
// 服务端生成的行身份（字符串）。仓库面：
//   - Create / BatchCreate：建节点（属性经 $props / $rows 参数整体写入，
//     element id 由 RETURN elementId(n) 回读到实体的 uuid 通道字段）
//   - GetByUUID：按 element id 取回（WHERE 合并租户谓词 + 客户端校验）
//   - Query / Count / Exists：按 WHERE 片段列出/计数（租户谓词注入）
//   - DeleteByUUIDs / DeleteByWhere：删节点（同一已注入条件先计数后删，
//     DETACH DELETE——行语义下关系非保留对象）
//
// 不提供的面（契约边界，详见模块 README）：
//   - 数值 ID 通道（Get/DeleteByIDs）：element id 是字符串，无数值身份；
//   - 关系与图遍历：仓库面为节点 CRUD，遍历/最短路径等图查询由调用方
//     直连 driver 的 Cypher 会话表达（要素不经本仓库映射）；
//   - 分页：调用方经 Query 的 WHERE 片段自行组合 SKIP/LIMIT。
//
// 租户隔离（嵌入 neo4j/mixin.TenantID 的实体自动启用）：
//   - 写入：租户业务视图下强制覆盖 tenant_id 属性（EnforceOnScopedInstance）；
//   - 读取/删除：WHERE 服务端注入 tenant_id 匹配谓词
//     （InjectTenantPredicate）；按 element id 直取的路径叠加客户端
//     属性租户校验（verifyTenantOnNode，纵深防御）。
// ─────────────────────────────────────────────────────────────────────────────

// Query 原生查询条件：Cypher WHERE 片段 + 命名参数。
// 片段中的字面量一律经 $参数 传递（如 "n.age > $age" 配 Params{"age": ...}），
// 不做文本拼接。
type Query struct {
	Where  string
	Params map[string]any
}

// Repository Neo4j 版仓库（泛型）。
type Repository[DTO any, ENTITY any] struct {
	mapper *mapper.CopierMapper[DTO, ENTITY]

	client *Client
	label  string
}

// NewRepository 创建 Neo4j 仓库实例。
func NewRepository[DTO any, ENTITY any](client *Client, label string, mapper *mapper.CopierMapper[DTO, ENTITY], logger log.Logger) *Repository[DTO, ENTITY] {
	if logger != nil {
		log.SetLogger(logger)
	}
	return &Repository[DTO, ENTITY]{
		client: client,
		label:  label,
		mapper: mapper,
	}
}

// ready 客户端与标签守卫（标签为空或含反引号——后者可经 Cypher 标签
// 拼接注入，一并拒绝）。
func (r *Repository[DTO, ENTITY]) ready() error {
	if r.client == nil || r.client.drv == nil {
		return ErrClientNotInitialized
	}
	if r.label == "" || strings.ContainsRune(r.label, '`') {
		return ErrInvalidRequest
	}
	return nil
}

// matchHead 拟合 MATCH 子句（标签经反引号包裹，ready 已排除反引号注入）。
func (r *Repository[DTO, ENTITY]) matchHead() string {
	return "MATCH (n:`" + r.label + "`)"
}

// whereClause 拟合 WHERE 子句（空条件即无条件全标签）。
func whereClause(where string) string {
	if where == "" {
		return ""
	}
	return " WHERE " + where
}

// normalizeQuery nil 查询条件归一化为空条件。
func normalizeQuery(q *Query) *Query {
	if q == nil {
		return &Query{}
	}
	return q
}

// run 执行自动提交语句并取回全部记录（会话即用即弃）。
func (r *Repository[DTO, ENTITY]) run(ctx context.Context, cypher string, params map[string]any) ([]*neo4j.Record, error) {
	sess := r.client.drv.NewSession(ctx, neo4j.SessionConfig{})
	defer func() { _ = sess.Close(ctx) }()
	res, err := sess.Run(ctx, cypher, params)
	if err != nil {
		return nil, err
	}
	return res.Collect(ctx)
}

// recordNode 从记录中取出 "n" 列的节点（记录缺失或类型不符返回 false）。
func recordNode(rec *neo4j.Record) (neo4j.Node, bool) {
	if rec == nil {
		return neo4j.Node{}, false
	}
	v, ok := rec.Get("n")
	if !ok {
		return neo4j.Node{}, false
	}
	node, ok := v.(neo4j.Node)
	return node, ok
}

// countWhere 按给定（已含租户谓词的）WHERE 计数。
func (r *Repository[DTO, ENTITY]) countWhere(ctx context.Context, where string, params map[string]any) (int64, error) {
	recs, err := r.run(ctx, r.matchHead()+whereClause(where)+" RETURN count(n)", params)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("neo4j count failed: %v", err))
		return 0, fmt.Errorf("%w: %v", ErrCountFailed, err)
	}
	if len(recs) == 0 || recs[0] == nil {
		return 0, nil
	}
	if v, ok := recs[0].Get("count(n)"); ok {
		if n, ok := v.(int64); ok {
			return n, nil
		}
	}
	return 0, nil
}

// Create 创建节点并回传。
// 属性经 $props 参数整体写入；element id 由 RETURN elementId(n) 回读到
// 实体的 uuid 通道字段（身份由服务端分配，非调用方可写）。
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

	props := structToProperties(ent)

	recs, err := r.run(ctx,
		"CREATE (n:`"+r.label+"`) SET n = $props RETURN elementId(n)",
		map[string]any{"props": props},
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("neo4j create failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrInsertFailed, err)
	}

	setElementID(ent, recordElementID(recs, 0))
	return r.mapper.ToDTO(ent), nil
}

// BatchCreate 批量创建节点（UNWIND 单事务批量路径）。
// 逐实体强制租户；element id 按 UNWIND 行序回填（单线程自动提交事务内
// 逐行创建保序）。
func (r *Repository[DTO, ENTITY]) BatchCreate(ctx context.Context, dtos []*DTO) ([]*DTO, error) {
	if err := r.ready(); err != nil {
		return nil, err
	}
	if len(dtos) == 0 {
		return nil, nil
	}

	ents := make([]*ENTITY, 0, len(dtos))
	rows := make([]any, 0, len(dtos))
	for _, d := range dtos {
		if d == nil {
			continue
		}
		e := r.mapper.ToEntity(d)
		// 租户强制：每个实体在租户业务视图下强制覆盖 tenant_id。
		if err := viewer.EnforceOnScopedInstance(ctx, e); err != nil {
			return nil, err
		}
		ents = append(ents, e)
		rows = append(rows, structToProperties(e))
	}
	if len(ents) == 0 {
		return nil, nil
	}

	recs, err := r.run(ctx,
		"UNWIND $rows AS row CREATE (n:`"+r.label+"`) SET n = row RETURN elementId(n)",
		map[string]any{"rows": rows},
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("neo4j batch create failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrInsertFailed, err)
	}

	out := make([]*DTO, 0, len(ents))
	for i, e := range ents {
		setElementID(e, recordElementID(recs, i))
		out = append(out, r.mapper.ToDTO(e))
	}
	return out, nil
}

// recordElementID 从回读记录中取第 idx 条的 element id（缺失或类型不符
// 返回空串）。
func recordElementID(recs []*neo4j.Record, idx int) string {
	if idx < 0 || idx >= len(recs) || recs[idx] == nil {
		return ""
	}
	v, ok := recs[idx].Get("elementId(n)")
	if !ok {
		return ""
	}
	id, ok := v.(string)
	if !ok {
		return ""
	}
	return id
}

// Get 按数值 ID 取回单节点：不支持。
// Neo4j 的节点身份是 element id（字符串），无数值通道；数值属性过滤
// 请用 Query。
func (r *Repository[DTO, ENTITY]) Get(ctx context.Context, id uint64) (*DTO, error) {
	return nil, ErrInvalidRequest
}

// GetByUUID 按 element id 取回单节点。
// element id 条件与租户谓词合并（按 ID 直取同样受租户限定）；本租户外
// 的节点与不存在同构（ErrPointNotFound，防存在性泄露）。
func (r *Repository[DTO, ENTITY]) GetByUUID(ctx context.Context, uuid string) (*DTO, error) {
	if err := r.ready(); err != nil {
		return nil, err
	}
	if uuid == "" {
		return nil, ErrInvalidRequest
	}

	// 服务端谓词注入：element id 条件与租户谓词 AND 合并。
	where, params, err := InjectTenantPredicate[ENTITY](ctx, "elementId(n) = $eid", map[string]any{"eid": uuid})
	if err != nil {
		return nil, err
	}

	recs, err := r.run(ctx, r.matchHead()+whereClause(where)+" RETURN n", params)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("neo4j get failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrQueryFailed, err)
	}
	if len(recs) == 0 {
		return nil, ErrPointNotFound
	}
	node, ok := recordNode(recs[0])
	if !ok {
		return nil, ErrPointNotFound
	}

	// 纵深防御：服务端谓词已限定租户，客户端再校验一次节点属性租户。
	// 不匹配与不存在对外同构。
	if verr := verifyTenantOnNode[ENTITY](ctx, node); verr != nil {
		if errors.Is(verr, errTenantMismatch) {
			return nil, ErrPointNotFound
		}
		return nil, verr
	}

	var ent ENTITY
	if err = propertiesToEntity(node, &ent); err != nil {
		return nil, err
	}
	return r.mapper.ToDTO(&ent), nil
}

// Query 按 WHERE 片段列出节点（tenant-scoped 实体注入租户谓词）。
// 返回的 DTO 携带节点属性与 element id（uuid 通道字段）。
func (r *Repository[DTO, ENTITY]) Query(ctx context.Context, q *Query) ([]*DTO, error) {
	if err := r.ready(); err != nil {
		return nil, err
	}

	q = normalizeQuery(q)
	where, params, err := InjectTenantPredicate[ENTITY](ctx, q.Where, q.Params)
	if err != nil {
		return nil, err
	}

	recs, err := r.run(ctx, r.matchHead()+whereClause(where)+" RETURN n", params)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("neo4j query failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrQueryFailed, err)
	}

	out := make([]*DTO, 0, len(recs))
	for _, rec := range recs {
		node, ok := recordNode(rec)
		if !ok {
			continue
		}
		// 纵深防御：服务端谓词已限定租户，客户端再校验一次；不匹配静默剔除。
		if verr := verifyTenantOnNode[ENTITY](ctx, node); verr != nil {
			if errors.Is(verr, errTenantMismatch) {
				continue
			}
			return nil, verr
		}
		var ent ENTITY
		if err = propertiesToEntity(node, &ent); err != nil {
			return nil, err
		}
		out = append(out, r.mapper.ToDTO(&ent))
	}
	return out, nil
}

// Count 按 WHERE 片段计数（tenant-scoped 实体注入租户谓词；
// 条件为空时计全标签——租户视图下即为该租户的全部节点）。
func (r *Repository[DTO, ENTITY]) Count(ctx context.Context, q *Query) (int64, error) {
	if err := r.ready(); err != nil {
		return 0, err
	}

	q = normalizeQuery(q)
	where, params, err := InjectTenantPredicate[ENTITY](ctx, q.Where, q.Params)
	if err != nil {
		return 0, err
	}
	return r.countWhere(ctx, where, params)
}

// Exists 判断是否存在符合 WHERE 片段的节点（tenant-scoped 实体注入租户谓词）。
func (r *Repository[DTO, ENTITY]) Exists(ctx context.Context, q *Query) (bool, error) {
	n, err := r.Count(ctx, q)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// DeleteByIDs 按数值 ID 删除：不支持（element id 为字符串，见 Get）。
func (r *Repository[DTO, ENTITY]) DeleteByIDs(ctx context.Context, ids []uint64) (int64, error) {
	return 0, ErrInvalidRequest
}

// DeleteByUUIDs 按 element id 删除节点。
// element id 条件与租户谓词合并；删除计数按同一（已注入）条件预计数，
// 二者同条件保证不越权删。DETACH DELETE：行语义下节点的关系非保留对象。
func (r *Repository[DTO, ENTITY]) DeleteByUUIDs(ctx context.Context, uuids []string) (int64, error) {
	if err := r.ready(); err != nil {
		return 0, err
	}
	if len(uuids) == 0 {
		return 0, nil
	}

	where, params, err := InjectTenantPredicate[ENTITY](ctx, "elementId(n) IN $eids", map[string]any{"eids": uuids})
	if err != nil {
		return 0, err
	}
	total, err := r.countWhere(ctx, where, params)
	if err != nil {
		return 0, err
	}
	if total == 0 {
		return 0, nil
	}

	if _, err = r.run(ctx, r.matchHead()+whereClause(where)+" DETACH DELETE n", params); err != nil {
		log.Error(context.Background(), fmt.Sprintf("neo4j delete failed: %v", err))
		return 0, fmt.Errorf("%w: %v", ErrDeleteFailed, err)
	}
	return total, nil
}

// DeleteByWhere 按原生 WHERE 片段删除节点。
// 必须显式给出条件（防误删全标签）；tenant-scoped 实体服务端注入租户
// 谓词；计数按同一（已注入）条件预计数（同条件保证不越权删）。
func (r *Repository[DTO, ENTITY]) DeleteByWhere(ctx context.Context, q *Query) (int64, error) {
	if err := r.ready(); err != nil {
		return 0, err
	}
	q = normalizeQuery(q)
	if q.Where == "" {
		return 0, ErrInvalidRequest
	}

	where, params, err := InjectTenantPredicate[ENTITY](ctx, q.Where, q.Params)
	if err != nil {
		return 0, err
	}
	total, err := r.countWhere(ctx, where, params)
	if err != nil {
		return 0, err
	}
	if total == 0 {
		return 0, nil
	}

	if _, err = r.run(ctx, r.matchHead()+whereClause(where)+" DETACH DELETE n", params); err != nil {
		log.Error(context.Background(), fmt.Sprintf("neo4j delete by where failed: %v", err))
		return 0, fmt.Errorf("%w: %v", ErrDeleteFailed, err)
	}
	return total, nil
}
