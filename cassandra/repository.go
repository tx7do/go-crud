package cassandra

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/gocql/gocql"

	"github.com/tx7do/go-utils/mapper"
	"github.com/tx7do/go-wind/log"

	"github.com/tx7do/go-crud/viewer"
)

// ─────────────────────────────────────────────────────────────────────────────
// Cassandra 仓库（泛型）。
//
// 本模块按行语义使用 Cassandra：表须为简单主键（单一分区键、不含
// clustering columns），一行 = 一个分区键实例，列 = 属性。仓库面：
//   - Create / BatchCreate：INSERT（CQL INSERT 天然为 Upsert——同主键覆盖）
//   - Get / GetByUUID：按主键直取（客户端校验行租户）
//   - Query / Count / Exists：按原生 WHERE 片段（tenant 谓词注入）
//   - DeleteByIDs / DeleteByUUIDs：主键先按租户预过滤再删
//   - DeleteByWhere：CQL 的 DELETE 不接受主键外条件——以同一（已注入租户）
//     条件先查出主键，再按主键删除并回报计数
//
// 不提供的面（契约边界，详见模块 README）：
//   - 向量检索：Cassandra 5 的 vector 类型与 ANN 不在本模块适配面；
//   - 分页：调用方经 Query 片段自行组合；Rows.PageState 已在 Client 层透出；
//   - DDL：keyspace/表/索引由调用方经 Client.Exec 表达（README 有模板）。
//
// 租户隔离（嵌入 cassandra/mixin.TenantID 的实体自动启用）：
//   - 写入：租户业务视图下强制覆盖 tenant_id 列（EnforceOnScopedInstance）；
//   - 可注入 WHERE 的路径（Query/Count/Exists/Delete*）：服务端注入
//     tenant_id = ? 谓词（InjectTenantWhere，表须建 tenant_id 二级索引
//     或置 AllowFiltering）；
//   - 主键直取路径（Get/GetByUUID）与删除前预过滤：客户端按行校验/
//     过滤租户（verifyTenantOnRow，不匹配与不存在同构，防存在性泄露）。
// ─────────────────────────────────────────────────────────────────────────────

// tablePattern 表名/键空间限定名白名单（裸表名或 keyspace.table；名称会
// 拼进语句文本，一律先校验）。
var tablePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$`)

// Query 原生查询条件：CQL WHERE 片段 + 位置绑定参数。
// 片段中的字面量一律经 ? 占位符传递（如 "age > ?" 配 Args{18}），
// 不做文本拼接。AllowFiltering 置位时语句追加 ALLOW FILTERING
// （非主键列过滤须建二级索引或允许过滤，二选一）。
type Query struct {
	Where          string
	Args           []any
	AllowFiltering bool
}

// Repository Cassandra 版仓库（泛型）
type Repository[DTO any, ENTITY any] struct {
	mapper *mapper.CopierMapper[DTO, ENTITY]

	exec  sessionExecutor
	table string
}

// NewRepository 创建 Cassandra 仓库实例。
// table 为表名或 "keyspace.table" 限定名（会话已绑定键空间时用裸表名）。
func NewRepository[DTO any, ENTITY any](client *Client, table string, mapper *mapper.CopierMapper[DTO, ENTITY], logger log.Logger) *Repository[DTO, ENTITY] {
	if logger != nil {
		log.SetLogger(logger)
	}
	return newRepository[DTO, ENTITY](client, table, mapper)
}

// newRepository 仓库构造内部入口（exec 可为测试替身）。
func newRepository[DTO any, ENTITY any](exec sessionExecutor, table string, mapper *mapper.CopierMapper[DTO, ENTITY]) *Repository[DTO, ENTITY] {
	return &Repository[DTO, ENTITY]{
		exec:   exec,
		table:  table,
		mapper: mapper,
	}
}

// ready 执行器与表名守卫（表名/键空间限定名经白名单校验——名称会拼进
// 语句文本）。
func (r *Repository[DTO, ENTITY]) ready() error {
	if r.exec == nil {
		return ErrClientNotInitialized
	}
	if r.table == "" || !tablePattern.MatchString(r.table) {
		return ErrInvalidRequest
	}
	return nil
}

// pkSpec 主键通道字段描述（不存在报错）。
func pkSpec(ent any) (columnSpec, error) {
	specs, err := collectSpecsOf(ent)
	if err != nil {
		return columnSpec{}, err
	}
	for _, spec := range specs {
		if spec.isPK {
			return spec, nil
		}
	}
	return columnSpec{}, fmt.Errorf("%w: entity has no id/uuid primary key field", ErrInvalidPointID)
}

// Create 插入一行（CQL INSERT 天然 Upsert：同主键覆盖）。
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

	cols, args, err := columnsAndArgsFromEntity(ent)
	if err != nil {
		return nil, err
	}
	pk, err := pkSpec(ent)
	if err != nil {
		return nil, err
	}
	pkv := pkValueFromEntity(ent)
	if pkv == nil {
		return nil, fmt.Errorf("%w: entity carries an empty primary key", ErrInvalidPointID)
	}

	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(cols)+1), ", ")
	stmt := fmt.Sprintf("INSERT INTO %s (%s, %s) VALUES (%s)", r.table, pk.name, strings.Join(cols, ", "), placeholders)
	if err := r.exec.Exec(ctx, stmt, append([]any{pkv}, args...)...); err != nil {
		log.Error(context.Background(), fmt.Sprintf("cassandra insert failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrInsertFailed, err)
	}
	return r.mapper.ToDTO(ent), nil
}

// BatchCreate 批量插入（单次 batch；逐实体强制租户）。
func (r *Repository[DTO, ENTITY]) BatchCreate(ctx context.Context, dtos []*DTO) ([]*DTO, error) {
	if err := r.ready(); err != nil {
		return nil, err
	}
	if len(dtos) == 0 {
		return nil, nil
	}

	ents := make([]*ENTITY, 0, len(dtos))
	stmts := make([]string, 0, len(dtos))
	argsList := make([][]any, 0, len(dtos))
	for _, d := range dtos {
		if d == nil {
			continue
		}
		e := r.mapper.ToEntity(d)
		// 租户强制：每个实体在租户业务视图下强制覆盖 tenant_id。
		if err := viewer.EnforceOnScopedInstance(ctx, e); err != nil {
			return nil, err
		}
		cols, args, err := columnsAndArgsFromEntity(e)
		if err != nil {
			return nil, err
		}
		pk, err := pkSpec(e)
		if err != nil {
			return nil, err
		}
		pkv := pkValueFromEntity(e)
		if pkv == nil {
			return nil, fmt.Errorf("%w: entity carries an empty primary key", ErrInvalidPointID)
		}
		placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(cols)+1), ", ")
		stmts = append(stmts, fmt.Sprintf("INSERT INTO %s (%s, %s) VALUES (%s)", r.table, pk.name, strings.Join(cols, ", "), placeholders))
		argsList = append(argsList, append([]any{pkv}, args...))
		ents = append(ents, e)
	}
	if len(ents) == 0 {
		return nil, nil
	}

	if err := r.exec.Batch(ctx, gocql.LoggedBatch, stmts, argsList); err != nil {
		log.Error(context.Background(), fmt.Sprintf("cassandra batch insert failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrInsertFailed, err)
	}

	out := make([]*DTO, 0, len(ents))
	for _, e := range ents {
		out = append(out, r.mapper.ToDTO(e))
	}
	return out, nil
}

// Get 按数值主键直取单行。
// 主键直取路径无服务端租户过滤（CQL 主键外 WHERE 需索引），行租户在
// 客户端校验（不匹配与不存在同构，防存在性泄露）。
func (r *Repository[DTO, ENTITY]) Get(ctx context.Context, id uint64) (*DTO, error) {
	return r.getByPK(ctx, any(int64(id)), "")
}

// GetByUUID 按字符串主键（text/uuid 列）直取单行（租户语义同 Get）。
func (r *Repository[DTO, ENTITY]) GetByUUID(ctx context.Context, uuid string) (*DTO, error) {
	if uuid == "" {
		return nil, ErrInvalidRequest
	}
	return r.getByPK(ctx, nil, uuid)
}

func (r *Repository[DTO, ENTITY]) getByPK(ctx context.Context, pkv any, strID string) (*DTO, error) {
	if err := r.ready(); err != nil {
		return nil, err
	}

	var e ENTITY
	specs, err := collectSpecsOf(&e)
	if err != nil {
		return nil, err
	}
	pk, err := pkSpec(&e)
	if err != nil {
		return nil, err
	}

	switch pk.kind {
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if strID != "" {
			return nil, fmt.Errorf("%w: primary key column %q is numeric", ErrInvalidRequest, pk.name)
		}
	case reflect.String:
		if strID == "" {
			return nil, fmt.Errorf("%w: primary key column %q is textual", ErrInvalidRequest, pk.name)
		}
		pkv = strID
	default:
		return nil, fmt.Errorf("%w: primary key field %q must be an integer or string", ErrInvalidPointID, pk.name)
	}

	rows, err := r.exec.Select(ctx,
		fmt.Sprintf("SELECT %s FROM %s WHERE %s = ?", columnListOrError(specs), r.table, pk.name),
		pkv,
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("cassandra get failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrQueryFailed, err)
	}
	if len(rows) == 0 {
		return nil, ErrPointNotFound
	}
	row := rows[0]

	// 主键直取路径的租户校验在客户端做（不匹配与不存在同构）。
	if verr := verifyTenantOnRow[ENTITY](ctx, row); verr != nil {
		if errors.Is(verr, errTenantMismatch) {
			return nil, ErrPointNotFound
		}
		return nil, verr
	}

	var ent ENTITY
	if err = entityFromRow(row, &ent); err != nil {
		return nil, err
	}
	return r.mapper.ToDTO(&ent), nil
}

// columnListOrError 全列清单（供 SELECT）。
func columnListOrError(specs []columnSpec) string {
	names := make([]string, 0, len(specs))
	for _, spec := range specs {
		names = append(names, spec.name)
	}
	return strings.Join(names, ", ")
}

// Query 按 WHERE 片段列出各行（tenant-scoped 实体注入租户谓词）。
func (r *Repository[DTO, ENTITY]) Query(ctx context.Context, q *Query) ([]*DTO, error) {
	if err := r.ready(); err != nil {
		return nil, err
	}

	q = normalizeQuery(q)
	where, args, err := InjectTenantWhere[ENTITY](ctx, q.Where, q.Args)
	if err != nil {
		return nil, err
	}

	var e ENTITY
	specs, err := collectSpecsOf(&e)
	if err != nil {
		return nil, err
	}

	stmt := fmt.Sprintf("SELECT %s FROM %s", columnListOrError(specs), r.table)
	if where != "" {
		stmt += " WHERE " + where
	}
	if q.AllowFiltering {
		stmt += " ALLOW FILTERING"
	}

	rows, err := r.exec.Select(ctx, stmt, args...)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("cassandra query failed: %v", err))
		return nil, fmt.Errorf("%w: %v", ErrQueryFailed, err)
	}

	out := make([]*DTO, 0, len(rows))
	for _, row := range rows {
		// 纵深防御：服务端谓词已限定租户，客户端再校验一次；不匹配静默剔除。
		if verr := verifyTenantOnRow[ENTITY](ctx, row); verr != nil {
			if errors.Is(verr, errTenantMismatch) {
				continue
			}
			return nil, verr
		}
		var ent ENTITY
		if err = entityFromRow(row, &ent); err != nil {
			return nil, err
		}
		out = append(out, r.mapper.ToDTO(&ent))
	}
	return out, nil
}

// Count 按 WHERE 片段计数（tenant-scoped 实体注入租户谓词；
// 条件为空时计全表——租户视图下即为该租户的全部行）。
func (r *Repository[DTO, ENTITY]) Count(ctx context.Context, q *Query) (int64, error) {
	if err := r.ready(); err != nil {
		return 0, err
	}

	q = normalizeQuery(q)
	where, args, err := InjectTenantWhere[ENTITY](ctx, q.Where, q.Args)
	if err != nil {
		return 0, err
	}

	stmt := fmt.Sprintf("SELECT count(*) AS row_count FROM %s", r.table)
	if where != "" {
		stmt += " WHERE " + where
	}
	if q.AllowFiltering {
		stmt += " ALLOW FILTERING"
	}

	rows, err := r.exec.Select(ctx, stmt, args...)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("cassandra count failed: %v", err))
		return 0, fmt.Errorf("%w: %v", ErrCountFailed, err)
	}
	if len(rows) == 0 {
		return 0, nil
	}
	return jsonNumberToInt64(rows[0]["row_count"]), nil
}

// Exists 判断是否存在符合 WHERE 片段的行（tenant-scoped 实体注入租户谓词）。
func (r *Repository[DTO, ENTITY]) Exists(ctx context.Context, q *Query) (bool, error) {
	n, err := r.Count(ctx, q)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// DeleteByIDs 按数值主键删除（先按租户预过滤主键，再按主键删除；
// 预过滤与删除非原子，语义同 milvus 模块的先过滤后删）。
func (r *Repository[DTO, ENTITY]) DeleteByIDs(ctx context.Context, ids []uint64) (int64, error) {
	pks := make([]any, 0, len(ids))
	for _, id := range ids {
		pks = append(pks, int64(id))
	}
	return deleteByPKValues(ctx, r, pks)
}

// DeleteByUUIDs 按字符串主键删除（租户语义同 DeleteByIDs）。
func (r *Repository[DTO, ENTITY]) DeleteByUUIDs(ctx context.Context, uuids []string) (int64, error) {
	pks := make([]any, 0, len(uuids))
	for _, u := range uuids {
		pks = append(pks, u)
	}
	return deleteByPKValues(ctx, r, pks)
}

// deleteByPKValues 主键租户预过滤 → 按过滤后的主键删除（包级泛型函数：
// Go 的方法不能自带独立类型参数）。
func deleteByPKValues[DTO any, ENTITY any](ctx context.Context, r *Repository[DTO, ENTITY], pks []any) (int64, error) {
	if err := r.ready(); err != nil {
		return 0, err
	}
	if len(pks) == 0 {
		return 0, nil
	}

	var e ENTITY
	pk, err := pkSpec(&e)
	if err != nil {
		return 0, err
	}

	// 主键租户预过滤（服务端注入 tenant 谓词；表须建索引或允许过滤）。
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(pks)), ", ")
	where, args, err := InjectTenantWhere[ENTITY](ctx,
		fmt.Sprintf("%s IN (%s)", pk.name, placeholders), pks)
	if err != nil {
		return 0, err
	}
	rows, err := r.exec.Select(ctx,
		fmt.Sprintf("SELECT %s FROM %s WHERE %s ALLOW FILTERING", pk.name, r.table, where),
		args...)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("cassandra delete pre-filter failed: %v", err))
		return 0, fmt.Errorf("%w: %v", ErrQueryFailed, err)
	}
	if len(rows) == 0 {
		return 0, nil
	}

	deletable := make([]any, 0, len(rows))
	for _, row := range rows {
		if v, ok := rowColumnValue(row, pk.name); ok {
			deletable = append(deletable, v)
		}
	}
	if len(deletable) == 0 {
		return 0, nil
	}

	delPlaceholders := strings.TrimSuffix(strings.Repeat("?, ", len(deletable)), ", ")
	if err := r.exec.Exec(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)", r.table, pk.name, delPlaceholders),
		deletable...,
	); err != nil {
		log.Error(context.Background(), fmt.Sprintf("cassandra delete failed: %v", err))
		return 0, fmt.Errorf("%w: %v", ErrDeleteFailed, err)
	}
	return int64(len(deletable)), nil
}

// DeleteByWhere 按原生 WHERE 片段删除。
// CQL 的 DELETE 不接受主键外条件：以同一（已注入租户）条件先查出主键，
// 再按主键删除并回报计数（必须显式给出条件，防误删全表）。
func (r *Repository[DTO, ENTITY]) DeleteByWhere(ctx context.Context, q *Query) (int64, error) {
	if err := r.ready(); err != nil {
		return 0, err
	}
	q = normalizeQuery(q)
	if q.Where == "" {
		return 0, ErrInvalidRequest
	}

	var e ENTITY
	pk, err := pkSpec(&e)
	if err != nil {
		return 0, err
	}

	where, args, err := InjectTenantWhere[ENTITY](ctx, q.Where, q.Args)
	if err != nil {
		return 0, err
	}

	stmt := fmt.Sprintf("SELECT %s FROM %s WHERE %s", pk.name, r.table, where)
	if q.AllowFiltering {
		stmt += " ALLOW FILTERING"
	}
	rows, err := r.exec.Select(ctx, stmt, args...)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("cassandra delete pre-select failed: %v", err))
		return 0, fmt.Errorf("%w: %v", ErrQueryFailed, err)
	}
	if len(rows) == 0 {
		return 0, nil
	}

	deletable := make([]any, 0, len(rows))
	for _, row := range rows {
		if v, ok := rowColumnValue(row, pk.name); ok {
			deletable = append(deletable, v)
		}
	}
	if len(deletable) == 0 {
		return 0, nil
	}

	delPlaceholders := strings.TrimSuffix(strings.Repeat("?, ", len(deletable)), ", ")
	if err := r.exec.Exec(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)", r.table, pk.name, delPlaceholders),
		deletable...,
	); err != nil {
		log.Error(context.Background(), fmt.Sprintf("cassandra delete by where failed: %v", err))
		return 0, fmt.Errorf("%w: %v", ErrDeleteFailed, err)
	}
	return int64(len(deletable)), nil
}

// normalizeQuery nil 查询条件归一化为空条件。
func normalizeQuery(q *Query) *Query {
	if q == nil {
		return &Query{}
	}
	return q
}

// jsonNumberToInt64 计数列宽容转换（gocql count(*) 产出 int；替身可给出
// int64）。
func jsonNumberToInt64(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case int32:
		return int64(n)
	}
	return 0
}
