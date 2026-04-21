package doris

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/go-utils/mapper"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/doris/field"
	"github.com/tx7do/go-crud/doris/filter"
	paging "github.com/tx7do/go-crud/doris/pagination"
	"github.com/tx7do/go-crud/doris/query"
	"github.com/tx7do/go-crud/doris/sorting"
	paginationFilter "github.com/tx7do/go-crud/pagination/filter"
	paginationSorting "github.com/tx7do/go-crud/pagination/sorting"
)

// PagingResult 是通用的分页返回结构，包含 items 和 total 字段
type PagingResult[E any] struct {
	Items []*E   `json:"items"`
	Total uint64 `json:"total"`
}

// Repository GORM 仓库，包含常用的 CRUD 方法
type Repository[DTO any, ENTITY any] struct {
	mapper *mapper.CopierMapper[DTO, ENTITY]

	offsetPaginator *paging.OffsetPaginator
	pagePaginator   *paging.PagePaginator
	tokenPaginator  *paging.TokenPaginator

	structuredFilter *filter.StructuredFilter

	structuredSorting      *sorting.StructuredSorting
	orderByStringConverter *paginationSorting.OrderByStringConverter

	fieldSelector *field.Selector

	client *Client
	log    *log.Helper

	table string
}

func NewRepository[DTO any, ENTITY any](
	client *Client,
	mapper *mapper.CopierMapper[DTO, ENTITY],
	table string,
	log *log.Helper,
) *Repository[DTO, ENTITY] {
	return &Repository[DTO, ENTITY]{
		client: client,
		mapper: mapper,

		table: table,
		log:   log,

		offsetPaginator: paging.NewOffsetPaginator(),
		pagePaginator:   paging.NewPagePaginator(),
		tokenPaginator:  paging.NewTokenPaginator(),

		structuredFilter: filter.NewStructuredFilter(),

		structuredSorting:      sorting.NewStructuredSorting(),
		orderByStringConverter: paginationSorting.NewOrderByStringConverter(),

		fieldSelector: field.NewFieldSelector(),
	}
}

// Count 使用 Doris client 计算符合 baseWhere 的记录数
// baseWhere: 可以包含 "WHERE ..." 前缀或只写条件表达式（函数会自动拼接）
// 示例调用： total, err := q.Count(ctx, "id = ?", id)
// 支持当只传入一个切片参数时自动展开： q.Count(ctx, "id IN (?)", []int{1,2,3})
func (r *Repository[DTO, ENTITY]) Count(ctx context.Context, baseWhere string, whereArgs ...any) (uint64, error) {
	if r.client == nil {
		return 0, errors.New("doris client is nil")
	}
	if r.table == "" {
		return 0, errors.New("table is empty")
	}

	// 展开单个切片参数为独立参数
	if len(whereArgs) == 1 {
		v := reflect.ValueOf(whereArgs[0])
		if v.IsValid() && v.Kind() == reflect.Slice {
			expanded := make([]any, v.Len())
			for i := 0; i < v.Len(); i++ {
				expanded[i] = v.Index(i).Interface()
			}
			whereArgs = expanded
		}
	}

	aSql := "SELECT COUNT(1) FROM " + r.table
	bw := strings.TrimSpace(baseWhere)
	if bw != "" {
		// 如果用户传入的不包含 WHERE 前缀，自动添加
		if !strings.HasPrefix(strings.ToUpper(bw), "WHERE") {
			aSql += " WHERE " + bw
		} else {
			aSql += " " + bw
		}
	}

	// 使用底层连接执行查询
	var cnt uint64
	err := r.client.GetContext(ctx, &cnt, aSql, whereArgs...)
	if err != nil {
		r.log.Errorf("doris count query failed: %v", err)
		return 0, errors.New("count query failed")
	}

	// 没有行时返回 0
	return cnt, nil
}

// ListWithPaging 使用 PagingRequest 查询列表
func (r *Repository[DTO, ENTITY]) ListWithPaging(ctx context.Context, req *paginationV1.PagingRequest) (*PagingResult[DTO], error) {
	if r.client == nil {
		return nil, errors.New("doris client is nil")
	}
	if r.table == "" {
		return nil, errors.New("table is empty")
	}

	queryBuilder := query.NewQueryBuilder(r.table, r.log)

	var err error

	// filters
	var filterExpr *paginationV1.FilterExpr
	filterExpr, err = paginationFilter.ConvertFilterByPagingRequest(req)
	if err != nil {
		log.Errorf("convert filter string to filter expr failed: %s", err.Error())
		return nil, err
	}
	req.FilteringType = &paginationV1.PagingRequest_FilterExpr{FilterExpr: filterExpr}

	_, err = r.structuredFilter.BuildSelectors(queryBuilder, req.GetFilterExpr())
	if err != nil {
		log.Errorf("build structured filter selectors failed: %s", err.Error())
	}

	// 计数
	aSql, args := queryBuilder.BuildWhereParam()
	total, err := r.Count(ctx, aSql, args...)
	if err != nil {
		r.log.Errorf("count query failed: %v", err)
		return nil, err
	}

	// select fields
	if req.FieldMask != nil && len(req.GetFieldMask().Paths) > 0 {
		_, err = r.fieldSelector.BuildSelector(queryBuilder, req.GetFieldMask().GetPaths())
		if err != nil {
			log.Errorf("build field select selector failed: %s", err.Error())
		}
	}

	// order by
	if len(req.GetSorting()) > 0 {
		_ = r.structuredSorting.BuildOrderClause(queryBuilder, req.GetSorting())
	} else if len(req.GetOrderBy()) > 0 {
		var orderBy []*paginationV1.Sorting
		orderBy, err = r.orderByStringConverter.Convert(req.GetOrderBy())
		if err != nil {
			log.Errorf("convert order by string to sorting failed: %s", err.Error())
			return nil, err
		}
		_ = r.structuredSorting.BuildOrderClause(queryBuilder, orderBy)
	}

	// pagination
	if !req.GetNoPaging() {
		if req.Page != nil && req.PageSize != nil {
			_ = r.pagePaginator.BuildClause(queryBuilder, int(req.GetPage()), int(req.GetPageSize()))
		} else if req.Offset != nil && req.Limit != nil {
			_ = r.offsetPaginator.BuildClause(queryBuilder, int(req.GetOffset()), int(req.GetLimit()))
		} else if req.Token != nil && req.Offset != nil {
			_ = r.tokenPaginator.BuildClause(queryBuilder, req.GetToken(), int(req.GetOffset()))
		}
	}

	// 使用 client.Query（creator + results slice）
	var rawResults []any
	creator := func() any {
		var e ENTITY
		return &e
	}
	aSql, args = queryBuilder.Build()
	if err = r.client.Query(ctx, creator, &rawResults, aSql, args...); err != nil {
		r.log.Errorf("list query failed: %v", err)
		return nil, errors.New("list query failed")
	}

	// 转换为 DTOs
	dtos := make([]*DTO, 0, len(rawResults))
	for _, res := range rawResults {
		if ptr, ok := res.(*ENTITY); ok {
			dtos = append(dtos, r.mapper.ToDTO(ptr))
		}
	}

	res := &PagingResult[DTO]{
		Items: dtos,
		Total: uint64(total),
	}
	return res, nil
}

// ListWithPagination 使用 PaginationRequest 查询列表
func (r *Repository[DTO, ENTITY]) ListWithPagination(ctx context.Context, req *paginationV1.PaginationRequest) (*PagingResult[DTO], error) {
	if r.client == nil {
		return nil, errors.New("doris client is nil")
	}
	if r.table == "" {
		return nil, errors.New("table is empty")
	}

	queryBuilder := query.NewQueryBuilder(r.table, r.log)

	var err error

	// filters
	var filterExpr *paginationV1.FilterExpr
	filterExpr, err = paginationFilter.ConvertFilterByPaginationRequest(req)
	if err != nil {
		log.Errorf("convert filter string to filter expr failed: %s", err.Error())
		return nil, err
	}
	req.FilteringType = &paginationV1.PaginationRequest_FilterExpr{FilterExpr: filterExpr}

	_, err = r.structuredFilter.BuildSelectors(queryBuilder, req.GetFilterExpr())
	if err != nil {
		log.Errorf("build structured filter selectors failed: %s", err.Error())
	}

	// 计数
	aSql, args := queryBuilder.BuildWhereParam()
	total, err := r.Count(ctx, aSql, args...)
	if err != nil {
		r.log.Errorf("count query failed: %v", err)
		return nil, err
	}

	// select fields
	if req.FieldMask != nil && len(req.GetFieldMask().Paths) > 0 {
		_, err = r.fieldSelector.BuildSelector(queryBuilder, req.GetFieldMask().GetPaths())
		if err != nil {
			log.Errorf("build field select selector failed: %s", err.Error())
		}
	}

	// order by
	if len(req.GetSorting()) > 0 {
		_ = r.structuredSorting.BuildOrderClause(queryBuilder, req.GetSorting())
	} else if len(req.GetOrderBy()) > 0 {
		var orderBy []*paginationV1.Sorting
		orderBy, err = r.orderByStringConverter.Convert(req.GetOrderBy())
		if err != nil {
			log.Errorf("convert order by string to sorting failed: %s", err.Error())
			return nil, err
		}
		_ = r.structuredSorting.BuildOrderClause(queryBuilder, orderBy)
	}

	// pagination
	switch req.GetPaginationType().(type) {
	case *paginationV1.PaginationRequest_OffsetBased:
		_ = r.offsetPaginator.BuildClause(queryBuilder, int(req.GetOffsetBased().GetOffset()), int(req.GetOffsetBased().GetLimit()))
	case *paginationV1.PaginationRequest_PageBased:
		_ = r.pagePaginator.BuildClause(queryBuilder, int(req.GetPageBased().GetPage()), int(req.GetPageBased().GetPageSize()))
	case *paginationV1.PaginationRequest_TokenBased:
		_ = r.tokenPaginator.BuildClause(queryBuilder, req.GetTokenBased().GetToken(), int(req.GetTokenBased().GetPageSize()))
	}

	// 使用 client.Query（creator + results slice）
	var rawResults []any
	creator := func() any {
		var e ENTITY
		return &e
	}
	aSql, args = queryBuilder.Build()
	if err = r.client.Query(ctx, creator, &rawResults, aSql, args...); err != nil {
		r.log.Errorf("list query failed: %v", err)
		return nil, errors.New("list query failed")
	}

	// 转换为 DTOs
	dtos := make([]*DTO, 0, len(rawResults))
	for _, res := range rawResults {
		if ptr, ok := res.(*ENTITY); ok {
			dtos = append(dtos, r.mapper.ToDTO(ptr))
		}
	}

	res := &PagingResult[DTO]{
		Items: dtos,
		Total: uint64(total),
	}
	return res, nil
}

// Get 根据查询条件获取单条记录
func (r *Repository[DTO, ENTITY]) Get(ctx context.Context, qb *query.Builder, viewMask *fieldmaskpb.FieldMask) (*DTO, error) {
	if r.client == nil {
		return nil, errors.New("doris client is nil")
	}
	if r.table == "" {
		return nil, errors.New("table is empty")
	}

	// 规范 viewMask 路径
	field.NormalizeFieldMaskPaths(viewMask)

	// 构建查询
	if qb == nil {
		qb = query.NewQueryBuilder(r.table, r.log)
	} else {
		qb.WithTableName(r.table).WithLogger(r.log)
	}

	// 如果提供了 viewMask，则构建 select 子句（日志记录错误但继续）
	if viewMask != nil && len(viewMask.Paths) > 0 {
		if _, err := r.fieldSelector.BuildSelector(qb, viewMask.GetPaths()); err != nil {
			r.log.Errorf("build field select selector failed: %s", err.Error())
		}
	}

	// 获取 SQL 与参数，并确保只取一条记录
	sqlStr, args := qb.Build()
	sqlStr = strings.TrimSpace(sqlStr)
	upper := strings.ToUpper(sqlStr)
	if !strings.Contains(upper, "LIMIT") {
		sqlStr = sqlStr + " LIMIT 1"
	}

	// 执行查询并读取首条结果
	var rawResults []any
	creator := func() any {
		var e ENTITY
		return &e
	}
	if err := r.client.Query(ctx, creator, &rawResults, sqlStr, args...); err != nil {
		r.log.Errorf("get query failed: %v", err)
		return nil, errors.New("get query failed")
	}

	if len(rawResults) == 0 {
		return nil, nil
	}

	if ptr, ok := rawResults[0].(*ENTITY); ok {
		return r.mapper.ToDTO(ptr), nil
	}

	r.log.Errorf("unexpected result type")
	return nil, errors.New("unexpected result type")
}

// Only alias
func (r *Repository[DTO, ENTITY]) Only(ctx context.Context, qb *query.Builder, viewMask *fieldmaskpb.FieldMask) (*DTO, error) {
	return r.Get(ctx, qb, viewMask)
}

// Create 在数据库中创建一条记录，返回创建后的 DTO
func (r *Repository[DTO, ENTITY]) Create(ctx context.Context, dto *DTO, viewMask *fieldmaskpb.FieldMask) (*DTO, error) {
	if r.client == nil {
		return nil, errors.New("doris client is nil")
	}
	if r.table == "" {
		return nil, errors.New("table is empty")
	}
	if dto == nil {
		return nil, errors.New("dto is nil")
	}

	// 规范 viewMask 路径
	field.NormalizeFieldMaskPaths(viewMask)

	// DTO -> ENTITY
	ent := r.mapper.ToEntity(dto)

	// 通过反射收集列名与值（优先使用 struct tag: db -> ch -> json，否则使用小写字段名）
	v := reflect.ValueOf(ent)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return nil, errors.New("entity must be a struct or pointer to struct")
	}
	//t := v.Type()

	mask := map[string]bool{}
	if viewMask != nil {
		for _, p := range viewMask.Paths {
			mask[p] = true
		}
	}

	cols := make([]string, 0)
	vals := make([]any, 0)
	//for i := 0; i < t.NumField(); i++ {
	//	sf := t.Field(i)
	//	// skip unexported
	//	if sf.PkgPath != "" {
	//		continue
	//	}
	//	// determine column name
	//	col := sf.Tag.Get("db")
	//	if col == "" {
	//		col = sf.Tag.Get("json")
	//		if idx := strings.Index(col, ","); idx != -1 {
	//			col = col[:idx]
	//		}
	//	}
	//	if col == "" {
	//		col = strings.ToLower(sf.Name)
	//	}
	//
	//	// apply viewMask if present (支持按字段名或列名匹配)
	//	if viewMask != nil && len(mask) > 0 {
	//		if !mask[sf.Name] && !mask[col] {
	//			continue
	//		}
	//	}
	//
	//	cols = append(cols, col)
	//	vals = append(vals, v.Field(i).Interface())
	//}

	var err error

	cols, vals, err = structToColumnsAndValues(v)
	if err != nil {
		r.log.Errorf("extract columns and values failed: %v", err)
		return nil, errors.New("extract columns and values failed")
	}

	if len(cols) == 0 {
		return nil, errors.New("no columns to insert")
	}

	placeholders := strings.Repeat("?,", len(cols))
	placeholders = strings.TrimRight(placeholders, ",")
	aSql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", r.table, strings.Join(cols, ","), placeholders)

	if _, err := r.client.Exec(aSql, vals...); err != nil {
		r.log.Errorf("create failed: %v", err)
		return nil, errors.New("create failed")
	}

	// 返回创建后的 DTO（Doris 不一定会回填自增字段，视表结构而定）
	return r.mapper.ToDTO(ent), nil
}

// CreateX 使用传入的 db 创建记录，支持 viewMask 指定插入字段，返回受影响行数
func (r *Repository[DTO, ENTITY]) CreateX(ctx context.Context, dto *DTO, viewMask *fieldmaskpb.FieldMask) (int64, error) {
	if r.client == nil {
		return 0, errors.New("doris client is nil")
	}
	if r.table == "" {
		return 0, errors.New("table is empty")
	}
	if dto == nil {
		return 0, errors.New("dto is nil")
	}

	// 规范 viewMask 路径
	field.NormalizeFieldMaskPaths(viewMask)

	// DTO -> ENTITY
	ent := r.mapper.ToEntity(dto)

	// 通过反射收集列名与值（优先使用 struct tag: db -> ch -> json，否则使用小写字段名）
	v := reflect.ValueOf(ent)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return 0, errors.New("entity must be a struct or pointer to struct")
	}
	t := v.Type()

	mask := map[string]bool{}
	if viewMask != nil {
		for _, p := range viewMask.Paths {
			mask[p] = true
		}
	}

	cols := make([]string, 0)
	vals := make([]any, 0)
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		// skip unexported
		if sf.PkgPath != "" {
			continue
		}
		// determine column name
		col := sf.Tag.Get("db")
		if col == "" {
			col = sf.Tag.Get("json")
			if idx := strings.Index(col, ","); idx != -1 {
				col = col[:idx]
			}
		}
		if col == "" {
			col = strings.ToLower(sf.Name)
		}

		// apply viewMask if present (支持按字段名或列名匹配)
		if viewMask != nil && len(mask) > 0 {
			if !mask[sf.Name] && !mask[col] {
				continue
			}
		}

		cols = append(cols, col)
		vals = append(vals, v.Field(i).Interface())
	}

	if len(cols) == 0 {
		return 0, errors.New("no columns to insert")
	}

	placeholders := strings.Repeat("?,", len(cols))
	placeholders = strings.TrimRight(placeholders, ",")
	aSql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", r.table, strings.Join(cols, ","), placeholders)

	// 执行插入（底层 Exec 通常只返回 error）
	if _, err := r.client.Exec(aSql, vals...); err != nil {
		r.log.Errorf("create failed: %v", err)
		return 0, errors.New("create failed")
	}

	// Doris Exec 通常不提供 RowsAffected，成功则返回 1（表示已插入一条）
	return 1, nil
}

// BatchCreate 批量创建记录，返回创建后的 DTO 列表
func (r *Repository[DTO, ENTITY]) BatchCreate(ctx context.Context, dtos []*DTO, viewMask *fieldmaskpb.FieldMask) ([]*DTO, error) {
	if r.client == nil {
		return nil, errors.New("doris client is nil")
	}
	if r.table == "" {
		return nil, errors.New("table is empty")
	}
	if len(dtos) == 0 {
		return nil, nil
	}

	// 规范 viewMask 路径
	field.NormalizeFieldMaskPaths(viewMask)

	// 将 DTO 映射为实体切片（保留具体类型 ENTITY）
	entities := make([]*ENTITY, 0, len(dtos))
	for _, dto := range dtos {
		if dto == nil {
			continue
		}
		ent := r.mapper.ToEntity(dto)
		entities = append(entities, ent)
	}
	if len(entities) == 0 {
		return nil, nil
	}

	// 使用第一个实体的类型和字段信息构建列列表（与单条 Create 保持一致的规则）
	var cols []string
	var fieldIndexes []int
	firstVal := reflect.ValueOf(entities[0])
	if firstVal.Kind() == reflect.Ptr {
		firstVal = firstVal.Elem()
	}
	if !firstVal.IsValid() || firstVal.Kind() != reflect.Struct {
		return nil, errors.New("entity must be a struct or pointer to struct")
	}
	t := firstVal.Type()

	mask := map[string]bool{}
	if viewMask != nil {
		for _, p := range viewMask.Paths {
			mask[p] = true
		}
	}

	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		// skip unexported
		if sf.PkgPath != "" {
			continue
		}
		// determine column name
		col := sf.Tag.Get("db")
		if col == "" {
			col = sf.Tag.Get("json")
			if idx := strings.Index(col, ","); idx != -1 {
				col = col[:idx]
			}
		}
		if col == "" {
			col = strings.ToLower(sf.Name)
		}

		// apply viewMask if present (支持按字段名或列名匹配)
		if viewMask != nil && len(mask) > 0 {
			if !mask[sf.Name] && !mask[col] {
				continue
			}
		}

		cols = append(cols, col)
		fieldIndexes = append(fieldIndexes, i)
	}

	if len(cols) == 0 {
		return nil, errors.New("no columns to insert")
	}

	// 为每条实体收集值，保证顺序与 cols 对应
	vals := make([]any, 0, len(entities)*len(cols))
	for _, ent := range entities {
		v := reflect.ValueOf(ent)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
		for _, idx := range fieldIndexes {
			vals = append(vals, v.Field(idx).Interface())
		}
	}

	// 构造批量 INSERT 占位符
	rowPlaceholders := "(" + strings.TrimRight(strings.Repeat("?,", len(cols)), ",") + ")"
	rows := make([]string, 0, len(entities))
	for i := 0; i < len(entities); i++ {
		rows = append(rows, rowPlaceholders)
	}
	placeholders := strings.Join(rows, ",")
	aSql := "INSERT INTO " + r.table + " (" + strings.Join(cols, ",") + ") VALUES " + placeholders

	// 执行插入
	if _, err := r.client.Exec(aSql, vals...); err != nil {
		r.log.Errorf("batch create failed: %v", err)
		return nil, errors.New("batch create failed")
	}

	// 将实体映射回 DTO 列表并返回
	res := make([]*DTO, 0, len(entities))
	for _, ent := range entities {
		// 保证传入的是 *ENTITY
		e := ent
		res = append(res, r.mapper.ToDTO(e))
	}
	return res, nil
}

// Update 使用传入的 db（可包含 Where）更新记录，支持 updateMask 指定更新字段
func (r *Repository[DTO, ENTITY]) Update(ctx context.Context, dto *DTO, updateMask *fieldmaskpb.FieldMask) (*DTO, error) {
	if r.client == nil {
		return nil, errors.New("doris client is nil")
	}
	if r.table == "" {
		return nil, errors.New("table is empty")
	}
	if dto == nil {
		return nil, errors.New("dto is nil")
	}

	// 规范 updateMask 路径
	field.NormalizeFieldMaskPaths(updateMask)
	mask := map[string]bool{}
	if updateMask != nil {
		for _, p := range updateMask.Paths {
			mask[p] = true
		}
	}

	// DTO -> ENTITY
	ent := r.mapper.ToEntity(dto)

	// 反射处理实体字段
	v := reflect.ValueOf(ent)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return nil, errors.New("entity must be a struct or pointer to struct")
	}
	t := v.Type()

	// 识别主键字段（优先： tag `pk:"true"`，其次列名或字段名为 id/ID/Id）
	pkIdx := -1
	var pkCol string
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue
		}
		// detect pk tag
		if sf.Tag.Get("pk") == "true" {
			pkIdx = i
		}
		// determine column name
		col := sf.Tag.Get("db")
		if col == "" {
			col = sf.Tag.Get("json")
			if idx := strings.Index(col, ","); idx != -1 {
				col = col[:idx]
			}
		}
		if col == "" {
			col = strings.ToLower(sf.Name)
		}
		// detect id-like column if pk not set
		if pkIdx == -1 {
			lc := strings.ToLower(col)
			if lc == "id" || strings.ToLower(sf.Name) == "id" {
				pkIdx = i
			}
		}
		// record pk column if this index chosen
		if pkIdx == i {
			pkCol = col
		}
	}

	if pkIdx == -1 || pkCol == "" {
		return nil, errors.New("primary key field not found; cannot determine WHERE clause")
	}

	// 构建更新列和值，排除主键
	setExprs := make([]string, 0)
	setVals := make([]any, 0)
	for i := 0; i < t.NumField(); i++ {
		if i == pkIdx {
			continue
		}
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue
		}
		// determine column name
		col := sf.Tag.Get("db")
		if col == "" {
			col = sf.Tag.Get("json")
			if idx := strings.Index(col, ","); idx != -1 {
				col = col[:idx]
			}
		}
		if col == "" {
			col = strings.ToLower(sf.Name)
		}

		// 如果提供了 updateMask，则只更新被包含的字段（支持按字段名或列名）
		if len(mask) > 0 {
			if !mask[sf.Name] && !mask[col] {
				continue
			}
		} else {
			// 未提供 updateMask 时，跳过零值字段（避免覆盖为零值）
			if v.Field(i).IsZero() {
				continue
			}
		}

		setExprs = append(setExprs, fmt.Sprintf("%s = ?", col))
		setVals = append(setVals, v.Field(i).Interface())
	}

	if len(setExprs) == 0 {
		return nil, errors.New("no columns to update")
	}

	// 主键值
	pkVal := v.Field(pkIdx).Interface()

	// 构造 ALTER TABLE ... UPDATE ... WHERE ... （Doris mutation）
	whereClause := fmt.Sprintf("%s = ?", pkCol)
	aSql := fmt.Sprintf("ALTER TABLE %s UPDATE %s WHERE %s", r.table, strings.Join(setExprs, ", "), whereClause)

	// 执行更新
	args := append(setVals, pkVal)
	if _, err := r.client.Exec(aSql, args...); err != nil {
		r.log.Errorf("update failed: %v", err)
		return nil, errors.New("update failed")
	}

	// 尝试读取更新后的记录并返回（注意：Doris mutation 可能是异步的）
	var rawResults []any
	creator := func() any {
		var e ENTITY
		return &e
	}
	selectSQL := fmt.Sprintf("SELECT * FROM %s WHERE %s LIMIT 1", r.table, whereClause)
	if err := r.client.Query(ctx, creator, &rawResults, selectSQL, pkVal); err != nil {
		r.log.Errorf("read updated record failed: %v", err)
		return nil, errors.New("read updated record failed")
	}
	if len(rawResults) == 0 {
		return nil, nil
	}
	if ptr, ok := rawResults[0].(*ENTITY); ok {
		return r.mapper.ToDTO(ptr), nil
	}
	r.log.Errorf("unexpected result type after update")
	return nil, errors.New("unexpected result type")
}

// UpdateX 使用传入的 db（可包含 Where）更新记录，支持 updateMask 指定更新字段，返回受影响行数
func (r *Repository[DTO, ENTITY]) UpdateX(ctx context.Context, dto *DTO, updateMask *fieldmaskpb.FieldMask) (int64, error) {
	if r.client == nil {
		return 0, errors.New("doris client is nil")
	}
	if r.table == "" {
		return 0, errors.New("table is empty")
	}
	if dto == nil {
		return 0, errors.New("dto is nil")
	}

	// 规范 updateMask 路径
	field.NormalizeFieldMaskPaths(updateMask)
	mask := map[string]bool{}
	if updateMask != nil {
		for _, p := range updateMask.Paths {
			mask[p] = true
		}
	}

	// DTO -> ENTITY
	ent := r.mapper.ToEntity(dto)

	// 反射处理实体字段
	v := reflect.ValueOf(ent)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return 0, errors.New("entity must be a struct or pointer to struct")
	}
	t := v.Type()

	// 识别主键字段（优先： tag `pk:"true"`，其次列名或字段名为 id/ID/Id）
	pkIdx := -1
	var pkCol string
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue
		}
		if sf.Tag.Get("pk") == "true" {
			pkIdx = i
		}
		col := sf.Tag.Get("db")
		if col == "" {
			col = sf.Tag.Get("json")
			if idx := strings.Index(col, ","); idx != -1 {
				col = col[:idx]
			}
		}
		if col == "" {
			col = strings.ToLower(sf.Name)
		}
		if pkIdx == -1 {
			lc := strings.ToLower(col)
			if lc == "id" || strings.ToLower(sf.Name) == "id" {
				pkIdx = i
			}
		}
		if pkIdx == i {
			pkCol = col
		}
	}

	if pkIdx == -1 || pkCol == "" {
		return 0, errors.New("primary key field not found; cannot determine WHERE clause")
	}

	// 构建更新列和值，排除主键
	setExprs := make([]string, 0)
	setVals := make([]any, 0)
	for i := 0; i < t.NumField(); i++ {
		if i == pkIdx {
			continue
		}
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue
		}
		col := sf.Tag.Get("db")
		if col == "" {
			col = sf.Tag.Get("json")
			if idx := strings.Index(col, ","); idx != -1 {
				col = col[:idx]
			}
		}
		if col == "" {
			col = strings.ToLower(sf.Name)
		}

		// 如果提供了 updateMask，则只更新被包含的字段（支持按字段名或列名）
		if len(mask) > 0 {
			if !mask[sf.Name] && !mask[col] {
				continue
			}
		} else {
			// 未提供 updateMask 时，跳过零值字段（避免覆盖为零值）
			if v.Field(i).IsZero() {
				continue
			}
		}

		setExprs = append(setExprs, fmt.Sprintf("%s = ?", col))
		setVals = append(setVals, v.Field(i).Interface())
	}

	if len(setExprs) == 0 {
		return 0, errors.New("no columns to update")
	}

	// 主键值
	pkVal := v.Field(pkIdx).Interface()

	// 构造 ALTER TABLE ... UPDATE ... WHERE ... （Doris mutation）
	whereClause := fmt.Sprintf("%s = ?", pkCol)
	aSql := fmt.Sprintf("ALTER TABLE %s UPDATE %s WHERE %s", r.table, strings.Join(setExprs, ", "), whereClause)

	// 执行更新
	args := append(setVals, pkVal)
	if _, err := r.client.Exec(aSql, args...); err != nil {
		r.log.Errorf("update failed: %v", err)
		return 0, errors.New("update failed")
	}

	// Doris Exec 通常不提供 RowsAffected，成功则返回 1（表示已提交 mutation）
	return 1, nil
}

// Upsert 使用传入的 db（可包含 Where/其他 scope）执行插入或冲突更新，支持 updateMask 指定冲突时更新的字段
func (r *Repository[DTO, ENTITY]) Upsert(ctx context.Context, dto *DTO, updateMask *fieldmaskpb.FieldMask) (*DTO, error) {
	if r.client == nil {
		return nil, errors.New("doris client is nil")
	}
	if r.table == "" {
		return nil, errors.New("table is empty")
	}
	if dto == nil {
		return nil, errors.New("dto is nil")
	}

	// 规范 updateMask 路径
	field.NormalizeFieldMaskPaths(updateMask)
	mask := map[string]bool{}
	if updateMask != nil {
		for _, p := range updateMask.Paths {
			mask[p] = true
		}
	}

	// DTO -> ENTITY
	ent := r.mapper.ToEntity(dto)

	// 反射实体，识别主键并构建列/值用于 INSERT
	v := reflect.ValueOf(ent)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return nil, errors.New("entity must be a struct or pointer to struct")
	}
	t := v.Type()

	// 识别主键索引与列名（同 Update 的规则）
	pkIdx := -1
	var pkCol string
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue
		}
		if sf.Tag.Get("pk") == "true" {
			pkIdx = i
		}
		col := sf.Tag.Get("db")
		if col == "" {
			col = sf.Tag.Get("json")
			if idx := strings.Index(col, ","); idx != -1 {
				col = col[:idx]
			}
		}
		if col == "" {
			col = strings.ToLower(sf.Name)
		}
		if pkIdx == -1 {
			lc := strings.ToLower(col)
			if lc == "id" || strings.ToLower(sf.Name) == "id" {
				pkIdx = i
			}
		}
		if pkIdx == i {
			pkCol = col
		}
	}

	// 构建 INSERT 列与值（仍包含主键字段）
	cols := make([]string, 0)
	vals := make([]any, 0)
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue
		}
		col := sf.Tag.Get("db")
		if col == "" {
			col = sf.Tag.Get("json")
			if idx := strings.Index(col, ","); idx != -1 {
				col = col[:idx]
			}
		}
		if col == "" {
			col = strings.ToLower(sf.Name)
		}

		cols = append(cols, col)
		vals = append(vals, v.Field(i).Interface())
	}

	if len(cols) == 0 {
		return nil, errors.New("no columns to insert")
	}

	placeholders := strings.Repeat("?,", len(cols))
	placeholders = strings.TrimRight(placeholders, ",")
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", r.table, strings.Join(cols, ","), placeholders)

	// 先尝试 INSERT
	if _, err := r.client.Exec(insertSQL, vals...); err == nil {
		// 插入成功，返回 DTO
		e := ent
		return r.mapper.ToDTO(e), nil
	}

	// 插入失败：尝试按主键 UPDATE
	if pkIdx == -1 || pkCol == "" {
		r.log.Errorf("upsert insert failed and primary key not found")
		return nil, errors.New("upsert failed and primary key not found")
	}

	// 构建 UPDATE 的 set 列与值（排除主键）
	setExprs := make([]string, 0)
	setVals := make([]any, 0)
	for i := 0; i < t.NumField(); i++ {
		if i == pkIdx {
			continue
		}
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue
		}
		col := sf.Tag.Get("db")
		if col == "" {
			col = sf.Tag.Get("json")
			if idx := strings.Index(col, ","); idx != -1 {
				col = col[:idx]
			}
		}
		if col == "" {
			col = strings.ToLower(sf.Name)
		}

		if len(mask) > 0 {
			if !mask[sf.Name] && !mask[col] {
				continue
			}
		} else {
			if v.Field(i).IsZero() {
				continue
			}
		}

		setExprs = append(setExprs, fmt.Sprintf("%s = ?", col))
		setVals = append(setVals, v.Field(i).Interface())
	}

	if len(setExprs) == 0 {
		r.log.Errorf("upsert: no columns to update after insert failure")
		return nil, errors.New("no columns to update")
	}

	pkVal := v.Field(pkIdx).Interface()
	whereClause := fmt.Sprintf("%s = ?", pkCol)
	updateSQL := fmt.Sprintf("ALTER TABLE %s UPDATE %s WHERE %s", r.table, strings.Join(setExprs, ", "), whereClause)

	args := append(setVals, pkVal)
	if _, err := r.client.Exec(updateSQL, args...); err != nil {
		r.log.Errorf("upsert update failed: %v", err)
		return nil, errors.New("upsert update failed")
	}

	// 读取更新后的记录并返回（注意 mutation 可能异步）
	var rawResults []any
	creator := func() any {
		var e ENTITY
		return &e
	}
	selectSQL := fmt.Sprintf("SELECT * FROM %s WHERE %s LIMIT 1", r.table, whereClause)
	if err := r.client.Query(ctx, creator, &rawResults, selectSQL, pkVal); err != nil {
		r.log.Errorf("read upserted record failed: %v", err)
		return nil, errors.New("read upserted record failed")
	}
	if len(rawResults) == 0 {
		return nil, nil
	}
	if ptr, ok := rawResults[0].(*ENTITY); ok {
		return r.mapper.ToDTO(ptr), nil
	}
	r.log.Errorf("unexpected result type after upsert")
	return nil, errors.New("unexpected result type after upsert")
}

// Delete 使用传入的 query.Builder（可包含 Where）删除记录，支持软/硬删除
func (r *Repository[DTO, ENTITY]) Delete(ctx context.Context, qb *query.Builder, notSoftDelete bool) (int64, error) {
	if r.client == nil {
		return 0, errors.New("doris client is nil")
	}
	if r.table == "" {
		return 0, errors.New("table is empty")
	}

	var args []any
	whereSQL := ""
	if qb != nil {
		whereSQL, args = qb.BuildWhereParam()
	}

	if notSoftDelete {
		// 硬删除，按条件删除
		sqlStr := fmt.Sprintf("DELETE FROM %s", r.table)
		if whereSQL != "" {
			if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(whereSQL)), "WHERE") {
				sqlStr += " WHERE " + whereSQL
			} else {
				sqlStr += " " + whereSQL
			}
		}
		res, err := r.client.ExecContext(ctx, sqlStr, args...)
		if err != nil {
			r.log.Errorf("DELETE failed: %v, sql: %s, args: %v", err, sqlStr, args)
			return 0, errors.New("delete failed")
		}
		rows, _ := res.RowsAffected()
		return rows, nil
	}

	// 软删除：查找 deleted_at 字段
	t := reflect.TypeOf((*ENTITY)(nil)).Elem()
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t == nil || t.Kind() != reflect.Struct {
		return 0, errors.New("entity must be a struct type")
	}
	deletedCol := ""
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue
		}
		col := sf.Tag.Get("db")
		if col == "" {
			col = sf.Tag.Get("json")
			if idx := strings.Index(col, ","); idx != -1 {
				col = col[:idx]
			}
		}
		if col == "" {
			col = strings.ToLower(sf.Name)
		}
		lc := strings.ToLower(col)
		nameLc := strings.ToLower(sf.Name)
		if lc == "deleted_at" || lc == "deletedat" || nameLc == "deleted_at" || nameLc == "deletedat" {
			deletedCol = col
			break
		}
	}
	if deletedCol == "" {
		return 0, errors.New("soft delete not supported: deleted_at field not found on entity")
	}

	sqlStr := fmt.Sprintf("UPDATE %s SET %s = now()", r.table, deletedCol)
	if whereSQL != "" {
		if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(whereSQL)), "WHERE") {
			sqlStr += " WHERE " + whereSQL
		} else {
			sqlStr += " " + whereSQL
		}
	}
	res, err := r.client.ExecContext(ctx, sqlStr, args...)
	if err != nil {
		r.log.Errorf("soft delete (update deleted_at) failed: %v, sql: %s, args: %v", err, sqlStr, args)
		return 0, errors.New("delete failed")
	}
	rows, _ := res.RowsAffected()
	return rows, nil
}

// SoftDelete 对符合 whereSelectors 的记录执行软删除
// whereSelectors: 应用到查询的 where scopes（按顺序）
func (r *Repository[DTO, ENTITY]) SoftDelete(ctx context.Context, qb *query.Builder) (int64, error) {
	return r.Delete(ctx, qb, false)
}

// Exists 使用传入的 db（可包含 Where）检查是否存在记录
func (r *Repository[DTO, ENTITY]) Exists(ctx context.Context, baseWhere string, whereArgs ...any) (bool, error) {
	if r.client == nil {
		return false, errors.New("doris client is nil")
	}
	if r.table == "" {
		return false, errors.New("table is empty")
	}

	// 展开单个切片参数为独立参数
	if len(whereArgs) == 1 {
		v := reflect.ValueOf(whereArgs[0])
		if v.IsValid() && v.Kind() == reflect.Slice {
			expanded := make([]any, v.Len())
			for i := 0; i < v.Len(); i++ {
				expanded[i] = v.Index(i).Interface()
			}
			whereArgs = expanded
		}
	}

	sqlStr := fmt.Sprintf("SELECT 1 FROM %s", r.table)
	bw := strings.TrimSpace(baseWhere)
	if bw != "" {
		if !strings.HasPrefix(strings.ToUpper(bw), "WHERE") {
			sqlStr += " WHERE " + bw
		} else {
			sqlStr += " " + bw
		}
	}
	sqlStr += " LIMIT 1"

	var dummy uint8
	err := r.client.GetContext(ctx, &dummy, sqlStr, whereArgs...)
	if err != nil {
		// 没有行时部分驱动返回 sql.ErrNoRows
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		// 有些驱动可能返回包含 "no rows" 的错误字符串
		if strings.Contains(strings.ToLower(err.Error()), "no rows") || strings.Contains(strings.ToLower(err.Error()), "not found") {
			return false, nil
		}
		r.log.Errorf("exists query failed: %v", err)
		return false, errors.New("exists query failed")
	}
	return true, nil
}

// Insert 支持 map[string]any 或 struct 入参，插入单条数据
func (r *Repository[DTO, ENTITY]) Insert(ctx context.Context, dto *DTO, viewMask *fieldmaskpb.FieldMask) (*DTO, error) {
	return r.Create(ctx, dto, viewMask)
}

// BatchInsert 支持 map[string]any 数组或 struct 数组，批量插入
func (r *Repository[DTO, ENTITY]) BatchInsert(ctx context.Context, data any) error {
	if r.client == nil {
		return errors.New("doris client is nil")
	}
	if r.table == "" {
		return errors.New("table is empty")
	}
	rv := reflect.ValueOf(data)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return errors.New("input must be slice or array")
	}
	if rv.Len() == 0 {
		return nil
	}
	// 取第一个元素确定列
	first := rv.Index(0).Interface()
	var cols []string
	var err error
	if m, ok := first.(map[string]any); ok {
		cols, _, err = mapToColumnsAndValues(m)
		if err != nil {
			return err
		}
	} else if m, ok := first.(map[string]interface{}); ok {
		cols, _, err = mapToColumnsAndValues(m)
		if err != nil {
			return err
		}
	} else {
		cols, _, err = structToColumnsAndValues(reflect.ValueOf(first))
		if err != nil {
			return err
		}
	}
	if len(cols) == 0 {
		return errors.New("no columns to insert")
	}
	vals := make([]any, 0, rv.Len()*len(cols))
	for i := 0; i < rv.Len(); i++ {
		item := rv.Index(i).Interface()
		if m, ok := item.(map[string]any); ok {
			_, vvals, err := mapToColumnsAndValues(m)
			if err != nil {
				return err
			}
			vals = append(vals, vvals...)
		} else if m, ok := item.(map[string]interface{}); ok {
			_, vvals, err := mapToColumnsAndValues(m)
			if err != nil {
				return err
			}
			vals = append(vals, vvals...)
		} else {
			_, vvals, err := structToColumnsAndValues(reflect.ValueOf(item))
			if err != nil {
				return err
			}
			vals = append(vals, vvals...)
		}
	}
	rowPlaceholder := "(" + strings.TrimRight(strings.Repeat("?,", len(cols)), ",") + ")"
	rows := make([]string, 0, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		rows = append(rows, rowPlaceholder)
	}
	placeholders := strings.Join(rows, ",")
	sqlStr := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", r.table, strings.Join(cols, ","), placeholders)
	if _, err = r.client.Exec(sqlStr, vals...); err != nil {
		return err
	}
	return nil
}

// BatchInsertStruct 支持 struct slice 入参，批量插入
func (r *Repository[DTO, ENTITY]) BatchInsertStruct(ctx context.Context, data interface{}) error {
	rv := reflect.ValueOf(data)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return errors.New("input must be slice or array")
	}
	if rv.Len() == 0 {
		return nil
	}
	return r.BatchInsert(ctx, data)
}
