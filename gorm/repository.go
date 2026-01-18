package gorm

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"github.com/go-kratos/kratos/v2/log"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/tx7do/go-utils/mapper"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/gorm/field"
	"github.com/tx7do/go-crud/gorm/filter"
	paging "github.com/tx7do/go-crud/gorm/pagination"
	"github.com/tx7do/go-crud/gorm/sorting"
	paginationFilter "github.com/tx7do/go-crud/pagination/filter"
	paginationSorting "github.com/tx7do/go-crud/pagination/sorting"
)

// PagingResult 通用分页返回
type PagingResult[E any] struct {
	Items []*E   `json:"items"`
	Total uint64 `json:"total"`
}

// CountOptions 为扩展的计数选项
type CountOptions struct {
	// Distinct 指定要去重的字段（比如 "user_id"）
	Distinct string
	// Scopes 额外的自定义 scope，按顺序应用
	Scopes []func(*gorm.DB) *gorm.DB
	// Timeout 为查询超时时间（0 表示不设置超时）
	Timeout time.Duration
}

// Repository GORM 仓库，包含常用的 CRUD 方法
type Repository[DTO any, ENTITY any] struct {
	mapper *mapper.CopierMapper[DTO, ENTITY]

	offsetPaginator *paging.OffsetPaginator
	pagePaginator   *paging.PagePaginator
	tokenPaginator  *paging.TokenPaginator

	structuredFilter      *filter.StructuredFilter
	queryStringConverter  *paginationFilter.QueryStringConverter
	filterStringConverter *paginationFilter.FilterStringConverter

	structuredSorting      *sorting.StructuredSorting
	orderByStringConverter *paginationSorting.OrderByStringConverter

	fieldSelector *field.Selector
}

func NewRepository[DTO any, ENTITY any](mapper *mapper.CopierMapper[DTO, ENTITY]) *Repository[DTO, ENTITY] {
	return &Repository[DTO, ENTITY]{
		mapper: mapper,

		structuredSorting: sorting.NewStructuredSorting(),

		offsetPaginator: paging.NewOffsetPaginator(),
		pagePaginator:   paging.NewPagePaginator(),
		tokenPaginator:  paging.NewTokenPaginator(),

		structuredFilter: filter.NewStructuredFilter(),

		queryStringConverter:  paginationFilter.NewQueryStringConverter(),
		filterStringConverter: paginationFilter.NewFilterStringConverter(),

		orderByStringConverter: paginationSorting.NewOrderByStringConverter(),

		fieldSelector: field.NewFieldSelector(),
	}
}

// Count 使用 whereSelectors 计算符合条件的记录数
func (r *Repository[DTO, ENTITY]) Count(ctx context.Context, db *gorm.DB, whereSelectors []func(*gorm.DB) *gorm.DB) (int64, error) {
	if db == nil {
		return 0, errors.New("db is nil")
	}

	countDB := db.WithContext(ctx).Model(new(ENTITY))
	for _, s := range whereSelectors {
		if s != nil {
			countDB = s(countDB)
		}
	}

	var cnt int64
	if err := countDB.Count(&cnt).Error; err != nil {
		log.Errorf("query count failed: %s", err.Error())
		return 0, errors.New("query count failed")
	}
	return cnt, nil
}

// CountWithOptions 使用可选参数执行计数，返回 int64（更通用）
// 保持原有 whereSelectors 参数风格，额外行为由 opts 控制
func (r *Repository[DTO, ENTITY]) CountWithOptions(ctx context.Context, db *gorm.DB, whereSelectors []func(*gorm.DB) *gorm.DB, opts *CountOptions) (int64, error) {
	if db == nil {
		return 0, errors.New("db is nil")
	}
	if opts == nil {
		opts = &CountOptions{}
	}

	// 支持超时
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	countDB := db.WithContext(ctx).Model(new(ENTITY))

	// 应用 where selectors
	for _, s := range whereSelectors {
		if s != nil {
			countDB = s(countDB)
		}
	}

	// 应用额外 scopes（可用于 join、额外过滤等）
	for _, s := range opts.Scopes {
		if s != nil {
			countDB = s(countDB)
		}
	}

	// 支持 distinct 计数
	if opts.Distinct != "" {
		countDB = countDB.Distinct(opts.Distinct)
	}

	var cnt int64
	if err := countDB.Count(&cnt).Error; err != nil {
		log.Errorf("query count failed: %s", err.Error())
		return 0, errors.New("query count failed")
	}
	return cnt, nil
}

// ListWithPaging 使用 PagingRequest 查询列表（接收 *gorm.DB）
func (r *Repository[DTO, ENTITY]) ListWithPaging(ctx context.Context, db *gorm.DB, req *paginationV1.PagingRequest) (*PagingResult[DTO], error) {
	if req == nil {
		return nil, errors.New("paging request is nil")
	}
	if db == nil {
		return nil, errors.New("db is nil")
	}

	var err error
	var whereSelectors []func(*gorm.DB) *gorm.DB
	var selectSelector func(*gorm.DB) *gorm.DB
	var sortingSelector func(*gorm.DB) *gorm.DB
	var pagingSelector func(*gorm.DB) *gorm.DB

	// apply filters
	if req.GetQuery() != "" {
		var filterExpr *paginationV1.FilterExpr
		filterExpr, err = r.queryStringConverter.Convert(req.GetQuery())
		if err != nil {
			log.Errorf("convert query to filter expr failed: %s", err.Error())
			return nil, err
		}
		req.FilteringType = &paginationV1.PagingRequest_FilterExpr{FilterExpr: filterExpr}
	} else if req.GetFilter() != "" {
		var filterExpr *paginationV1.FilterExpr
		filterExpr, err = r.filterStringConverter.Convert(req.GetFilter())
		if err != nil {
			log.Errorf("convert filter string to filter expr failed: %s", err.Error())
			return nil, err
		}
		req.FilteringType = &paginationV1.PagingRequest_FilterExpr{FilterExpr: filterExpr}
	}

	whereSelectors, err = r.structuredFilter.BuildSelectors(req.GetFilterExpr())
	if err != nil {
		log.Errorf("build structured filter selectors failed: %s", err.Error())
	}

	// select fields
	if req.GetFieldMask() != nil && len(req.GetFieldMask().Paths) > 0 {
		selectSelector, err = r.fieldSelector.BuildSelector(req.GetFieldMask().GetPaths())
		if err != nil {
			log.Errorf("build field select selector failed: %s", err.Error())
		}
	}

	// order by
	if len(req.GetOrderBy()) > 0 {
		var sortings []*paginationV1.Sorting
		sortings, err = r.orderByStringConverter.Convert(req.GetOrderBy())
		if err != nil {
			log.Errorf("convert order by string to sorting failed: %s", err.Error())
			return nil, err
		}
		sortingSelector = r.structuredSorting.BuildScope(sortings)
	} else if len(req.GetSorting()) > 0 {
		sortingSelector = r.structuredSorting.BuildScope(req.GetSorting())
	}

	// pagination
	if !req.GetNoPaging() {
		if req.Page != nil && req.PageSize != nil {
			pagingSelector = r.pagePaginator.BuildDB(int(req.GetPage()), int(req.GetPageSize()))
		} else if req.Offset != nil && req.Limit != nil {
			pagingSelector = r.offsetPaginator.BuildDB(int(req.GetOffset()), int(req.GetLimit()))
		} else if req.Token != nil && req.Offset != nil {
			pagingSelector = r.tokenPaginator.BuildDB(req.GetToken(), int(req.GetOffset()))
		}
	}

	// 构造查询 DB 并应用 selectors
	listDB := db.WithContext(ctx).Model(new(ENTITY))
	for _, s := range whereSelectors {
		if s != nil {
			listDB = s(listDB)
		}
	}
	if selectSelector != nil {
		listDB = selectSelector(listDB)
	}
	if sortingSelector != nil {
		listDB = sortingSelector(listDB)
	}
	if pagingSelector != nil {
		listDB = pagingSelector(listDB)
	}

	// 执行查询
	var entities []*ENTITY
	if err = listDB.Find(&entities).Error; err != nil {
		log.Errorf("query list failed: %s", err.Error())
		return nil, errors.New("query list failed")
	}

	// map to DTOs
	dtos := make([]*DTO, 0, len(entities))
	for _, e := range entities {
		dtos = append(dtos, r.mapper.ToDTO(e))
	}

	// 计数（只使用 whereSelectors）
	total, err := r.Count(ctx, db, whereSelectors)
	if err != nil {
		log.Errorf("count query failed: %s", err.Error())
		return nil, err
	}

	res := &PagingResult[DTO]{
		Items: dtos,
		Total: uint64(total),
	}
	return res, nil
}

// ListWithPagination 使用 PaginationRequest 查询列表（接收 *gorm.DB）
func (r *Repository[DTO, ENTITY]) ListWithPagination(ctx context.Context, db *gorm.DB, req *paginationV1.PaginationRequest) (*PagingResult[DTO], error) {
	if req == nil {
		return nil, errors.New("pagination request is nil")
	}
	if db == nil {
		return nil, errors.New("db is nil")
	}

	var err error
	var whereSelectors []func(*gorm.DB) *gorm.DB
	var selectSelector func(*gorm.DB) *gorm.DB
	var sortingSelector func(*gorm.DB) *gorm.DB
	var pagingSelector func(*gorm.DB) *gorm.DB

	// filters
	if req.GetQuery() != "" {
		var filterExpr *paginationV1.FilterExpr
		filterExpr, err = r.queryStringConverter.Convert(req.GetQuery())
		if err != nil {
			log.Errorf("convert query to filter expr failed: %s", err.Error())
			return nil, err
		}
		req.FilteringType = &paginationV1.PaginationRequest_FilterExpr{FilterExpr: filterExpr}

	} else if req.GetFilter() != "" {
		var filterExpr *paginationV1.FilterExpr
		filterExpr, err = r.filterStringConverter.Convert(req.GetFilter())
		if err != nil {
			log.Errorf("convert filter string to filter expr failed: %s", err.Error())
			return nil, err
		}
		req.FilteringType = &paginationV1.PaginationRequest_FilterExpr{FilterExpr: filterExpr}
	}

	whereSelectors, err = r.structuredFilter.BuildSelectors(req.GetFilterExpr())
	if err != nil {
		log.Errorf("build structured filter selectors failed: %s", err.Error())
	}

	// select fields
	if req.GetFieldMask() != nil && len(req.GetFieldMask().Paths) > 0 {
		selectSelector, err = r.fieldSelector.BuildSelector(req.GetFieldMask().GetPaths())
		if err != nil {
			log.Errorf("build field select selector failed: %s", err.Error())
		}
	}

	// order by
	if len(req.GetOrderBy()) > 0 {
		var sortings []*paginationV1.Sorting
		sortings, err = r.orderByStringConverter.Convert(req.GetOrderBy())
		if err != nil {
			log.Errorf("convert order by string to sorting failed: %s", err.Error())
			return nil, err
		}
		sortingSelector = r.structuredSorting.BuildScope(sortings)
	} else if len(req.GetSorting()) > 0 {
		sortingSelector = r.structuredSorting.BuildScope(req.GetSorting())
	}

	// pagination types
	switch req.GetPaginationType().(type) {
	case *paginationV1.PaginationRequest_OffsetBased:
		pagingSelector = r.offsetPaginator.BuildDB(int(req.GetOffsetBased().GetOffset()), int(req.GetOffsetBased().GetLimit()))
	case *paginationV1.PaginationRequest_PageBased:
		pagingSelector = r.pagePaginator.BuildDB(int(req.GetPageBased().GetPage()), int(req.GetPageBased().GetPageSize()))
	case *paginationV1.PaginationRequest_TokenBased:
		pagingSelector = r.tokenPaginator.BuildDB(req.GetTokenBased().GetToken(), int(req.GetTokenBased().GetPageSize()))
	}

	// 构造查询 DB 并应用 selectors
	listDB := db.WithContext(ctx).Model(new(ENTITY))
	for _, s := range whereSelectors {
		if s != nil {
			listDB = s(listDB)
		}
	}
	if selectSelector != nil {
		listDB = selectSelector(listDB)
	}
	if sortingSelector != nil {
		listDB = sortingSelector(listDB)
	}
	if pagingSelector != nil {
		listDB = pagingSelector(listDB)
	}

	// 执行查询
	var entities []*ENTITY
	if err = listDB.Find(&entities).Error; err != nil {
		log.Errorf("query list failed: %s", err.Error())
		return nil, errors.New("query list failed")
	}

	// map to DTOs
	dtos := make([]*DTO, 0, len(entities))
	for _, e := range entities {
		dtos = append(dtos, r.mapper.ToDTO(e))
	}

	// 计数
	total, err := r.Count(ctx, db, whereSelectors)
	if err != nil {
		log.Errorf("count query failed: %s", err.Error())
		return nil, err
	}

	res := &PagingResult[DTO]{
		Items: dtos,
		Total: uint64(total),
	}
	return res, nil
}

// Get 根据查询条件获取单条记录
// 示例调用： `dto, err := q.Get(ctx, db.Where("id = ?", id), nil)`
func (r *Repository[DTO, ENTITY]) Get(ctx context.Context, db *gorm.DB, viewMask *fieldmaskpb.FieldMask) (*DTO, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}

	field.NormalizeFieldMaskPaths(viewMask)

	qdb := db.WithContext(ctx).Model(new(ENTITY))
	if viewMask != nil && len(viewMask.Paths) > 0 {
		qdb = qdb.Select(viewMask.GetPaths())
	}

	var ent ENTITY
	if err := qdb.First(&ent).Error; err != nil {
		return nil, err
	}

	dto := r.mapper.ToDTO(&ent)
	return dto, nil
}

// GetWithFilters 接受 whereSelectors 并在内部应用到查询 DB，然后执行原有逻辑
// 示例调用：使用 q.queryStringFilter 等构造 selectors 后调用新方法
// whereSelectors, _ := q.queryStringFilter.BuildSelectors(req.GetQuery(), req.GetOrQuery())
// dto, err := q.GetWithFilters(ctx, db, whereSelectors, viewMask)
func (r *Repository[DTO, ENTITY]) GetWithFilters(ctx context.Context, db *gorm.DB, whereSelectors []func(*gorm.DB) *gorm.DB, viewMask *fieldmaskpb.FieldMask) (*DTO, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}

	// 规范 viewMask 路径（复用已有 helper）
	field.NormalizeFieldMaskPaths(viewMask)

	// 构造查询 DB 并应用 where selectors
	qdb := db.WithContext(ctx).Model(new(ENTITY))
	for _, s := range whereSelectors {
		if s != nil {
			qdb = s(qdb)
		}
	}

	// 应用字段选择
	if viewMask != nil && len(viewMask.Paths) > 0 {
		qdb = qdb.Select(viewMask.GetPaths())
	}

	// 执行查询
	var ent ENTITY
	if err := qdb.First(&ent).Error; err != nil {
		return nil, err
	}

	dto := r.mapper.ToDTO(&ent)
	return dto, nil
}

// Only alias
func (r *Repository[DTO, ENTITY]) Only(ctx context.Context, db *gorm.DB, viewMask *fieldmaskpb.FieldMask) (*DTO, error) {
	return r.Get(ctx, db, viewMask)
}

// Create 在数据库中创建一条记录，返回创建后的 DTO
// 示例调用： `dto, err := q.Create(ctx, db, dto, viewMask)`
func (r *Repository[DTO, ENTITY]) Create(ctx context.Context, db *gorm.DB, dto *DTO, viewMask *fieldmaskpb.FieldMask) (*DTO, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}
	if dto == nil {
		return nil, errors.New("dto is nil")
	}

	// 规范 viewMask 路径（目前仅规范，返回时直接使用 mapper 的结果）
	field.NormalizeFieldMaskPaths(viewMask)

	// DTO -> ENTITY
	ent := r.mapper.ToEntity(dto)

	// 执行创建
	qdb := db.WithContext(ctx).Model(new(ENTITY))
	res := qdb.Create(&ent)
	if res.Error != nil {
		log.Errorf("create failed: %s", res.Error.Error())
		return nil, errors.New("create failed")
	}

	// 返回创建后的 DTO（ent 已由 GORM 填充自增等字段）
	return r.mapper.ToDTO(ent), nil
}

// CreateX 使用传入的 db 创建记录，支持 viewMask 指定插入字段，返回受影响行数
// 示例调用： `rows, err := q.CreateX(ctx, db, dto, viewMask)`
func (r *Repository[DTO, ENTITY]) CreateX(ctx context.Context, db *gorm.DB, dto *DTO, viewMask *fieldmaskpb.FieldMask) (int64, error) {
	if db == nil {
		return 0, errors.New("db is nil")
	}
	if dto == nil {
		return 0, errors.New("dto is nil")
	}

	// 规范 viewMask 路径（目前仅规范）
	field.NormalizeFieldMaskPaths(viewMask)

	// DTO -> ENTITY
	ent := r.mapper.ToEntity(dto)

	// 构造 DB（传入的 db 可已包含 where/其他 scope）
	qdb := db.WithContext(ctx).Model(new(ENTITY))

	// 指定插入字段（如果需要）
	if viewMask != nil && len(viewMask.Paths) > 0 {
		qdb = qdb.Select(viewMask.GetPaths())
	}

	// 执行创建
	res := qdb.Create(&ent)
	if res.Error != nil {
		log.Errorf("create failed: %s", res.Error.Error())
		return 0, errors.New("create failed")
	}
	return res.RowsAffected, nil
}

// CreateXWithFilters 接受 whereSelectors 并在内部应用到查询 DB，然后执行创建，返回受影响行数
// 示例调用：构造 selectors 后调用： `rows, err := q.CreateXWithFilters(ctx, db, whereSelectors, dto, viewMask)`
func (r *Repository[DTO, ENTITY]) CreateXWithFilters(ctx context.Context, db *gorm.DB, whereSelectors []func(*gorm.DB) *gorm.DB, dto *DTO, viewMask *fieldmaskpb.FieldMask) (int64, error) {
	if db == nil {
		return 0, errors.New("db is nil")
	}
	if dto == nil {
		return 0, errors.New("dto is nil")
	}

	// 规范 viewMask 路径
	field.NormalizeFieldMaskPaths(viewMask)

	// DTO -> ENTITY
	ent := r.mapper.ToEntity(dto)

	// 构造查询 DB 并应用 where selectors（尽管 Create 常不依赖 where，但遵循项目风格）
	qdb := db.WithContext(ctx).Model(new(ENTITY))
	for _, s := range whereSelectors {
		if s != nil {
			qdb = s(qdb)
		}
	}

	// 指定插入字段（如果需要）
	if viewMask != nil && len(viewMask.Paths) > 0 {
		qdb = qdb.Select(viewMask.GetPaths())
	}

	// 执行创建
	res := qdb.Create(&ent)
	if res.Error != nil {
		log.Errorf("create failed: %s", res.Error.Error())
		return 0, errors.New("create failed")
	}
	return res.RowsAffected, nil
}

// BatchCreate 批量创建记录，返回创建后的 DTO 列表
// 将此方法添加到 `gorm/repository.go` 中的 Repository 定义下
func (r *Repository[DTO, ENTITY]) BatchCreate(ctx context.Context, db *gorm.DB, dtos []*DTO, viewMask *fieldmaskpb.FieldMask) ([]*DTO, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}
	if len(dtos) == 0 {
		return nil, nil
	}

	// 规范 viewMask 路径
	field.NormalizeFieldMaskPaths(viewMask)

	res := make([]*DTO, 0, len(dtos))
	for _, dto := range dtos {
		if dto == nil {
			continue
		}

		// DTO -> ENTITY（保持与单条 Create 一致的映射方式）
		ent := r.mapper.ToEntity(dto)

		// 为每条记录构造独立的操作 DB（保留传入 db 的 scope）
		qdb := db.WithContext(ctx).Model(new(ENTITY))
		if viewMask != nil && len(viewMask.Paths) > 0 {
			qdb = qdb.Select(viewMask.GetPaths())
		}

		createResult := qdb.Create(&ent)
		if createResult.Error != nil {
			log.Errorf("batch create failed: %s", createResult.Error.Error())
			return nil, errors.New("batch create failed")
		}

		res = append(res, r.mapper.ToDTO(ent))
	}

	return res, nil
}

// Update 使用传入的 db（可包含 Where）更新记录，支持 updateMask 指定更新字段
// 示例调用： `dto, err := q.Update(ctx, db.Where("id = ?", id), dto, updateMask)`
func (r *Repository[DTO, ENTITY]) Update(ctx context.Context, db *gorm.DB, dto *DTO, updateMask *fieldmaskpb.FieldMask) (*DTO, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}
	if dto == nil {
		return nil, errors.New("dto is nil")
	}

	// 规范 updateMask 路径
	field.NormalizeFieldMaskPaths(updateMask)

	// DTO -> ENTITY
	ent := r.mapper.ToEntity(dto)

	// 构造查询 DB（传入的 db 可已包含 where）
	qdb := db.WithContext(ctx).Model(new(ENTITY))

	// 指定更新字段
	if updateMask != nil && len(updateMask.Paths) > 0 {
		qdb = qdb.Select(updateMask.GetPaths())
	}

	// 执行更新
	res := qdb.Updates(ent)
	if res.Error != nil {
		log.Errorf("update failed: %s", res.Error.Error())
		return nil, errors.New("update failed")
	}

	// 读取并返回更新后的实体
	var updated ENTITY
	readDB := qdb.Select("*")
	if err := readDB.First(&updated).Error; err != nil {
		return nil, err
	}
	return r.mapper.ToDTO(&updated), nil
}

// UpdateWithFilters 接受 whereSelectors 并在内部应用到查询 DB，然后执行更新
// 示例调用：构造 selectors 后调用： `dto, err := q.UpdateWithFilters(ctx, db, whereSelectors, dto, updateMask)`
func (r *Repository[DTO, ENTITY]) UpdateWithFilters(ctx context.Context, db *gorm.DB, whereSelectors []func(*gorm.DB) *gorm.DB, dto *DTO, updateMask *fieldmaskpb.FieldMask) (*DTO, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}
	if dto == nil {
		return nil, errors.New("dto is nil")
	}

	// 规范 updateMask 路径
	field.NormalizeFieldMaskPaths(updateMask)

	// DTO -> ENTITY
	ent := r.mapper.ToEntity(dto)

	// 构造查询 DB 并应用 where selectors
	qdb := db.WithContext(ctx).Model(new(ENTITY))
	for _, s := range whereSelectors {
		if s != nil {
			qdb = s(qdb)
		}
	}

	// 指定更新字段
	if updateMask != nil && len(updateMask.Paths) > 0 {
		qdb = qdb.Select(updateMask.GetPaths())
	}

	// 执行更新
	res := qdb.Updates(ent)
	if res.Error != nil {
		log.Errorf("update failed: %s", res.Error.Error())
		return nil, errors.New("update failed")
	}

	// 读取并返回更新后的实体
	var updated ENTITY
	readDB := qdb.Select("*")
	if err := readDB.First(&updated).Error; err != nil {
		return nil, err
	}
	return r.mapper.ToDTO(&updated), nil
}

// UpdateX 使用传入的 db（可包含 Where）更新记录，支持 updateMask 指定更新字段，返回受影响行数
// 示例调用： `rows, err := q.UpdateX(ctx, db.Where("id = ?", id), dto, updateMask)`
func (r *Repository[DTO, ENTITY]) UpdateX(ctx context.Context, db *gorm.DB, dto *DTO, updateMask *fieldmaskpb.FieldMask) (int64, error) {
	if db == nil {
		return 0, errors.New("db is nil")
	}
	if dto == nil {
		return 0, errors.New("dto is nil")
	}

	// 规范 updateMask 路径
	field.NormalizeFieldMaskPaths(updateMask)

	// DTO -> ENTITY
	ent := r.mapper.ToEntity(dto)

	// 构造查询 DB（传入的 db 可已包含 where）
	qdb := db.WithContext(ctx).Model(new(ENTITY))

	// 指定更新字段
	if updateMask != nil && len(updateMask.Paths) > 0 {
		qdb = qdb.Select(updateMask.GetPaths())
	}

	// 执行更新
	res := qdb.Updates(ent)
	if res.Error != nil {
		log.Errorf("update failed: %s", res.Error.Error())
		return 0, errors.New("update failed")
	}
	return res.RowsAffected, nil
}

// UpdateXWithFilters 接受 whereSelectors 并在内部应用到查询 DB，然后执行更新，返回受影响行数
// 示例调用：构造 selectors 后调用： `rows, err := q.UpdateXWithFilters(ctx, db, whereSelectors, dto, updateMask)`
func (r *Repository[DTO, ENTITY]) UpdateXWithFilters(ctx context.Context, db *gorm.DB, whereSelectors []func(*gorm.DB) *gorm.DB, dto *DTO, updateMask *fieldmaskpb.FieldMask) (int64, error) {
	if db == nil {
		return 0, errors.New("db is nil")
	}
	if dto == nil {
		return 0, errors.New("dto is nil")
	}

	// 规范 updateMask 路径
	field.NormalizeFieldMaskPaths(updateMask)

	// DTO -> ENTITY
	ent := r.mapper.ToEntity(dto)

	// 构造查询 DB 并应用 where selectors
	qdb := db.WithContext(ctx).Model(new(ENTITY))
	for _, s := range whereSelectors {
		if s != nil {
			qdb = s(qdb)
		}
	}

	// 指定更新字段
	if updateMask != nil && len(updateMask.Paths) > 0 {
		qdb = qdb.Select(updateMask.GetPaths())
	}

	// 执行更新
	res := qdb.Updates(ent)
	if res.Error != nil {
		log.Errorf("update failed: %s", res.Error.Error())
		return 0, errors.New("update failed")
	}
	return res.RowsAffected, nil
}

// Upsert 使用传入的 db（可包含 Where/其他 scope）执行插入或冲突更新，支持 updateMask 指定冲突时更新的字段
// 示例调用： `dto, err := q.Upsert(ctx, db, dto, updateMask)`
func (r *Repository[DTO, ENTITY]) Upsert(ctx context.Context, db *gorm.DB, dto *DTO, updateMask *fieldmaskpb.FieldMask) (*DTO, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}
	if dto == nil {
		return nil, errors.New("dto is nil")
	}

	// 规范 updateMask 路径
	field.NormalizeFieldMaskPaths(updateMask)

	// DTO -> ENTITY
	ent := r.mapper.ToEntity(dto)

	// 构造查询 DB（传入的 db 可已包含 where/其他 scope）
	qdb := db.WithContext(ctx).Model(new(ENTITY))

	// 构造 OnConflict 子句：若提供了 updateMask 则仅在冲突时更新指定列，否则更新所有列
	var onConflict clause.OnConflict
	if updateMask != nil && len(updateMask.Paths) > 0 {
		onConflict = clause.OnConflict{
			DoUpdates: clause.AssignmentColumns(updateMask.GetPaths()),
		}
	} else {
		onConflict = clause.OnConflict{
			UpdateAll: true,
		}
	}

	// 执行 upsert
	res := qdb.Clauses(onConflict).Create(&ent)
	if res.Error != nil {
		log.Errorf("upsert failed: %s", res.Error.Error())
		return nil, errors.New("upsert failed")
	}

	// 返回 upsert 后的 DTO（ent 已由 GORM 填充）
	return r.mapper.ToDTO(ent), nil
}

// UpsertWithFilters 接受 whereSelectors 并在内部应用到查询 DB，然后执行 upsert，支持 updateMask 指定冲突时更新的字段
// 示例调用：构造 selectors 后调用： `dto, err := q.UpsertWithFilters(ctx, db, whereSelectors, dto, updateMask)`
func (r *Repository[DTO, ENTITY]) UpsertWithFilters(ctx context.Context, db *gorm.DB, whereSelectors []func(*gorm.DB) *gorm.DB, dto *DTO, updateMask *fieldmaskpb.FieldMask) (*DTO, error) {
	if db == nil {
		return nil, errors.New("db is nil")
	}
	if dto == nil {
		return nil, errors.New("dto is nil")
	}

	// 规范 updateMask 路径
	field.NormalizeFieldMaskPaths(updateMask)

	// DTO -> ENTITY
	ent := r.mapper.ToEntity(dto)

	// 构造查询 DB 并应用 where selectors（遵循项目风格）
	qdb := db.WithContext(ctx).Model(new(ENTITY))
	for _, s := range whereSelectors {
		if s != nil {
			qdb = s(qdb)
		}
	}

	// 构造 OnConflict 子句
	var onConflict clause.OnConflict
	if updateMask != nil && len(updateMask.Paths) > 0 {
		onConflict = clause.OnConflict{
			DoUpdates: clause.AssignmentColumns(updateMask.GetPaths()),
		}
	} else {
		onConflict = clause.OnConflict{
			UpdateAll: true,
		}
	}

	// 执行 upsert
	res := qdb.Clauses(onConflict).Create(&ent)
	if res.Error != nil {
		log.Errorf("upsert failed: %s", res.Error.Error())
		return nil, errors.New("upsert failed")
	}

	return r.mapper.ToDTO(ent), nil
}

// UpsertX 使用传入的 db（可包含 Where/其他 scope）执行插入或冲突更新，支持 updateMask 指定冲突时更新的字段，返回受影响行数
// 示例调用： `rows, err := q.UpsertX(ctx, db, dto, updateMask)`
func (r *Repository[DTO, ENTITY]) UpsertX(ctx context.Context, db *gorm.DB, dto *DTO, updateMask *fieldmaskpb.FieldMask) (int64, error) {
	if db == nil {
		return 0, errors.New("db is nil")
	}
	if dto == nil {
		return 0, errors.New("dto is nil")
	}

	// 规范 updateMask 路径
	field.NormalizeFieldMaskPaths(updateMask)

	// DTO -> ENTITY
	ent := r.mapper.ToEntity(dto)

	// 构造查询 DB（传入的 db 可已包含 where/其他 scope）
	qdb := db.WithContext(ctx).Model(new(ENTITY))

	// 构造 OnConflict 子句：若提供了 updateMask 则仅在冲突时更新指定列，否则更新所有列
	var onConflict clause.OnConflict
	if updateMask != nil && len(updateMask.Paths) > 0 {
		onConflict = clause.OnConflict{
			DoUpdates: clause.AssignmentColumns(updateMask.GetPaths()),
		}
	} else {
		onConflict = clause.OnConflict{
			UpdateAll: true,
		}
	}

	// 执行 upsert
	res := qdb.Clauses(onConflict).Create(&ent)
	if res.Error != nil {
		log.Errorf("upsert failed: %s", res.Error.Error())
		return 0, errors.New("upsert failed")
	}

	return res.RowsAffected, nil
}

// UpsertXWithFilters 接受 whereSelectors 并在内部应用到查询 DB，然后执行 upsert，支持 updateMask 指定冲突时更新的字段，返回受影响行数
// 示例调用：构造 selectors 后调用： `rows, err := q.UpsertXWithFilters(ctx, db, whereSelectors, dto, updateMask)`
func (r *Repository[DTO, ENTITY]) UpsertXWithFilters(ctx context.Context, db *gorm.DB, whereSelectors []func(*gorm.DB) *gorm.DB, dto *DTO, updateMask *fieldmaskpb.FieldMask) (int64, error) {
	if db == nil {
		return 0, errors.New("db is nil")
	}
	if dto == nil {
		return 0, errors.New("dto is nil")
	}

	// 规范 updateMask 路径
	field.NormalizeFieldMaskPaths(updateMask)

	// DTO -> ENTITY
	ent := r.mapper.ToEntity(dto)

	// 构造查询 DB 并应用 where selectors（遵循项目风格）
	qdb := db.WithContext(ctx).Model(new(ENTITY))
	for _, s := range whereSelectors {
		if s != nil {
			qdb = s(qdb)
		}
	}

	// 构造 OnConflict 子句
	var onConflict clause.OnConflict
	if updateMask != nil && len(updateMask.Paths) > 0 {
		onConflict = clause.OnConflict{
			DoUpdates: clause.AssignmentColumns(updateMask.GetPaths()),
		}
	} else {
		onConflict = clause.OnConflict{
			UpdateAll: true,
		}
	}

	// 执行 upsert
	res := qdb.Clauses(onConflict).Create(&ent)
	if res.Error != nil {
		log.Errorf("upsert failed: %s", res.Error.Error())
		return 0, errors.New("upsert failed")
	}

	return res.RowsAffected, nil
}

// Delete 使用传入的 db（可包含 Where）删除记录
// 示例调用： `rows, err := q.Delete(ctx, db.Where("id = ?", id))`
func (r *Repository[DTO, ENTITY]) Delete(ctx context.Context, db *gorm.DB, notSoftDelete bool) (int64, error) {
	if db == nil {
		return 0, errors.New("db is nil")
	}

	qdb := db.WithContext(ctx).Model(new(ENTITY))

	if notSoftDelete {
		qdb = qdb.Unscoped()
	}

	res := qdb.Delete(new(ENTITY))
	if res.Error != nil {
		log.Errorf("delete failed: %s", res.Error.Error())
		return 0, errors.New("delete failed")
	}
	return res.RowsAffected, nil
}

// DeleteWithFilters 接受 whereSelectors 并在内部应用到查询 DB，然后执行删除
// 示例调用：构造 selectors 后调用： `rows, err := q.DeleteWithFilters(ctx, db, whereSelectors)`
func (r *Repository[DTO, ENTITY]) DeleteWithFilters(ctx context.Context, db *gorm.DB, whereSelectors []func(*gorm.DB) *gorm.DB) (int64, error) {
	if db == nil {
		return 0, errors.New("db is nil")
	}

	qdb := db.WithContext(ctx).Model(new(ENTITY))
	for _, s := range whereSelectors {
		if s != nil {
			qdb = s(qdb)
		}
	}

	res := qdb.Delete(new(ENTITY))
	if res.Error != nil {
		log.Errorf("delete failed: %s", res.Error.Error())
		return 0, errors.New("delete failed")
	}
	return res.RowsAffected, nil
}

// SoftDelete 对符合 whereSelectors 的记录执行软删除
// whereSelectors: 应用到查询的 where scopes（按顺序）
// doSoftDeleteFunc: 可选回调，接收当前 *gorm.DB 并执行自定义更新操作（应返回执行后的 *gorm.DB）
// 当 doSoftDeleteFunc 为 nil 时，默认更新 deleted_at 字段为当前时间
func (r *Repository[DTO, ENTITY]) SoftDelete(ctx context.Context, db *gorm.DB) (int64, error) {
	return r.Delete(ctx, db, false)
}

// Exists 使用传入的 db（可包含 Where）检查是否存在记录
// 示例调用： `exists, err := q.Exists(ctx, db.Where("id = ?", id))`
func (r *Repository[DTO, ENTITY]) Exists(ctx context.Context, db *gorm.DB) (bool, error) {
	if db == nil {
		return false, errors.New("db is nil")
	}

	qdb := db.WithContext(ctx).Model(new(ENTITY)).Limit(1)

	var ent ENTITY
	if err := qdb.First(&ent).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		log.Errorf("exists query failed: %s", err.Error())
		return false, errors.New("exists query failed")
	}
	return true, nil
}

// ExistsWithFilters 接受 whereSelectors 并在内部应用到查询 DB，然后检查是否存在记录
// 示例调用：构造 selectors 后调用： `exists, err := q.ExistsWithFilters(ctx, db, whereSelectors)`
func (r *Repository[DTO, ENTITY]) ExistsWithFilters(ctx context.Context, db *gorm.DB, whereSelectors []func(*gorm.DB) *gorm.DB) (bool, error) {
	if db == nil {
		return false, errors.New("db is nil")
	}

	qdb := db.WithContext(ctx).Model(new(ENTITY)).Limit(1)
	for _, s := range whereSelectors {
		if s != nil {
			qdb = s(qdb)
		}
	}

	var ent ENTITY
	if err := qdb.First(&ent).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		log.Errorf("exists query failed: %s", err.Error())
		return false, errors.New("exists query failed")
	}
	return true, nil
}
