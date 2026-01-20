package entgo

import (
	"context"
	"errors"

	"entgo.io/ent/dialect/sql"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/go-utils/fieldmaskutil"
	"github.com/tx7do/go-utils/mapper"
	"github.com/tx7do/go-utils/trans"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/entgo/field"
	"github.com/tx7do/go-crud/entgo/filter"
	paging "github.com/tx7do/go-crud/entgo/pagination"
	"github.com/tx7do/go-crud/entgo/sorting"
	"github.com/tx7do/go-crud/entgo/update"
	paginationFilter "github.com/tx7do/go-crud/pagination/filter"
	paginationSorting "github.com/tx7do/go-crud/pagination/sorting"
)

// Repository Ent查询器
type Repository[
	ENT_QUERY any, ENT_SELECT any,
	ENT_CREATE any, ENT_CREATE_BULK any,
	ENT_UPDATE any, ENT_UPDATE_ONE any,
	ENT_DELETE any,
	PREDICATE any, DTO any, ENTITY any,
] struct {
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

func NewRepository[
	ENT_QUERY any, ENT_SELECT any,
	ENT_CREATE any, ENT_CREATE_BULK any,
	ENT_UPDATE any, ENT_UPDATE_ONE any,
	ENT_DELETE any,
	PREDICATE any, DTO any, ENTITY any,
](mapper *mapper.CopierMapper[DTO, ENTITY]) *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
] {
	return &Repository[
		ENT_QUERY, ENT_SELECT,
		ENT_CREATE, ENT_CREATE_BULK,
		ENT_UPDATE, ENT_UPDATE_ONE,
		ENT_DELETE,
		PREDICATE, DTO, ENTITY,
	]{
		mapper: mapper,

		offsetPaginator: paging.NewOffsetPaginator(),
		pagePaginator:   paging.NewPagePaginator(),
		tokenPaginator:  paging.NewTokenPaginator(),

		fieldSelector: field.NewFieldSelector(),

		structuredFilter:      filter.NewStructuredFilter(),
		queryStringConverter:  paginationFilter.NewQueryStringConverter(),
		filterStringConverter: paginationFilter.NewFilterStringConverter(),

		structuredSorting:      sorting.NewStructuredSorting(),
		orderByStringConverter: paginationSorting.NewOrderByStringConverter(),
	}
}

// PagingResult 是通用的分页返回结构，包含 items 和 total 字段
type PagingResult[E any] struct {
	Items []*E   `json:"items"`
	Total uint64 `json:"total"`
}

// Count 计算符合条件的记录数
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) Count(
	ctx context.Context,
	builder QueryBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	predicates ...func(s *sql.Selector),
) (int, error) {
	if builder == nil {
		return 0, errors.New("query builder is nil")
	}

	if len(predicates) > 0 {
		builder.Modify(predicates...)
	}

	count, err := builder.Count(ctx)
	if err != nil {
		log.Errorf("query count failed: %s", err.Error())
		return 0, errors.New("query count failed")
	}

	return count, nil
}

// Exists 检查是否存在符合条件的记录，使用 builder.Exist 避免额外 Count 查询
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) Exists(
	ctx context.Context,
	builder QueryBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	predicates ...func(s *sql.Selector),
) (bool, error) {
	if builder == nil {
		return false, errors.New("query builder is nil")
	}

	if len(predicates) > 0 {
		builder.Modify(predicates...)
	}

	exists, err := builder.Exist(ctx)
	if err != nil {
		log.Errorf("exists check failed: %s", err.Error())
		return false, errors.New("exists check failed")
	}

	return exists, nil
}

// ListWithPaging 使用分页请求查询列表
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) ListWithPaging(
	ctx context.Context,
	builder ListBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	countBuilder ListBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	req *paginationV1.PagingRequest,
) (*PagingResult[DTO], error) {
	if req == nil {
		return nil, errors.New("paging request is nil")
	}

	if builder == nil {
		return nil, errors.New("query builder is nil")
	}

	whereSelectors, _, err := r.BuildListSelectorWithPaging(builder, req)
	if err != nil {
		return nil, err
	}

	entities, err := builder.All(ctx)
	if err != nil {
		log.Errorf("query list failed: %s", err.Error())
		return nil, errors.New("query list failed")
	}

	dtos := make([]*DTO, 0, len(entities))
	for _, entity := range entities {
		dto := r.mapper.ToDTO(entity)
		dtos = append(dtos, dto)
	}

	var count int
	if countBuilder != nil {
		if len(whereSelectors) != 0 {
			countBuilder.Modify(whereSelectors...)
		}
		count, err = countBuilder.Count(ctx)
		if err != nil {
			log.Errorf("query count failed: %s", err.Error())
			return nil, errors.New("query count failed")
		}
	}

	res := &PagingResult[DTO]{
		Items: dtos,
		Total: uint64(count),
	}

	return res, nil
}

// ListTreeWithPaging 使用分页请求查询树形结构列表
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) ListTreeWithPaging(
	ctx context.Context,
	builder ListBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	countBuilder ListBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	req *paginationV1.PagingRequest,
) (*PagingResult[DTO], error) {
	if req == nil {
		return nil, errors.New("paging request is nil")
	}

	if builder == nil {
		return nil, errors.New("query builder is nil")
	}

	whereSelectors, _, err := r.BuildListSelectorWithPaging(builder, req)
	if err != nil {
		return nil, err
	}

	entities, err := builder.All(ctx)
	if err != nil {
		log.Errorf("query list failed: %s", err.Error())
		return nil, errors.New("query list failed")
	}

	// 先把所有 ENTITY 映射为 DTO 列表
	allDTOs := make([]*DTO, 0, len(entities))
	for _, entity := range entities {
		dto := r.mapper.ToDTO(entity)
		allDTOs = append(allDTOs, dto)
	}

	// 建立 id->dto 映射（基于 DTO 的 ID 字段，支持 ID/Id 字段名且为 string 或 *string）
	idMap := make(map[string]*DTO, len(allDTOs))
	for _, dto := range allDTOs {
		if id, ok := getStringField(dto, []string{"ID", "Id"}); ok {
			idMap[id] = dto
		}
	}

	roots := make([]*DTO, 0, len(allDTOs))
	// 遍历 DTO，将子追加到父的 Children 字段（反射追加），找不到父则作为根
	for _, dto := range allDTOs {
		parentID, hasParent := getStringField(dto, []string{"ParentID", "ParentId"})
		if !hasParent || parentID == "" {
			roots = append(roots, dto)
			continue
		}
		if parent, found := idMap[parentID]; found {
			if ok := appendChild(parent, dto); ok {
				continue
			}
			// 如果无法追加到父的 Children 字段，则退回到根列表
			roots = append(roots, dto)
			continue
		}
		// 父不存在于当前集合，则视为根节点
		roots = append(roots, dto)
	}

	var count int
	if countBuilder != nil {
		if len(whereSelectors) != 0 {
			countBuilder.Modify(whereSelectors...)
		}
		count, err = countBuilder.Count(ctx)
		if err != nil {
			log.Errorf("query count failed: %s", err.Error())
			return nil, errors.New("query count failed")
		}
	}

	res := &PagingResult[DTO]{
		Items: roots,
		Total: uint64(count),
	}

	return res, nil
}

// BuildListSelectorWithPaging 使用分页请求查询列表
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) BuildListSelectorWithPaging(
	builder ListBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	req *paginationV1.PagingRequest,
) (whereSelectors []func(s *sql.Selector), querySelectors []func(s *sql.Selector), err error) {
	if req == nil {
		return nil, nil, errors.New("paging request is nil")
	}

	if builder == nil {
		return nil, nil, errors.New("query builder is nil")
	}

	var sortingSelector func(s *sql.Selector)
	var pagingSelector func(s *sql.Selector)
	var selectSelector func(s *sql.Selector)

	// filters
	filterExpr, err := r.ConvertFilterByPagingRequest(req)
	if err != nil {
		log.Errorf("convert filter by pagination request failed: %s", err.Error())
	}
	whereSelectors, err = r.structuredFilter.BuildSelectors(filterExpr)
	if err != nil {
		log.Errorf("build structured filter selectors failed: %s", err.Error())
	}

	if whereSelectors != nil {
		querySelectors = append(querySelectors, whereSelectors...)
	}

	// select fields
	if req.FieldMask != nil && len(req.GetFieldMask().Paths) > 0 {
		selectSelector, err = r.fieldSelector.BuildSelector(req.GetFieldMask().GetPaths())
		if err != nil {
			log.Errorf("build field select selector failed: %s", err.Error())
		}
	}
	if selectSelector != nil {
		querySelectors = append(querySelectors, selectSelector)
	}

	// order by
	if len(req.GetSorting()) > 0 {
		sortingSelector, err = r.structuredSorting.BuildSelector(req.GetSorting())
		if err != nil {
			log.Errorf("build structured sorting selector failed: %s", err.Error())
		}
	} else if len(req.GetOrderBy()) > 0 {
		var sortings []*paginationV1.Sorting
		sortings, err = r.orderByStringConverter.Convert(req.GetOrderBy())
		if err != nil {
			log.Errorf("convert order by string to sorting failed: %s", err.Error())
			return nil, nil, err
		}

		sortingSelector, err = r.structuredSorting.BuildSelector(sortings)
		if err != nil {
			log.Errorf("build query string sorting selector failed: %s", err.Error())
		}
	}
	if sortingSelector != nil {
		querySelectors = append(querySelectors, sortingSelector)
	}

	// pagination
	if !req.GetNoPaging() {
		if req.Page != nil && req.PageSize != nil {
			pagingSelector = r.pagePaginator.BuildSelector(int(req.GetPage()), int(req.GetPageSize()))
		} else if req.Offset != nil && req.Limit != nil {
			pagingSelector = r.offsetPaginator.BuildSelector(int(req.GetOffset()), int(req.GetLimit()))
		} else if req.Token != nil && req.Offset != nil {
			pagingSelector = r.tokenPaginator.BuildSelector(req.GetToken(), int(req.GetOffset()))
		}
	}
	if pagingSelector != nil {
		querySelectors = append(querySelectors, pagingSelector)
	}

	if len(querySelectors) != 0 {
		builder.Modify(querySelectors...)
	}

	return whereSelectors, querySelectors, nil
}

// ListWithPagination 使用通用的分页请求参数进行列表查询
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) ListWithPagination(
	ctx context.Context,
	builder ListBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	countBuilder ListBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	req *paginationV1.PaginationRequest,
) (*PagingResult[DTO], error) {
	if req == nil {
		return nil, errors.New("paginationV1 request is nil")
	}

	if builder == nil {
		return nil, errors.New("query builder is nil")
	}

	whereSelectors, _, err := r.BuildListSelectorWithPagination(builder, req)
	if err != nil {
		return nil, err
	}

	entities, err := builder.All(ctx)
	if err != nil {
		log.Errorf("query list failed: %s", err.Error())
		return nil, errors.New("query list failed")
	}

	dtos := make([]*DTO, 0, len(entities))
	for _, entity := range entities {
		dto := r.mapper.ToDTO(entity)
		dtos = append(dtos, dto)
	}

	var count int
	if countBuilder != nil {
		if len(whereSelectors) != 0 {
			countBuilder.Modify(whereSelectors...)
		}
		count, err = countBuilder.Count(ctx)
		if err != nil {
			log.Errorf("query count failed: %s", err.Error())
			return nil, errors.New("query count failed")
		}
	}

	res := &PagingResult[DTO]{
		Items: dtos,
		Total: uint64(count),
	}

	return res, nil
}

// ListTreeWithPagination 使用通用的分页请求参数进行树形结构列表查询
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) ListTreeWithPagination(
	ctx context.Context,
	builder ListBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	countBuilder ListBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	req *paginationV1.PaginationRequest,
) (*PagingResult[DTO], error) {
	if req == nil {
		return nil, errors.New("paging request is nil")
	}

	if builder == nil {
		return nil, errors.New("query builder is nil")
	}

	whereSelectors, _, err := r.BuildListSelectorWithPagination(builder, req)
	if err != nil {
		return nil, err
	}

	entities, err := builder.All(ctx)
	if err != nil {
		log.Errorf("query list failed: %s", err.Error())
		return nil, errors.New("query list failed")
	}

	// 先把所有 ENTITY 映射为 DTO 列表
	allDTOs := make([]*DTO, 0, len(entities))
	for _, entity := range entities {
		dto := r.mapper.ToDTO(entity)
		allDTOs = append(allDTOs, dto)
	}

	// 建立 id->dto 映射（基于 DTO 的 ID 字段，支持 ID/Id 字段名且为 string 或 *string）
	idMap := make(map[string]*DTO, len(allDTOs))
	for _, dto := range allDTOs {
		if id, ok := getStringField(dto, []string{"ID", "Id"}); ok {
			idMap[id] = dto
		}
	}

	roots := make([]*DTO, 0, len(allDTOs))
	// 遍历 DTO，将子追加到父的 Children 字段（反射追加），找不到父则作为根
	for _, dto := range allDTOs {
		parentID, hasParent := getStringField(dto, []string{"ParentID", "ParentId"})
		if !hasParent || parentID == "" {
			roots = append(roots, dto)
			continue
		}
		if parent, found := idMap[parentID]; found {
			if ok := appendChild(parent, dto); ok {
				continue
			}
			// 如果无法追加到父的 Children 字段，则退回到根列表
			roots = append(roots, dto)
			continue
		}
		// 父不存在于当前集合，则视为根节点
		roots = append(roots, dto)
	}

	var count int
	if countBuilder != nil {
		if len(whereSelectors) != 0 {
			countBuilder.Modify(whereSelectors...)
		}
		count, err = countBuilder.Count(ctx)
		if err != nil {
			log.Errorf("query count failed: %s", err.Error())
			return nil, errors.New("query count failed")
		}
	}

	res := &PagingResult[DTO]{
		Items: roots,
		Total: uint64(count),
	}

	return res, nil
}

// BuildListSelectorWithPagination 使用分页请求查询列表
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) BuildListSelectorWithPagination(
	builder ListBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	req *paginationV1.PaginationRequest,
) (whereSelectors []func(s *sql.Selector), querySelectors []func(s *sql.Selector), err error) {
	if req == nil {
		return nil, nil, errors.New("paginationV1 request is nil")
	}

	if builder == nil {
		return nil, nil, errors.New("query builder is nil")
	}

	var sortingSelector func(s *sql.Selector)
	var pagingSelector func(s *sql.Selector)
	var selectSelector func(s *sql.Selector)

	// filters
	filterExpr, err := r.ConvertFilterByPaginationRequest(req)
	if err != nil {
		log.Errorf("convert filter by pagination request failed: %s", err.Error())
	}
	whereSelectors, err = r.structuredFilter.BuildSelectors(filterExpr)
	if err != nil {
		log.Errorf("build structured filter selectors failed: %s", err.Error())
	}

	// select fields
	if req.FieldMask != nil && len(req.GetFieldMask().Paths) > 0 {
		selectSelector, err = r.fieldSelector.BuildSelector(req.GetFieldMask().GetPaths())
		if err != nil {
			log.Errorf("build field select selector failed: %s", err.Error())
		}
	}
	if selectSelector != nil {
		querySelectors = append(querySelectors, selectSelector)
	}

	// order by
	if len(req.GetSorting()) > 0 {
		sortingSelector, err = r.structuredSorting.BuildSelector(req.GetSorting())
		if err != nil {
			log.Errorf("build structured sorting selector failed: %s", err.Error())
		}
	} else if len(req.GetOrderBy()) > 0 {
		var sortings []*paginationV1.Sorting
		sortings, err = r.orderByStringConverter.Convert(req.GetOrderBy())
		if err != nil {
			log.Errorf("convert order by string to sorting failed: %s", err.Error())
			return nil, nil, err
		}

		sortingSelector, err = r.structuredSorting.BuildSelector(sortings)
		if err != nil {
			log.Errorf("build query string sorting selector failed: %s", err.Error())
		}
	}
	if sortingSelector != nil {
		querySelectors = append(querySelectors, sortingSelector)
	}

	// pagination
	switch req.GetPaginationType().(type) {
	case *paginationV1.PaginationRequest_OffsetBased:
		pagingSelector = r.offsetPaginator.BuildSelector(int(req.GetOffsetBased().GetOffset()), int(req.GetOffsetBased().GetLimit()))
	case *paginationV1.PaginationRequest_PageBased:
		pagingSelector = r.pagePaginator.BuildSelector(int(req.GetPageBased().GetPage()), int(req.GetPageBased().GetPageSize()))
	case *paginationV1.PaginationRequest_TokenBased:
		pagingSelector = r.tokenPaginator.BuildSelector(req.GetTokenBased().GetToken(), int(req.GetTokenBased().GetPageSize()))
	}
	if pagingSelector != nil {
		querySelectors = append(querySelectors, pagingSelector)
	}

	if len(querySelectors) != 0 {
		builder.Modify(querySelectors...)
	}

	return whereSelectors, querySelectors, nil
}

// Get 根据查询条件获取单条记录
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) Get(
	ctx context.Context,
	builder QueryBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	viewMask *fieldmaskpb.FieldMask,
	predicates ...func(s *sql.Selector),
) (*DTO, error) {
	if builder == nil {
		return nil, errors.New("query builder is nil")
	}

	if len(predicates) > 0 {
		builder.Modify(predicates...)
	}

	field.NormalizeFieldMaskPaths(viewMask)

	if viewMask != nil && len(viewMask.Paths) > 0 {
		builder.Select(viewMask.GetPaths()...)
	}

	entity, err := builder.Only(ctx)
	if err != nil {
		return nil, err
	}

	return r.mapper.ToDTO(entity), nil
}

// Only 根据查询条件获取单条记录
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) Only(
	ctx context.Context,
	builder QueryBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	viewMask *fieldmaskpb.FieldMask,
	predicates ...func(s *sql.Selector),
) (*DTO, error) {
	return r.Get(ctx, builder, viewMask, predicates...)
}

// Create 根据 DTO 创建一条记录，返回创建后的 DTO
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) Create(
	ctx context.Context,
	builder CreateBuilder[ENTITY],
	dto *DTO,
	createMask *fieldmaskpb.FieldMask,
	doCreateFieldFunc func(dto *DTO),
) (*DTO, error) {
	if builder == nil {
		return nil, errors.New("query builder is nil")
	}

	if dto == nil {
		return nil, errors.New("dto is nil")
	}

	field.NormalizeFieldMaskPaths(createMask)

	var dtoAny any = dto
	var dtoProto = dtoAny.(proto.Message)
	if dtoProto == nil {
		return nil, errors.New("dto proto message is nil")
	}
	if err := fieldmaskutil.FilterByFieldMask(trans.Ptr(dtoProto), createMask); err != nil {
		log.Errorf("invalid field mask [%v], error: %s", createMask, err.Error())
		return nil, err
	}

	if doCreateFieldFunc != nil {
		doCreateFieldFunc(dto)
	}

	entity, err := builder.Save(ctx)
	if err != nil {
		log.Errorf("create data failed: %s", err.Error())
		return nil, err
	}

	return r.mapper.ToDTO(entity), nil
}

// CreateX 仅执行创建操作，不返回创建后的数据
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) CreateX(
	ctx context.Context,
	builder CreateBuilder[ENTITY],
	dto *DTO,
	createMask *fieldmaskpb.FieldMask,
	doCreateFieldFunc func(dto *DTO),
) error {
	if builder == nil {
		return errors.New("query builder is nil")
	}

	if dto == nil {
		return errors.New("dto is nil")
	}

	field.NormalizeFieldMaskPaths(createMask)

	var dtoAny any = dto
	var dtoProto = dtoAny.(proto.Message)
	if dtoProto == nil {
		return errors.New("dto proto message is nil")
	}
	if err := fieldmaskutil.FilterByFieldMask(trans.Ptr(dtoProto), createMask); err != nil {
		log.Errorf("invalid field mask [%v], error: %s", createMask, err.Error())
		return err
	}

	if doCreateFieldFunc != nil {
		doCreateFieldFunc(dto)
	}

	if err := builder.Exec(ctx); err != nil {
		log.Errorf("create data failed: %s", err.Error())
		return err
	}

	return nil
}

// BatchCreate 批量创建记录，返回创建后的 DTO 列表
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) BatchCreate(
	ctx context.Context,
	builder CreateBulkBuilder[ENT_CREATE_BULK, ENTITY],
	dtos []*DTO,
	createMask *fieldmaskpb.FieldMask,
	doCreateFieldFunc func(dto *DTO),
) ([]*DTO, error) {
	if builder == nil {
		return nil, errors.New("query builder is nil")
	}
	if len(dtos) == 0 {
		return nil, errors.New("dtos is empty")
	}

	field.NormalizeFieldMaskPaths(createMask)

	ents := make([]*ENTITY, 0, len(dtos))
	for _, dto := range dtos {
		if dto == nil {
			continue
		}
		var dtoAny any = dto
		dtoProto := dtoAny.(proto.Message)
		if dtoProto == nil {
			continue
		}

		if err := fieldmaskutil.FilterByFieldMask(trans.Ptr(dtoProto), createMask); err != nil {
			log.Errorf("invalid field mask [%v], error: %s", createMask, err.Error())
			return nil, err
		}
		// 将 DTO 映射为 ENTITY（依赖 mapper 提供 ToEntity）
		ent := r.mapper.ToEntity(dto)
		ents = append(ents, ent)
	}

	createdEnts, err := builder.Save(ctx)
	if err != nil {
		log.Errorf("bulk create failed: %s", err.Error())
		return nil, err
	}

	res := make([]*DTO, 0, len(createdEnts))
	for _, e := range createdEnts {
		res = append(res, r.mapper.ToDTO(e))
	}
	return res, nil
}

// UpdateOne 根据查询条件更新单条记录，返回更新后的 DTO
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) UpdateOne(
	ctx context.Context,
	builder UpdateOneBuilder[ENT_UPDATE_ONE, PREDICATE, ENTITY],
	dto *DTO,
	updateMask *fieldmaskpb.FieldMask,
	doUpdateFieldFunc func(dto *DTO),
	predicates ...PREDICATE,
) (*DTO, error) {
	if builder == nil {
		return nil, errors.New("query builder is nil")
	}

	if dto == nil {
		return nil, errors.New("dto is nil")
	}

	if len(predicates) > 0 {
		builder.Where(predicates...)
	}

	field.NormalizeFieldMaskPaths(updateMask)

	var dtoAny any = dto
	var dtoProto = dtoAny.(proto.Message)
	if dtoProto == nil {
		return nil, errors.New("dto proto message is nil")
	}
	if err := fieldmaskutil.FilterByFieldMask(trans.Ptr(dtoProto), updateMask); err != nil {
		log.Errorf("invalid field mask [%v], error: %s", updateMask, err.Error())
		return nil, err
	}

	if doUpdateFieldFunc != nil {
		doUpdateFieldFunc(dto)
	}

	r.applyUpdateOneNilFieldMask(dtoProto, updateMask, builder)

	var err error
	var entity *ENTITY
	if entity, err = builder.Save(ctx); err != nil {
		log.Errorf("update one data failed: %s", err.Error())
		return nil, err
	}

	return r.mapper.ToDTO(entity), nil
}

// applyUpdateOneNilFieldMask 应用字段掩码以设置字段为NULL
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) applyUpdateOneNilFieldMask(
	msg proto.Message,
	updateMask *fieldmaskpb.FieldMask,
	builder UpdateOneBuilder[ENT_UPDATE_ONE, PREDICATE, ENTITY],
) {
	if msg == nil {
		return
	}
	if updateMask == nil {
		return
	}

	nilPaths := fieldmaskutil.NilValuePaths(msg, updateMask.GetPaths())
	nilUpdater := update.BuildSetNullUpdater(nilPaths)
	if nilUpdater != nil {
		if builder != nil {
			builder.Modify(nilUpdater)
		}
	}
}

// applyUpdateNilFieldMask 应用字段掩码以设置字段为NULL
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) applyUpdateNilFieldMask(
	msg proto.Message,
	updateMask *fieldmaskpb.FieldMask,
	builder UpdateBuilder[ENT_UPDATE, PREDICATE],
) {
	if msg == nil {
		return
	}
	if updateMask == nil {
		return
	}

	nilPaths := fieldmaskutil.NilValuePaths(msg, updateMask.GetPaths())
	nilUpdater := update.BuildSetNullUpdater(nilPaths)
	if nilUpdater != nil {
		if builder != nil {
			builder.Modify(nilUpdater)
		}
	}
}

// UpdateX 仅执行更新操作，不返回更新后的数据
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) UpdateX(
	ctx context.Context,
	builder UpdateBuilder[ENT_UPDATE, PREDICATE],
	dto *DTO,
	updateMask *fieldmaskpb.FieldMask,
	doUpdateFieldFunc func(dto *DTO),
	predicates ...PREDICATE,
) error {
	if builder == nil {
		return errors.New("query builder is nil")
	}

	if dto == nil {
		return errors.New("dto is nil")
	}

	if len(predicates) > 0 {
		builder.Where(predicates...)
	}

	field.NormalizeFieldMaskPaths(updateMask)

	var dtoAny any = dto
	var dtoProto = dtoAny.(proto.Message)
	if dtoProto == nil {
		return errors.New("dto proto message is nil")
	}
	if err := fieldmaskutil.FilterByFieldMask(trans.Ptr(dtoProto), updateMask); err != nil {
		log.Errorf("invalid field mask [%v], error: %s", updateMask, err.Error())
		return err
	}

	if doUpdateFieldFunc != nil {
		doUpdateFieldFunc(dto)
	}

	r.applyUpdateNilFieldMask(dtoProto, updateMask, builder)

	if err := builder.Exec(ctx); err != nil {
		log.Errorf("update one data failed: %s", err.Error())
		return err
	}

	return nil
}

// Delete 根据查询条件删除记录
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) Delete(
	ctx context.Context,
	builder DeleteBuilder[ENT_DELETE, PREDICATE],
	predicates ...PREDICATE,
) (int, error) {
	if builder == nil {
		return 0, errors.New("query builder is nil")
	}

	if len(predicates) > 0 {
		builder.Where(predicates...)
	}

	var affected int
	var err error
	if affected, err = builder.Exec(ctx); err != nil {
		log.Errorf("delete failed: %s", err.Error())
		return 0, errors.New("delete failed")
	}

	return affected, nil
}

// ConvertFilterByPagingRequest 将通用分页请求中的过滤条件转换为结构化表达式
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) ConvertFilterByPagingRequest(
	req *paginationV1.PagingRequest,
) (*paginationV1.FilterExpr, error) {
	if req == nil {
		return nil, nil
	}

	// 已有结构化表达式，直接返回
	if req.GetFilterExpr() != nil {
		return req.GetFilterExpr(), nil
	}

	// 优先使用 query/or_query 转换
	if req.GetQuery() != "" {
		return r.queryStringConverter.Convert(req.GetQuery())
	}

	// 最后使用 filter 字符串转换
	if req.GetFilter() != "" {
		return r.filterStringConverter.Convert(req.GetFilter())
	}

	return nil, nil
}

// ConvertFilterByPaginationRequest 使用通用的分页请求参数转换过滤表达式
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) ConvertFilterByPaginationRequest(
	req *paginationV1.PaginationRequest,
) (*paginationV1.FilterExpr, error) {
	if req == nil {
		return nil, nil
	}

	// 已有结构化表达式，直接返回
	if req.GetFilterExpr() != nil {
		return req.GetFilterExpr(), nil
	}

	// 优先使用 query/or_query 转换
	if req.GetQuery() != "" {
		return r.queryStringConverter.Convert(req.GetQuery())
	}

	// 最后使用 filter 字符串转换
	if req.GetFilter() != "" {
		return r.filterStringConverter.Convert(req.GetFilter())
	}

	return nil, nil
}
