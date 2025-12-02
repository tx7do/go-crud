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

	paginationV1 "github.com/tx7do/go-curd/api/gen/go/pagination/v1"
	"github.com/tx7do/go-curd/entgo/field"
	"github.com/tx7do/go-curd/entgo/filter"
	paging "github.com/tx7do/go-curd/entgo/pagination"
	"github.com/tx7do/go-curd/entgo/sorting"
)

type QueryBuilder[ENTQ any, ENTS any, ENTITY any] interface {
	Modify(modifiers ...func(s *sql.Selector)) QueryBuilder[ENTQ, ENTS, ENTITY]

	Clone() QueryBuilder[ENTQ, ENTS, ENTITY]

	All(ctx context.Context) ([]*ENTITY, error)

	Only(ctx context.Context) (*ENTITY, error)

	Count(ctx context.Context) (int, error)

	Select(fields ...string) *ENTS

	Exist(ctx context.Context) (bool, error)

	Exec(ctx context.Context) error

	ExecX(ctx context.Context)

	Save(ctx context.Context) (*ENTITY, error)

	SaveX(ctx context.Context) *ENTITY
}

// Repository Ent查询器
type Repository[ENTQ any, ENTS any, DTO any, ENTITY any] struct {
	mapper *mapper.CopierMapper[DTO, ENTITY]

	queryStringSorting *sorting.QueryStringSorting
	structuredSorting  *sorting.StructuredSorting

	offsetPaginator *paging.OffsetPaginator
	pagePaginator   *paging.PagePaginator
	tokenPaginator  *paging.TokenPaginator

	queryStringFilter *filter.QueryStringFilter
	structuredFilter  *filter.StructuredFilter

	fieldSelector *field.Selector
}

func NewRepository[ENTQ any, ENTS any, DTO any, ENTITY any](mapper *mapper.CopierMapper[DTO, ENTITY]) *Repository[ENTQ, ENTS, DTO, ENTITY] {
	return &Repository[ENTQ, ENTS, DTO, ENTITY]{
		mapper: mapper,

		queryStringSorting: sorting.NewQueryStringSorting(),
		structuredSorting:  sorting.NewStructuredSorting(),

		offsetPaginator: paging.NewOffsetPaginator(),
		pagePaginator:   paging.NewPagePaginator(),
		tokenPaginator:  paging.NewTokenPaginator(),

		queryStringFilter: filter.NewQueryStringFilter(),
		structuredFilter:  filter.NewStructuredFilter(),

		fieldSelector: field.NewFieldSelector(),
	}
}

// PagingResult 是通用的分页返回结构，包含 items 和 total 字段
type PagingResult[E any] struct {
	Items []*E   `json:"items"`
	Total uint64 `json:"total"`
}

// Count 计算符合条件的记录数
func (r *Repository[ENTQ, ENTS, DTO, ENTITY]) Count(
	ctx context.Context,
	builder QueryBuilder[ENTQ, ENTS, ENTITY],
	whereCond []func(s *sql.Selector),
) (int, error) {
	if builder == nil {
		return 0, errors.New("query builder is nil")
	}

	if len(whereCond) != 0 {
		builder.Modify(whereCond...)
	}

	count, err := builder.Count(ctx)
	if err != nil {
		log.Errorf("query count failed: %s", err.Error())
		return 0, errors.New("query count failed")
	}

	return count, nil
}

// Exists 检查是否存在符合条件的记录，使用 builder.Exist 避免额外 Count 查询
func (r *Repository[ENTQ, ENTS, DTO, ENTITY]) Exists(
	ctx context.Context,
	builder QueryBuilder[ENTQ, ENTS, ENTITY],
	whereCond []func(s *sql.Selector),
) (bool, error) {
	if builder == nil {
		return false, errors.New("query builder is nil")
	}

	if len(whereCond) != 0 {
		builder.Modify(whereCond...)
	}

	exists, err := builder.Exist(ctx)
	if err != nil {
		log.Errorf("exists check failed: %s", err.Error())
		return false, errors.New("exists check failed")
	}

	return exists, nil
}

// ListWithPaging 使用分页请求查询列表
func (r *Repository[ENTQ, ENTS, DTO, ENTITY]) ListWithPaging(
	ctx context.Context,
	builder QueryBuilder[ENTQ, ENTS, ENTITY],
	req *paginationV1.PagingRequest,
) (*PagingResult[DTO], error) {
	if req == nil {
		return nil, errors.New("paging request is nil")
	}

	if builder == nil {
		return nil, errors.New("query builder is nil")
	}

	countBuilder := builder.Clone()

	var err error

	var whereSelectors []func(s *sql.Selector)
	var querySelectors []func(s *sql.Selector)
	var sortingSelector func(s *sql.Selector)
	var pagingSelector func(s *sql.Selector)
	var selectSelector func(s *sql.Selector)

	// filters
	if req.Query != nil || req.OrQuery != nil {
		whereSelectors, err = r.queryStringFilter.BuildSelectors(req.GetQuery(), req.GetOrQuery())
		if err != nil {
			log.Errorf("build query string filter selectors failed: %s", err.Error())
		}
	} else if req.FilterExpr != nil {
		whereSelectors, err = r.structuredFilter.BuildSelectors(req.GetFilterExpr())
		if err != nil {
			log.Errorf("build structured filter selectors failed: %s", err.Error())
		}
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
		sortingSelector, err = r.queryStringSorting.BuildSelector(req.GetOrderBy())
		if err != nil {
			log.Errorf("build query string sorting selector failed: %s", err.Error())
		}
	}
	if sortingSelector != nil {
		querySelectors = append(querySelectors, sortingSelector)
	}

	// paginationV1
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

	if querySelectors != nil {
		builder.Modify(querySelectors...)
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

	count, err := r.Count(ctx, countBuilder, whereSelectors)
	if err != nil {
		log.Errorf("count query failed: %s", err.Error())
		return nil, err
	}

	res := &PagingResult[DTO]{
		Items: dtos,
		Total: uint64(count),
	}

	return res, nil
}

// ListWithPagination 使用通用的分页请求参数进行列表查询
func (r *Repository[ENTQ, ENTS, DTO, ENTITY]) ListWithPagination(
	ctx context.Context,
	builder QueryBuilder[ENTQ, ENTS, ENTITY],
	req *paginationV1.PaginationRequest,
) (*PagingResult[DTO], error) {
	if req == nil {
		return nil, errors.New("paginationV1 request is nil")
	}

	if builder == nil {
		return nil, errors.New("query builder is nil")
	}

	countBuilder := builder.Clone()

	var err error

	var whereSelectors []func(s *sql.Selector)
	var querySelectors []func(s *sql.Selector)
	var sortingSelector func(s *sql.Selector)
	var pagingSelector func(s *sql.Selector)
	var selectSelector func(s *sql.Selector)

	// filters
	if req.Query != nil || req.OrQuery != nil {
		whereSelectors, err = r.queryStringFilter.BuildSelectors(req.GetQuery(), req.GetOrQuery())
		if err != nil {
			log.Errorf("build query string filter selectors failed: %s", err.Error())
		}
	} else if req.FilterExpr != nil {
		whereSelectors, err = r.structuredFilter.BuildSelectors(req.GetFilterExpr())
		if err != nil {
			log.Errorf("build structured filter selectors failed: %s", err.Error())
		}
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
		sortingSelector, err = r.queryStringSorting.BuildSelector(req.GetOrderBy())
		if err != nil {
			log.Errorf("build query string sorting selector failed: %s", err.Error())
		}
	}
	if sortingSelector != nil {
		querySelectors = append(querySelectors, sortingSelector)
	}

	// paginationV1
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

	if querySelectors != nil {
		builder.Modify(querySelectors...)
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

	count, err := r.Count(ctx, countBuilder, whereSelectors)
	if err != nil {
		log.Errorf("count query failed: %s", err.Error())
		return nil, err
	}

	res := &PagingResult[DTO]{
		Items: dtos,
		Total: uint64(count),
	}

	return res, nil
}

// Get 根据查询条件获取单条记录
func (r *Repository[ENTQ, ENTS, DTO, ENTITY]) Get(
	ctx context.Context,
	builder QueryBuilder[ENTQ, ENTS, ENTITY],
	whereCond []func(s *sql.Selector),
	viewMask *fieldmaskpb.FieldMask,
) (*DTO, error) {
	if builder == nil {
		return nil, errors.New("query builder is nil")
	}

	if len(whereCond) != 0 {
		builder.Modify(whereCond...)
	}

	field.NormalizeFieldMaskPaths(viewMask)

	if viewMask != nil && len(viewMask.Paths) == 0 {
		builder.Select(viewMask.GetPaths()...)
	}

	entity, err := builder.Only(ctx)
	if err != nil {
		return nil, err
	}

	return r.mapper.ToDTO(entity), nil
}

// Only 根据查询条件获取单条记录
func (r *Repository[ENTQ, ENTS, DTO, ENTITY]) Only(
	ctx context.Context,
	builder QueryBuilder[ENTQ, ENTS, ENTITY],
	whereCond []func(s *sql.Selector),
	viewMask *fieldmaskpb.FieldMask,
) (*DTO, error) {
	return r.Get(ctx, builder, whereCond, viewMask)
}

// Create 根据 DTO 创建一条记录，返回创建后的 DTO
func (r *Repository[ENTQ, ENTS, DTO, ENTITY]) Create(
	ctx context.Context,
	builder QueryBuilder[ENTQ, ENTS, ENTITY],
	dto *DTO,
	createMask *fieldmaskpb.FieldMask,
	doCreateFieldFunc func(builder QueryBuilder[ENTQ, ENTS, ENTITY], dto *DTO),
) (*DTO, error) {
	if builder == nil {
		return nil, errors.New("query builder is nil")
	}

	if dto == nil {
		return nil, errors.New("dto is nil")
	}

	var dtoAny any = dto
	var dtoProto = dtoAny.(proto.Message)
	if err := fieldmaskutil.FilterByFieldMask(trans.Ptr(dtoProto), createMask); err != nil {
		log.Errorf("invalid field mask [%v], error: %s", createMask, err.Error())
		return nil, err
	}

	if doCreateFieldFunc != nil {
		doCreateFieldFunc(builder, dto)
	}

	entity, err := builder.Save(ctx)
	if err != nil {
		log.Errorf("create data failed: %s", err.Error())
		return nil, err
	}

	return r.mapper.ToDTO(entity), nil
}

// CreateX 仅执行创建操作，不返回创建后的数据
func (r *Repository[ENTQ, ENTS, DTO, ENTITY]) CreateX(
	ctx context.Context,
	builder QueryBuilder[ENTQ, ENTS, ENTITY],
	dto *DTO,
	createMask *fieldmaskpb.FieldMask,
	doCreateFieldFunc func(builder QueryBuilder[ENTQ, ENTS, ENTITY], dto *DTO),
) error {
	if builder == nil {
		return errors.New("query builder is nil")
	}

	if dto == nil {
		return errors.New("dto is nil")
	}

	var dtoAny any = dto
	var dtoProto = dtoAny.(proto.Message)
	if err := fieldmaskutil.FilterByFieldMask(trans.Ptr(dtoProto), createMask); err != nil {
		log.Errorf("invalid field mask [%v], error: %s", createMask, err.Error())
		return err
	}

	if doCreateFieldFunc != nil {
		doCreateFieldFunc(builder, dto)
	}

	if _, err := builder.Save(ctx); err != nil {
		log.Errorf("create data failed: %s", err.Error())
		return err
	}

	return nil
}

// BatchCreate 批量创建记录，返回创建后的 DTO 列表
func (r *Repository[ENTQ, ENTS, DTO, ENTITY]) BatchCreate(
	ctx context.Context,
	builder QueryBuilder[ENTQ, ENTS, ENTITY],
	dtos []*DTO,
	createMask *fieldmaskpb.FieldMask,
	doCreateFieldFunc func(b QueryBuilder[ENTQ, ENTS, ENTITY], dto *DTO),
) ([]*DTO, error) {
	if builder == nil {
		return nil, errors.New("query builder is nil")
	}
	if len(dtos) == 0 {
		return nil, nil
	}

	res := make([]*DTO, 0, len(dtos))
	for _, dto := range dtos {
		var dtoAny any = dto
		var dtoProto = dtoAny.(proto.Message)
		if err := fieldmaskutil.FilterByFieldMask(trans.Ptr(dtoProto), createMask); err != nil {
			log.Errorf("invalid field mask [%v], error: %s", createMask, err.Error())
			return nil, err
		}

		b := builder.Clone()
		if doCreateFieldFunc != nil {
			doCreateFieldFunc(b, dto)
		}

		entity, err := b.Save(ctx)
		if err != nil {
			log.Errorf("batch create failed: %s", err.Error())
			return nil, err
		}

		res = append(res, r.mapper.ToDTO(entity))
	}

	return res, nil
}

// Update 根据实体更新数据
func (r *Repository[ENTQ, ENTS, DTO, ENTITY]) Update(
	ctx context.Context,
	builder QueryBuilder[ENTQ, ENTS, ENTITY],
	dto *DTO,
	updateMask *fieldmaskpb.FieldMask,
	doUpdateFieldFunc func(builder QueryBuilder[ENTQ, ENTS, ENTITY], dto *DTO),
) (*DTO, error) {
	if builder == nil {
		return nil, errors.New("query builder is nil")
	}

	if dto == nil {
		return nil, errors.New("dto is nil")
	}

	var dtoAny any = dto
	var dtoProto = dtoAny.(proto.Message)
	if err := fieldmaskutil.FilterByFieldMask(trans.Ptr(dtoProto), updateMask); err != nil {
		log.Errorf("invalid field mask [%v], error: %s", updateMask, err.Error())
		return nil, err
	}

	doUpdateFieldFunc(builder, dto)

	var err error
	var entity *ENTITY
	if entity, err = builder.Save(ctx); err != nil {
		log.Errorf("update one data failed: %s", err.Error())
		return nil, err
	}

	return r.mapper.ToDTO(entity), nil
}

// UpdateX 仅执行更新操作，不返回更新后的数据
func (r *Repository[ENTQ, ENTS, DTO, ENTITY]) UpdateX(
	ctx context.Context,
	builder QueryBuilder[ENTQ, ENTS, ENTITY],
	dto *DTO,
	updateMask *fieldmaskpb.FieldMask,
	doUpdateFieldFunc func(builder QueryBuilder[ENTQ, ENTS, ENTITY], dto *DTO),
) error {
	if builder == nil {
		return errors.New("query builder is nil")
	}

	if dto == nil {
		return errors.New("dto is nil")
	}

	var dtoAny any = dto
	var dtoProto = dtoAny.(proto.Message)
	if err := fieldmaskutil.FilterByFieldMask(trans.Ptr(dtoProto), updateMask); err != nil {
		log.Errorf("invalid field mask [%v], error: %s", updateMask, err.Error())
		return err
	}

	doUpdateFieldFunc(builder, dto)

	if err := builder.Exec(ctx); err != nil {
		log.Errorf("update one data failed: %s", err.Error())
		return err
	}

	return nil
}

// Delete 根据查询条件删除记录
func (r *Repository[ENTQ, ENTS, DTO, ENTITY]) Delete(
	ctx context.Context,
	builder QueryBuilder[ENTQ, ENTS, ENTITY],
	whereCond []func(s *sql.Selector),
) error {
	if builder == nil {
		return errors.New("query builder is nil")
	}

	if len(whereCond) != 0 {
		builder.Modify(whereCond...)
	}

	if err := builder.Exec(ctx); err != nil {
		log.Errorf("delete failed: %s", err.Error())
		return errors.New("delete failed")
	}

	return nil
}
