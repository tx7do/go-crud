package gorm

import (
	"context"
	"errors"

	"gorm.io/gorm"

	"github.com/go-kratos/kratos/v2/log"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/tx7do/go-utils/mapper"

	paginationV1 "github.com/tx7do/go-curd/api/gen/go/pagination/v1"
	"github.com/tx7do/go-curd/gorm/field"
	"github.com/tx7do/go-curd/gorm/filter"
	paging "github.com/tx7do/go-curd/gorm/pagination"
	"github.com/tx7do/go-curd/gorm/sorting"
)

// Querier GORM 查询器（不再依赖 QueryBuilder）
type Querier[DTO any, ENTITY any] struct {
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

func NewQuerier[DTO any, ENTITY any](mapper *mapper.CopierMapper[DTO, ENTITY]) *Querier[DTO, ENTITY] {
	return &Querier[DTO, ENTITY]{
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

// PagingResult 通用分页返回
type PagingResult[E any] struct {
	Items []*E   `json:"items"`
	Total uint64 `json:"total"`
}

// Count 使用 whereSelectors 计算符合条件的记录数
func (q *Querier[DTO, ENTITY]) Count(ctx context.Context, db *gorm.DB, whereSelectors []func(*gorm.DB) *gorm.DB) (int, error) {
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
	return int(cnt), nil
}

// ListWithPaging 使用 PagingRequest 查询列表（接收 *gorm.DB）
func (q *Querier[DTO, ENTITY]) ListWithPaging(ctx context.Context, db *gorm.DB, req *paginationV1.PagingRequest) (*PagingResult[DTO], error) {
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

	// filters
	if req.Query != nil || req.OrQuery != nil {
		whereSelectors, err = q.queryStringFilter.BuildSelectors(req.GetQuery(), req.GetOrQuery())
		if err != nil {
			log.Errorf("build query string filter selectors failed: %s", err.Error())
		}
	} else if req.FilterExpr != nil {
		whereSelectors, err = q.structuredFilter.BuildSelectors(req.GetFilterExpr())
		if err != nil {
			log.Errorf("build structured filter selectors failed: %s", err.Error())
		}
	}

	// select fields
	if req.GetFieldMask() != nil && len(req.GetFieldMask().Paths) > 0 {
		selectSelector, err = q.fieldSelector.BuildSelector(req.GetFieldMask().GetPaths())
		if err != nil {
			log.Errorf("build field select selector failed: %s", err.Error())
		}
	}

	// order by
	if len(req.GetSorting()) > 0 {
		sortingSelector = q.structuredSorting.BuildScope(req.GetSorting())
	} else if len(req.GetOrderBy()) > 0 {
		sortingSelector = q.queryStringSorting.BuildScope(req.GetOrderBy())
	}

	// pagination
	if !req.GetNoPaging() {
		if req.Page != nil && req.PageSize != nil {
			pagingSelector = q.pagePaginator.BuildDB(int(req.GetPage()), int(req.GetPageSize()))
		} else if req.Offset != nil && req.Limit != nil {
			pagingSelector = q.offsetPaginator.BuildDB(int(req.GetOffset()), int(req.GetLimit()))
		} else if req.Token != nil && req.Offset != nil {
			pagingSelector = q.tokenPaginator.BuildDB(req.GetToken(), int(req.GetOffset()))
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
	if err := listDB.Find(&entities).Error; err != nil {
		log.Errorf("query list failed: %s", err.Error())
		return nil, errors.New("query list failed")
	}

	// map to DTOs
	dtos := make([]*DTO, 0, len(entities))
	for _, e := range entities {
		dtos = append(dtos, q.mapper.ToDTO(e))
	}

	// 计数（只使用 whereSelectors）
	total, err := q.Count(ctx, db, whereSelectors)
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
func (q *Querier[DTO, ENTITY]) ListWithPagination(ctx context.Context, db *gorm.DB, req *paginationV1.PaginationRequest) (*PagingResult[DTO], error) {
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
	if req.Query != nil || req.OrQuery != nil {
		whereSelectors, err = q.queryStringFilter.BuildSelectors(req.GetQuery(), req.GetOrQuery())
		if err != nil {
			log.Errorf("build query string filter selectors failed: %s", err.Error())
		}
	} else if req.FilterExpr != nil {
		whereSelectors, err = q.structuredFilter.BuildSelectors(req.GetFilterExpr())
		if err != nil {
			log.Errorf("build structured filter selectors failed: %s", err.Error())
		}
	}

	// select fields
	if req.GetFieldMask() != nil && len(req.GetFieldMask().Paths) > 0 {
		selectSelector, err = q.fieldSelector.BuildSelector(req.GetFieldMask().GetPaths())
		if err != nil {
			log.Errorf("build field select selector failed: %s", err.Error())
		}
	}

	// order by
	if len(req.GetSorting()) > 0 {
		sortingSelector = q.structuredSorting.BuildScope(req.GetSorting())
	} else if len(req.GetOrderBy()) > 0 {
		sortingSelector = q.queryStringSorting.BuildScope(req.GetOrderBy())
	}

	// pagination types
	switch req.GetPaginationType().(type) {
	case *paginationV1.PaginationRequest_OffsetBased:
		pagingSelector = q.offsetPaginator.BuildDB(int(req.GetOffsetBased().GetOffset()), int(req.GetOffsetBased().GetLimit()))
	case *paginationV1.PaginationRequest_PageBased:
		pagingSelector = q.pagePaginator.BuildDB(int(req.GetPageBased().GetPage()), int(req.GetPageBased().GetPageSize()))
	case *paginationV1.PaginationRequest_TokenBased:
		pagingSelector = q.tokenPaginator.BuildDB(req.GetTokenBased().GetToken(), int(req.GetTokenBased().GetPageSize()))
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
	if err := listDB.Find(&entities).Error; err != nil {
		log.Errorf("query list failed: %s", err.Error())
		return nil, errors.New("query list failed")
	}

	// map to DTOs
	dtos := make([]*DTO, 0, len(entities))
	for _, e := range entities {
		dtos = append(dtos, q.mapper.ToDTO(e))
	}

	// 计数
	total, err := q.Count(ctx, db, whereSelectors)
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

// Get 根据查询条件获取单条记录（接收 *gorm.DB）
func (q *Querier[DTO, ENTITY]) Get(ctx context.Context, db *gorm.DB, viewMask *fieldmaskpb.FieldMask) (*DTO, error) {
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

	dto := q.mapper.ToDTO(&ent)
	return dto, nil
}

// Only alias
func (q *Querier[DTO, ENTITY]) Only(ctx context.Context, db *gorm.DB, viewMask *fieldmaskpb.FieldMask) (*DTO, error) {
	return q.Get(ctx, db, viewMask)
}
