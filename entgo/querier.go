package entgo

import (
	"context"
	"errors"

	"entgo.io/ent/dialect/sql"
	"github.com/go-kratos/kratos/v2/log"

	"github.com/tx7do/go-utils/mapper"

	pagination "github.com/tx7do/go-curd/api/gen/go/pagination/v1"
	"github.com/tx7do/go-curd/entgo/field"
	"github.com/tx7do/go-curd/entgo/filter"
	paging "github.com/tx7do/go-curd/entgo/pagination"
	"github.com/tx7do/go-curd/entgo/sorting"
)

type QueryBuilder[ENTQ any, ENTITY any] interface {
	Modify(modifiers ...func(s *sql.Selector)) QueryBuilder[ENTQ, ENTITY]

	Clone() QueryBuilder[ENTQ, ENTITY]

	All(ctx context.Context) ([]*ENTITY, error)

	Count(ctx context.Context) (int, error)
}

// Querier Ent查询器
type Querier[ENTQ any, DTO any, ENTITY any] struct {
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

func NewQuerier[ENTQ any, DTO any, ENTITY any](mapper *mapper.CopierMapper[DTO, ENTITY]) *Querier[ENTQ, DTO, ENTITY] {
	return &Querier[ENTQ, DTO, ENTITY]{
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
func (q *Querier[ENTQ, DTO, ENTITY]) Count(ctx context.Context, builder QueryBuilder[ENTQ, ENTITY], whereCond []func(s *sql.Selector)) (int, error) {
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

// ListWithPaging 使用分页请求查询列表
func (q *Querier[ENTQ, DTO, ENTITY]) ListWithPaging(ctx context.Context, builder QueryBuilder[ENTQ, ENTITY], req *pagination.PagingRequest) (*PagingResult[DTO], error) {
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
	if len(req.GetFieldMask().Paths) > 0 {
		selectSelector, err = q.fieldSelector.BuildSelector(req.GetFieldMask().GetPaths())
		if err != nil {
			log.Errorf("build field select selector failed: %s", err.Error())
		}
	}
	if selectSelector != nil {
		querySelectors = append(querySelectors, selectSelector)
	}

	// order by
	if len(req.GetSorting()) > 0 {
		sortingSelector, err = q.structuredSorting.BuildSelector(req.GetSorting())
		if err != nil {
			log.Errorf("build structured sorting selector failed: %s", err.Error())
		}
	} else if len(req.GetOrderBy()) > 0 {
		sortingSelector, err = q.queryStringSorting.BuildSelector(req.GetOrderBy())
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
			pagingSelector = q.pagePaginator.BuildSelector(int(req.GetPage()), int(req.GetPageSize()))
		} else if req.Offset != nil && req.Limit != nil {
			pagingSelector = q.offsetPaginator.BuildSelector(int(req.GetOffset()), int(req.GetLimit()))
		} else if req.Token != nil && req.Offset != nil {
			pagingSelector = q.tokenPaginator.BuildSelector(req.GetToken(), int(req.GetOffset()))
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
		dto := q.mapper.ToDTO(entity)
		dtos = append(dtos, dto)
	}

	count, err := q.Count(ctx, countBuilder, whereSelectors)
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
func (q *Querier[ENTQ, DTO, ENTITY]) ListWithPagination(ctx context.Context, builder QueryBuilder[ENTQ, ENTITY], req *pagination.PaginationRequest) (*PagingResult[DTO], error) {
	if req == nil {
		return nil, errors.New("pagination request is nil")
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
	if len(req.GetFieldMask().Paths) > 0 {
		selectSelector, err = q.fieldSelector.BuildSelector(req.GetFieldMask().GetPaths())
		if err != nil {
			log.Errorf("build field select selector failed: %s", err.Error())
		}
	}
	if selectSelector != nil {
		querySelectors = append(querySelectors, selectSelector)
	}

	// order by
	if len(req.GetSorting()) > 0 {
		sortingSelector, err = q.structuredSorting.BuildSelector(req.GetSorting())
		if err != nil {
			log.Errorf("build structured sorting selector failed: %s", err.Error())
		}
	} else if len(req.GetOrderBy()) > 0 {
		sortingSelector, err = q.queryStringSorting.BuildSelector(req.GetOrderBy())
		if err != nil {
			log.Errorf("build query string sorting selector failed: %s", err.Error())
		}
	}
	if sortingSelector != nil {
		querySelectors = append(querySelectors, sortingSelector)
	}

	// pagination
	switch req.GetPaginationType().(type) {
	case *pagination.PaginationRequest_OffsetBased:
		pagingSelector = q.offsetPaginator.BuildSelector(int(req.GetOffsetBased().GetOffset()), int(req.GetOffsetBased().GetLimit()))
	case *pagination.PaginationRequest_PageBased:
		pagingSelector = q.pagePaginator.BuildSelector(int(req.GetPageBased().GetPage()), int(req.GetPageBased().GetPageSize()))
	case *pagination.PaginationRequest_TokenBased:
		pagingSelector = q.tokenPaginator.BuildSelector(req.GetTokenBased().GetToken(), int(req.GetTokenBased().GetPageSize()))
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
		dto := q.mapper.ToDTO(entity)
		dtos = append(dtos, dto)
	}

	count, err := q.Count(ctx, countBuilder, whereSelectors)
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
