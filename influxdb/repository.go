package influxdb

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-kratos/kratos/v2/log"
	paginationFilter "github.com/tx7do/go-crud/pagination/filter"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/influxdb/field"
	"github.com/tx7do/go-crud/influxdb/filter"
	paging "github.com/tx7do/go-crud/influxdb/pagination"
	"github.com/tx7do/go-crud/influxdb/query"
	"github.com/tx7do/go-crud/influxdb/sorting"
)

// Repository MongoDB 版仓库（泛型）
type Repository[DTO any, ENTITY any] struct {
	queryStringSorting *sorting.QueryStringSorting
	structuredSorting  *sorting.StructuredSorting

	offsetPaginator *paging.OffsetPaginator
	pagePaginator   *paging.PagePaginator
	tokenPaginator  *paging.TokenPaginator

	structuredFilter *filter.StructuredFilter

	queryStringConverter  *paginationFilter.QueryStringConverter
	filterStringConverter *paginationFilter.FilterStringConverter

	fieldSelector *field.Selector

	client     *Client
	collection string
	log        *log.Helper
}

func NewRepository[DTO any, ENTITY any](client *Client, collection string, logger *log.Helper) *Repository[DTO, ENTITY] {
	return &Repository[DTO, ENTITY]{
		client:     client,
		collection: collection,

		log: logger,

		queryStringSorting: sorting.NewQueryStringSorting(),
		structuredSorting:  sorting.NewStructuredSorting(),

		offsetPaginator: paging.NewOffsetPaginator(),
		pagePaginator:   paging.NewPagePaginator(),
		tokenPaginator:  paging.NewTokenPaginator(),

		structuredFilter: filter.NewStructuredFilter(),

		queryStringConverter:  paginationFilter.NewQueryStringConverter(),
		filterStringConverter: paginationFilter.NewFilterStringConverter(),

		fieldSelector: field.NewFieldSelector(),
	}
}

// ListWithPaging 针对 paginationV1.PagingRequest 的列表查询（兼容 Query/OrQuery/FilterExpr）
func (r *Repository[DTO, ENTITY]) ListWithPaging(ctx context.Context, req *paginationV1.PagingRequest) ([]*DTO, int64, error) {
	if r.client == nil {
		return nil, 0, errors.New("influxdb database is nil")
	}
	if r.collection == "" {
		return nil, 0, errors.New("collection is empty")
	}

	qb := query.NewQueryBuilder(r.collection)

	var err error

	// apply filters
	if req.GetQuery() != "" {
		req.FilterExpr, err = r.queryStringConverter.Convert(req.GetQuery())
		if err != nil {
			log.Errorf("convert query to filter expr failed: %s", err.Error())
			return nil, 0, err
		}
	} else if req.GetFilter() != "" {
		req.FilterExpr, err = r.filterStringConverter.Convert(req.GetFilter())
		if err != nil {
			log.Errorf("convert filter string to filter expr failed: %s", err.Error())
			return nil, 0, err
		}
	}

	if _, err = r.structuredFilter.BuildSelectors(qb, req.FilterExpr); err != nil {
		return nil, 0, err
	}

	// select fields
	if req.FieldMask != nil && len(req.GetFieldMask().Paths) > 0 {
		if _, err := r.fieldSelector.BuildSelector(qb, req.GetFieldMask().GetPaths()); err != nil {
			r.log.Errorf("field selector build error: %v", err)
		}
	}

	// sorting
	if len(req.GetSorting()) > 0 {
		_ = r.structuredSorting.BuildOrderClause(qb, req.GetSorting())
	} else if len(req.GetOrderBy()) > 0 {
		_ = r.queryStringSorting.BuildOrderClause(qb, req.GetOrderBy())
	}

	// pagination
	if !req.GetNoPaging() {
		if req.Page != nil && req.PageSize != nil {
			_ = r.pagePaginator.BuildClause(qb, int(req.GetPage()), int(req.GetPageSize()))
		} else if req.Offset != nil && req.Limit != nil {
			_ = r.offsetPaginator.BuildClause(qb, int(req.GetOffset()), int(req.GetLimit()))
		} else if req.Token != nil && req.Offset != nil {
			_ = r.tokenPaginator.BuildClause(qb, req.GetToken(), int(req.GetOffset()))
		}
	}

	// 计数
	total, err := r.client.Count(ctx, qb.Build())
	if err != nil {
		return nil, 0, err
	}

	return nil, total, nil
}

// ListWithPagination 针对 paginationV1.PaginationRequest 的列表查询
func (r *Repository[DTO, ENTITY]) ListWithPagination(ctx context.Context, req *paginationV1.PaginationRequest) ([]*DTO, int64, error) {
	if r.client == nil {
		return nil, 0, errors.New("influxdb database is nil")
	}
	if r.collection == "" {
		return nil, 0, errors.New("collection is empty")
	}

	qb := query.NewQueryBuilder(r.collection)

	var err error

	// apply filters
	if req.GetQuery() != "" {
		req.FilterExpr, err = r.queryStringConverter.Convert(req.GetQuery())
		if err != nil {
			log.Errorf("convert query to filter expr failed: %s", err.Error())
			return nil, 0, err
		}
	} else if req.GetFilter() != "" {
		req.FilterExpr, err = r.filterStringConverter.Convert(req.GetFilter())
		if err != nil {
			log.Errorf("convert filter string to filter expr failed: %s", err.Error())
			return nil, 0, err
		}
	}

	if _, err = r.structuredFilter.BuildSelectors(qb, req.FilterExpr); err != nil {
		return nil, 0, err
	}

	// select fields
	if req.FieldMask != nil && len(req.GetFieldMask().Paths) > 0 {
		if _, err := r.fieldSelector.BuildSelector(qb, req.GetFieldMask().GetPaths()); err != nil {
			r.log.Errorf("field selector build error: %v", err)
		}
	}

	// sorting
	if len(req.GetSorting()) > 0 {
		_ = r.structuredSorting.BuildOrderClause(qb, req.GetSorting())
	} else if len(req.GetOrderBy()) > 0 {
		_ = r.queryStringSorting.BuildOrderClause(qb, req.GetOrderBy())
	}

	// pagination
	switch req.GetPaginationType().(type) {
	case *paginationV1.PaginationRequest_OffsetBased:
		_ = r.offsetPaginator.BuildClause(qb, int(req.GetOffsetBased().GetOffset()), int(req.GetOffsetBased().GetLimit()))
	case *paginationV1.PaginationRequest_PageBased:
		_ = r.pagePaginator.BuildClause(qb, int(req.GetPageBased().GetPage()), int(req.GetPageBased().GetPageSize()))
	case *paginationV1.PaginationRequest_TokenBased:
		_ = r.tokenPaginator.BuildClause(qb, req.GetTokenBased().GetToken(), int(req.GetTokenBased().GetPageSize()))
	}

	// 计数
	total, err := r.client.Count(ctx, qb.Build())
	if err != nil {
		return nil, 0, err
	}

	return nil, total, nil
}

// Create 插入一条记录
func (r *Repository[DTO, ENTITY]) Create(_ context.Context, dto *DTO) (*DTO, error) {
	if r.client == nil {
		return nil, errors.New("influxdb database is nil")
	}
	if r.collection == "" {
		return nil, errors.New("collection is empty")
	}
	if dto == nil {
		return nil, errors.New("dto is nil")
	}

	return nil, nil
}

// BatchCreate 批量插入
func (r *Repository[DTO, ENTITY]) BatchCreate(_ context.Context, dtos []*DTO) ([]*DTO, error) {
	if r.client == nil {
		return nil, errors.New("influxdb database is nil")
	}
	if r.collection == "" {
		return nil, errors.New("collection is empty")
	}
	if len(dtos) == 0 {
		return nil, nil
	}

	return nil, nil
}

// Count 按给定 builder 中的 filter 统计数量
func (r *Repository[DTO, ENTITY]) Count(ctx context.Context, baseWhere string, whereArgs ...any) (int64, error) {
	if r.client == nil {
		return 0, errors.New("influxdb database is nil")
	}
	if r.collection == "" {
		return 0, errors.New("collection is empty")
	}

	qb := query.NewQueryBuilder(r.collection)

	if baseWhere != "" {
		if len(whereArgs) > 0 {
			baseWhere = fmt.Sprintf(baseWhere, whereArgs...)
		}
		// 使用 WhereFromRaw 添加 where 片段（函数会去掉可能的 "WHERE" 前缀）
		qb.WhereFromRaw(baseWhere)
	}

	aSql := qb.Build()
	if aSql == "" {
		return 0, errors.New("query builder produced empty query")
	}

	count, err := r.client.Count(ctx, aSql)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// Exists 检查是否存在符合条件的记录
func (r *Repository[DTO, ENTITY]) Exists(ctx context.Context, baseWhere string, whereArgs ...any) (bool, error) {
	if r.client == nil {
		return false, errors.New("influxdb database is nil")
	}
	if r.collection == "" {
		return false, errors.New("collection is empty")
	}

	qb := query.NewQueryBuilder(r.collection)

	if baseWhere != "" {
		if len(whereArgs) > 0 {
			baseWhere = fmt.Sprintf(baseWhere, whereArgs...)
		}
		// 使用 WhereFromRaw 添加 where 片段（函数会去掉可能的 "WHERE" 前缀）
		qb.WhereFromRaw(baseWhere)
	}

	aSql := qb.Build()
	if aSql == "" {
		return false, errors.New("query builder produced empty query")
	}

	exists, err := r.client.Exist(ctx, aSql)
	if err != nil {
		return false, err
	}
	return exists, nil
}
