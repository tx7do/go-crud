package mongodb

import (
	"context"
	"errors"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/go-utils/mapper"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/mongodb/field"
	"github.com/tx7do/go-crud/mongodb/filter"
	paging "github.com/tx7do/go-crud/mongodb/pagination"
	"github.com/tx7do/go-crud/mongodb/query"
	"github.com/tx7do/go-crud/mongodb/sorting"
	paginationFilter "github.com/tx7do/go-crud/pagination/filter"

	bsonV2 "go.mongodb.org/mongo-driver/v2/bson"
	optionsV2 "go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Repository MongoDB 版仓库（泛型）
type Repository[DTO any, ENTITY any] struct {
	mapper *mapper.CopierMapper[DTO, ENTITY]

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

func NewRepository[DTO any, ENTITY any](client *Client, collection string, mapper *mapper.CopierMapper[DTO, ENTITY], logger *log.Helper) *Repository[DTO, ENTITY] {
	return &Repository[DTO, ENTITY]{
		client:     client,
		collection: collection,

		mapper: mapper,
		log:    logger,

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
		return nil, 0, errors.New("mongodb database is nil")
	}
	if r.collection == "" {
		return nil, 0, errors.New("collection is empty")
	}

	qb := query.NewQueryBuilder()

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
	total, err := r.Count(ctx, qb)
	if err != nil {
		return nil, 0, err
	}

	// 执行查询
	filterDoc, _, err := qb.BuildFind()
	if err != nil {
		return nil, 0, err
	}
	if filterDoc == nil {
		filterDoc = bsonV2.M{}
	}

	var results []*ENTITY
	if err = r.client.Find(ctx, r.collection, filterDoc, &results); err != nil {
		r.log.Errorf("find failed: %v", err)
		return nil, 0, err
	}

	// 转换为 DTO
	dtos := make([]*DTO, 0, len(results))
	for _, ent := range results {
		dtos = append(dtos, r.mapper.ToDTO(ent))
	}
	return dtos, total, nil
}

// ListWithPagination 针对 paginationV1.PaginationRequest 的列表查询
func (r *Repository[DTO, ENTITY]) ListWithPagination(ctx context.Context, req *paginationV1.PaginationRequest) ([]*DTO, int64, error) {
	if r.client == nil {
		return nil, 0, errors.New("mongodb database is nil")
	}
	if r.collection == "" {
		return nil, 0, errors.New("collection is empty")
	}

	qb := query.NewQueryBuilder()

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
	total, err := r.Count(ctx, qb)
	if err != nil {
		return nil, 0, err
	}

	// 执行查询
	filterDoc, _, err := qb.BuildFind()
	if err != nil {
		return nil, 0, err
	}
	if filterDoc == nil {
		filterDoc = bsonV2.M{}
	}

	var results []*ENTITY
	if err = r.client.Find(ctx, r.collection, filterDoc, &results); err != nil {
		r.log.Errorf("find failed: %v", err)
		return nil, 0, err
	}

	// 转换为 DTO
	dtos := make([]*DTO, 0, len(results))
	for _, ent := range results {
		dtos = append(dtos, r.mapper.ToDTO(ent))
	}
	return dtos, total, nil
}

// Get 根据过滤条件返回单条记录（使用 FilterExpr 或 Query/OrQuery 前置构建 qb）
func (r *Repository[DTO, ENTITY]) Get(ctx context.Context, qb *query.Builder) (*DTO, error) {
	if r.client == nil {
		return nil, errors.New("mongodb database is nil")
	}
	if r.collection == "" {
		return nil, errors.New("collection is empty")
	}
	if qb == nil {
		qb = query.NewQueryBuilder()
	}

	filterDoc, _, err := qb.BuildFindOne()
	if err != nil {
		return nil, err
	}
	if filterDoc == nil {
		filterDoc = bsonV2.M{}
	}

	var ent ENTITY
	if err = r.client.FindOne(ctx, r.collection, filterDoc, &ent); err != nil {
		r.log.Errorf("find one failed: %v", err)
		return nil, err
	}

	dto := r.mapper.ToDTO(&ent)
	return dto, nil
}

// Create 插入一条记录
func (r *Repository[DTO, ENTITY]) Create(ctx context.Context, dto *DTO) (*DTO, error) {
	if r.client == nil {
		return nil, errors.New("mongodb database is nil")
	}
	if r.collection == "" {
		return nil, errors.New("collection is empty")
	}
	if dto == nil {
		return nil, errors.New("dto is nil")
	}

	ent := r.mapper.ToEntity(dto)

	if _, err := r.client.InsertOne(ctx, r.collection, ent); err != nil {
		r.log.Errorf("insert failed: %v", err)
		return nil, err
	}

	return r.mapper.ToDTO(ent), nil
}

// BatchCreate 批量插入
func (r *Repository[DTO, ENTITY]) BatchCreate(ctx context.Context, dtos []*DTO) ([]*DTO, error) {
	if r.client == nil {
		return nil, errors.New("mongodb database is nil")
	}
	if r.collection == "" {
		return nil, errors.New("collection is empty")
	}
	if len(dtos) == 0 {
		return nil, nil
	}

	docs := make([]interface{}, 0, len(dtos))
	ents := make([]*ENTITY, 0, len(dtos))
	for _, d := range dtos {
		e := r.mapper.ToEntity(d)
		ents = append(ents, e)
		docs = append(docs, e)
	}

	if _, err := r.client.InsertMany(ctx, r.collection, docs); err != nil {
		r.log.Errorf("insert many failed: %v", err)
		return nil, err
	}

	out := make([]*DTO, 0, len(ents))
	for _, e := range ents {
		out = append(out, r.mapper.ToDTO(e))
	}
	return out, nil
}

// Update 根据 filter 在 qb 中定位并更新（qb 应包含 where/selector info）
func (r *Repository[DTO, ENTITY]) Update(ctx context.Context, qb *query.Builder, updateDoc interface{}) (*DTO, error) {
	if r.client == nil {
		return nil, errors.New("mongodb database is nil")
	}
	if r.collection == "" {
		return nil, errors.New("collection is empty")
	}
	if qb == nil {
		return nil, errors.New("query builder is nil for update")
	}

	filterDoc, _, err := qb.BuildFindOne()
	if err != nil {
		return nil, err
	}
	if filterDoc == nil {
		return nil, errors.New("empty filter for update")
	}

	var ent ENTITY
	err = r.client.FindOneAndUpdate(ctx, r.collection,
		filterDoc, updateDoc,
		&ent,
		optionsV2.FindOneAndUpdate().SetReturnDocument(optionsV2.After),
	)
	if err != nil {
		r.log.Errorf("update one failed: %v", err)
		return nil, err
	}

	return r.mapper.ToDTO(&ent), nil
}

// Delete 根据 qb 中的 filter 删除（硬删除）
func (r *Repository[DTO, ENTITY]) Delete(ctx context.Context, qb *query.Builder) (int64, error) {
	if r.client == nil {
		return 0, errors.New("mongodb database is nil")
	}
	if r.collection == "" {
		return 0, errors.New("collection is empty")
	}
	if qb == nil {
		return 0, errors.New("query builder is nil for delete")
	}

	filterDoc, _, err := qb.BuildFind()
	if err != nil {
		return 0, err
	}
	if filterDoc == nil {
		filterDoc = bsonV2.M{}
	}

	res, err := r.client.DeleteMany(ctx, r.collection, filterDoc)
	if err != nil {
		r.log.Errorf("delete documents failed: %v", err)
		return 0, err
	}

	return res.DeletedCount, nil
}

// Count 按给定 builder 中的 filter 统计数量
func (r *Repository[DTO, ENTITY]) Count(ctx context.Context, qb *query.Builder) (int64, error) {
	if r.client == nil {
		return 0, errors.New("mongodb database is nil")
	}
	if r.collection == "" {
		return 0, errors.New("collection is empty")
	}
	if qb == nil {
		qb = query.NewQueryBuilder()
	}

	filterDoc, _, err := qb.BuildFind()
	if err != nil {
		return 0, err
	}

	count, err := r.client.Count(ctx, r.collection, filterDoc)
	if err != nil {
		r.log.Errorf("count documents failed: %v", err)
		return 0, err
	}

	return count, nil
}

// Exists 判断是否存在符合 qb 的记录
func (r *Repository[DTO, ENTITY]) Exists(ctx context.Context, qb *query.Builder) (bool, error) {
	if r.client == nil {
		return false, errors.New("mongodb database is nil")
	}
	if r.collection == "" {
		return false, errors.New("collection is empty")
	}
	if qb == nil {
		qb = query.NewQueryBuilder()
	}

	filterDoc, _, err := qb.BuildFind()
	if err != nil {
		return false, err
	}

	exist, err := r.client.Exist(ctx, r.collection, filterDoc)
	if err != nil {
		r.log.Errorf("exist documents failed: %v", err)
		return false, err
	}

	return exist, nil
}
