package entgo

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/tx7do/go-crud/log"
	"github.com/redis/go-redis/v9"
	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/cache"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) WithCache(
	redisClient *redis.Client,
	prefix string,
	singleTTL time.Duration,
	listTTL time.Duration,
) *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
] {
	r.cacheKeyPrefix = prefix
	r.cacheTTL = singleTTL
	r.cacheListTTL = listTTL

	r.cacheSupportSingle = cache.NewCacheSupport[DTO](redisClient, singleTTL)
	r.cacheSupportList = cache.NewCacheSupport[PagingResult[DTO]](redisClient, listTTL)

	return r
}

func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) WithCacheFromRedis(
	redisClient *redis.Client,
	prefix string,
	ttl time.Duration,
) *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
] {
	return r.WithCache(redisClient, prefix, ttl, ttl/3)
}

// generateCacheKey 生成单条记录的缓存 key
// 示例: "user:id:123"
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) generateCacheKey(id any) string {
	return fmt.Sprintf("%sid:%v", r.cacheKeyPrefix, id)
}

// generateListCacheKey 生成列表查询的缓存 key（基于 filter + sort + page 的 hash）
// 示例: "user:list:hash_abc123"
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) generateListCacheKey(req *paginationV1.PagingRequest) (string, error) {
	if req == nil {
		return "", errors.New("paging request is nil")
	}

	var sig strings.Builder
	sig.WriteString(r.cacheKeyPrefix)
	sig.WriteString("list:")

	// 1. 分页参数（指针字段必须判空）
	if req.Page != nil {
		sig.WriteString(fmt.Sprintf("p%d:", *req.Page))
	}
	if req.PageSize != nil {
		sig.WriteString(fmt.Sprintf("s%d:", *req.PageSize))
	}
	if req.Offset != nil {
		sig.WriteString(fmt.Sprintf("o%d:", *req.Offset))
	}
	if req.Limit != nil {
		sig.WriteString(fmt.Sprintf("l%d:", *req.Limit))
	}

	// 2. Filter 表达式（使用 protobuf Marshal）
	if req.GetFilterExpr() != nil {
		filterBytes, err := proto.Marshal(req.GetFilterExpr())
		if err != nil {
			return "", fmt.Errorf("marshal filter: %w", err)
		}
		sig.WriteString(fmt.Sprintf("f:%s:", base64.RawURLEncoding.EncodeToString(filterBytes)))
	}

	// 3. OrderBy / Sorting
	for _, ob := range req.GetOrderBy() {
		sig.WriteString(fmt.Sprintf("ob:%d:", ob))
	}
	for _, sort := range req.GetSorting() {
		sig.WriteString(fmt.Sprintf("s:%s:%s:", sort.GetField(), sort.GetDirection()))
	}

	// 4. FieldMask（影响返回字段）
	if req.GetFieldMask() != nil && len(req.GetFieldMask().Paths) > 0 {
		paths := strings.Join(req.GetFieldMask().Paths, ",")
		sig.WriteString(fmt.Sprintf("fm:%s:", paths))
	}

	// 5. 计算短 hash 避免 key 过长
	hash := md5.Sum([]byte(sig.String()))
	return fmt.Sprintf("%slist:%s", r.cacheKeyPrefix, hex.EncodeToString(hash[:8])), nil
}

// generateListCacheKeyFromPagination 从 PaginationRequest 生成缓存 key
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) generateListCacheKeyFromPagination(req *paginationV1.PaginationRequest) (string, error) {
	if req == nil {
		return "", errors.New("pagination request is nil")
	}

	var sig strings.Builder
	sig.WriteString(r.cacheKeyPrefix)
	sig.WriteString("list:")

	// 1. 分页参数（根据不同类型的分页方式）
	switch paginationType := req.GetPaginationType().(type) {
	case *paginationV1.PaginationRequest_OffsetBased:
		if paginationType.OffsetBased != nil {
			sig.WriteString(fmt.Sprintf("o%d:l%d:", paginationType.OffsetBased.GetOffset(), paginationType.OffsetBased.GetLimit()))
		}
	case *paginationV1.PaginationRequest_PageBased:
		if paginationType.PageBased != nil {
			sig.WriteString(fmt.Sprintf("p%d:s%d:", paginationType.PageBased.GetPage(), paginationType.PageBased.GetPageSize()))
		}
	case *paginationV1.PaginationRequest_TokenBased:
		if paginationType.TokenBased != nil {
			sig.WriteString(fmt.Sprintf("t%s:s%d:", paginationType.TokenBased.GetToken(), paginationType.TokenBased.GetPageSize()))
		}
	}

	// 2. Filter 表达式
	if req.GetFilterExpr() != nil {
		filterBytes, err := proto.Marshal(req.GetFilterExpr())
		if err != nil {
			return "", fmt.Errorf("marshal filter: %w", err)
		}
		sig.WriteString(fmt.Sprintf("f:%s:", base64.RawURLEncoding.EncodeToString(filterBytes)))
	}

	// 3. OrderBy / Sorting
	for _, ob := range req.GetOrderBy() {
		sig.WriteString(fmt.Sprintf("ob:%d:", ob))
	}
	for _, sort := range req.GetSorting() {
		sig.WriteString(fmt.Sprintf("s:%s:%s:", sort.GetField(), sort.GetDirection()))
	}

	// 4. FieldMask
	if req.GetFieldMask() != nil && len(req.GetFieldMask().Paths) > 0 {
		paths := strings.Join(req.GetFieldMask().Paths, ",")
		sig.WriteString(fmt.Sprintf("fm:%s:", paths))
	}

	// 5. 计算短 hash
	hash := md5.Sum([]byte(sig.String()))
	return fmt.Sprintf("%slist:%s", r.cacheKeyPrefix, hex.EncodeToString(hash[:8])), nil
}

// GetByIDWithCache 根据ID带缓存的单条查询
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) GetByIDWithCache(
	ctx context.Context,
	builder QueryBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	id any,
	viewMask *fieldmaskpb.FieldMask,
	opts ...cache.Option,
) (*DTO, error) {
	if r.cacheSupportSingle == nil {
		// 降级：直接查询
		return r.Get(ctx, builder, viewMask)
	}

	cacheKey := r.generateCacheKey(id)

	dto, err := r.cacheSupportSingle.GetOrLoad(
		ctx,
		cacheKey,
		func(ctx context.Context) (*DTO, error) {
			return r.Get(ctx, builder, viewMask)
		},
		opts...,
	)
	return dto, err
}

// ListWithPagingCache 带缓存的分页列表查询
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) ListWithPagingCache(
	ctx context.Context,
	builder ListBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	countBuilder ListBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	req *paginationV1.PagingRequest,
	opts ...cache.Option,
) (*PagingResult[DTO], error) {
	// 未配置缓存 → 降级
	if r.cacheSupportList == nil {
		return r.ListWithPaging(ctx, builder, countBuilder, req)
	}

	cacheKey, err := r.generateListCacheKey(req)
	if err != nil {
		log.Warnf("generate list cache key failed: %v, fallback to db", err)
		return r.ListWithPaging(ctx, builder, countBuilder, req)
	}

	// 使用 GetOrLoad 加载列表
	// 注意：这里缓存的是整个分页结果（包含 total）
	pageResult, err := r.cacheSupportList.GetOrLoad(
		ctx,
		cacheKey,
		func(ctx context.Context) (*PagingResult[DTO], error) {
			return r.ListWithPaging(ctx, builder, countBuilder, req)
		},
		opts...,
	)

	return pageResult, err
}

// ListWithPaginationCache 带缓存的通用分页列表查询
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) ListWithPaginationCache(
	ctx context.Context,
	builder ListBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	countBuilder ListBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	req *paginationV1.PaginationRequest,
	opts ...cache.Option,
) (*PagingResult[DTO], error) {
	// 未配置缓存 → 降级
	if r.cacheSupportList == nil {
		return r.ListWithPagination(ctx, builder, countBuilder, req)
	}

	// 生成缓存键（复用 PagingRequest 的逻辑，需要转换）
	cacheKey, err := r.generateListCacheKeyFromPagination(req)
	if err != nil {
		log.Warnf("generate list cache key failed: %v, fallback to db", err)
		return r.ListWithPagination(ctx, builder, countBuilder, req)
	}

	// 使用 GetOrLoad 加载列表
	pageResult, err := r.cacheSupportList.GetOrLoad(
		ctx,
		cacheKey,
		func(ctx context.Context) (*PagingResult[DTO], error) {
			return r.ListWithPagination(ctx, builder, countBuilder, req)
		},
		opts...,
	)

	return pageResult, err
}

// ListTreeWithPagingCache 带缓存的树形分页列表查询
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) ListTreeWithPagingCache(
	ctx context.Context,
	builder ListBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	countBuilder ListBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	req *paginationV1.PagingRequest,
	opts ...cache.Option,
) (*PagingResult[DTO], error) {
	// 未配置缓存 → 降级
	if r.cacheSupportList == nil {
		return r.ListTreeWithPaging(ctx, builder, countBuilder, req)
	}

	cacheKey, err := r.generateListCacheKey(req)
	if err != nil {
		log.Warnf("generate list cache key failed: %v, fallback to db", err)
		return r.ListTreeWithPaging(ctx, builder, countBuilder, req)
	}

	// 使用 GetOrLoad 加载列表
	pageResult, err := r.cacheSupportList.GetOrLoad(
		ctx,
		cacheKey,
		func(ctx context.Context) (*PagingResult[DTO], error) {
			return r.ListTreeWithPaging(ctx, builder, countBuilder, req)
		},
		opts...,
	)

	return pageResult, err
}

// ListTreeWithPaginationCache 带缓存的树形通用分页列表查询
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) ListTreeWithPaginationCache(
	ctx context.Context,
	builder ListBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	countBuilder ListBuilder[ENT_QUERY, ENT_SELECT, ENTITY],
	req *paginationV1.PaginationRequest,
	opts ...cache.Option,
) (*PagingResult[DTO], error) {
	// 未配置缓存 → 降级
	if r.cacheSupportList == nil {
		return r.ListTreeWithPagination(ctx, builder, countBuilder, req)
	}

	cacheKey, err := r.generateListCacheKeyFromPagination(req)
	if err != nil {
		log.Warnf("generate list cache key failed: %v, fallback to db", err)
		return r.ListTreeWithPagination(ctx, builder, countBuilder, req)
	}

	// 使用 GetOrLoad 加载列表
	pageResult, err := r.cacheSupportList.GetOrLoad(
		ctx,
		cacheKey,
		func(ctx context.Context) (*PagingResult[DTO], error) {
			return r.ListTreeWithPagination(ctx, builder, countBuilder, req)
		},
		opts...,
	)

	return pageResult, err
}

// CreateWithCache 创建记录 + 失效相关缓存
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) CreateWithCache(
	ctx context.Context,
	builder CreateBuilder[ENTITY],
	dto *DTO,
	createMask *fieldmaskpb.FieldMask,
	doCreateFieldFunc func(dto *DTO),
) (*DTO, error) {
	// 1. 执行创建
	result, err := r.Create(ctx, builder, dto, createMask, doCreateFieldFunc)
	if err != nil {
		return nil, err
	}

	// 2. 失效缓存（假设 ENTITY 有 ID 字段，且创建后已填充）
	// 需从 result 或 dto 中提取新记录的 id
	if id := r.extractIDFromDTO(result); id != nil {
		r.invalidateCache(ctx, id)
	}

	return result, nil
}

// UpdateWithCache 更新记录 + 失效缓存
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) UpdateWithCache(
	ctx context.Context,
	builder UpdateOneBuilder[ENT_UPDATE_ONE, PREDICATE, ENTITY],
	dto *DTO,
	updateMask *fieldmaskpb.FieldMask,
	doUpdateFieldFunc func(dto *DTO),
	predicates ...PREDICATE,
) (*DTO, error) {
	// 1. 执行更新
	result, err := r.UpdateOne(ctx, builder, dto, updateMask, doUpdateFieldFunc, predicates...)
	if err != nil {
		return nil, err
	}

	// 2. 失效缓存
	if id := r.extractIDFromDTO(result); id != nil {
		r.invalidateCache(ctx, id)
	}

	return result, nil
}

// DeleteWithCache 删除记录 + 失效缓存
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) DeleteWithCache(
	ctx context.Context,
	builder DeleteBuilder[ENT_DELETE, PREDICATE],
	id any,
	predicates ...PREDICATE,
) (int, error) {
	// 1. 执行删除
	affected, err := r.Delete(ctx, builder, predicates...)
	if err != nil {
		return 0, err
	}

	// 2. 失效缓存
	if affected > 0 {
		r.invalidateCache(ctx, id)
	}

	return affected, nil
}

// extractIDFromDTO 从 DTO 中提取主键值（简化版）
// ⚠️ 生产环境建议：通过反射解析 DTO 的 `json:"id"` 或 `gorm:"primaryKey"` tag
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) extractIDFromDTO(dto *DTO) any {
	if dto == nil {
		return nil
	}
	// 示例：假设 DTO 有 GetId() 方法（可由 protobuf 生成）
	// 实际应根据项目规范实现
	// 这里用类型断言演示（需 DTO 实现接口）
	type IDGetter interface {
		GetId() int64
	}
	if getter, ok := any(dto).(IDGetter); ok {
		return getter.GetId()
	}
	return nil
}

// invalidateCache 删除单条 + 列表缓存（写操作后调用）
func (r *Repository[
	ENT_QUERY, ENT_SELECT,
	ENT_CREATE, ENT_CREATE_BULK,
	ENT_UPDATE, ENT_UPDATE_ONE,
	ENT_DELETE,
	PREDICATE, DTO, ENTITY,
]) invalidateCache(ctx context.Context, id any) {
	if r.cacheSupportSingle == nil {
		return
	}

	// 删除单条缓存
	_ = r.cacheSupportSingle.Cache.Del(ctx, r.generateCacheKey(id))
	// 注意：列表缓存的 key 生成依赖于查询参数，无法直接删除
	// 生产环境建议使用 Redis 的 key pattern 删除（如 "user:list:*"），但要谨慎使用以避免误删
}
