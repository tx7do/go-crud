package gorm

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/cache"
	"github.com/tx7do/go-crud/pagination"
	"github.com/tx7do/go-crud/viewer"
	"github.com/tx7do/go-wind/log"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"gorm.io/gorm"
)

func (r *Repository[DTO, ENTITY]) WithCache(
	redisClient *redis.Client,
	prefix string,
	singleTTL time.Duration,
	listTTL time.Duration,
) *Repository[DTO, ENTITY] {
	// 没有 Redis 客户端时，不启用缓存（保持 cacheSupport* 为 nil），
	// 让带缓存的方法自动降级为普通查询，而不是构造一个持有 nil redis 的支持对象导致后续 panic。
	if redisClient == nil {
		return r
	}

	r.cacheKeyPrefix = prefix
	r.cacheTTL = singleTTL
	r.cacheListTTL = listTTL
	r.cacheRedisClient = redisClient

	r.cacheSupportSingle = cache.NewCacheSupport[DTO](redisClient, singleTTL)
	r.cacheSupportList = cache.NewCacheSupport[PagingResult[DTO]](redisClient, listTTL)

	return r
}

func (r *Repository[DTO, ENTITY]) WithCacheFromRedis(
	redisClient *redis.Client,
	prefix string,
	ttl time.Duration,
) *Repository[DTO, ENTITY] {
	return r.WithCache(redisClient, prefix, ttl, ttl/3)
}

// viewMaskFingerprint 生成 FieldMask 的稳定指纹（排序后哈希），用于单条缓存 key。
// 不同 viewMask 的返回字段不同，共享缓存会把全字段的返回泄露给受限掩码的调用方。
func viewMaskFingerprint(viewMask *fieldmaskpb.FieldMask) string {
	if viewMask == nil || len(viewMask.GetPaths()) == 0 {
		return "all"
	}
	paths := append([]string(nil), viewMask.GetPaths()...)
	sort.Strings(paths)
	sum := md5.Sum([]byte(strings.Join(paths, "|")))
	return hex.EncodeToString(sum[:8])
}

// escapeScanPattern 转义 Redis MATCH 模式中的通配符，防止字符串主键携带
// * ? [ 时超范围匹配、误删其他记录的缓存。
func escapeScanPattern(s string) string {
	return strings.NewReplacer(`\`, `\\`, `*`, `\*`, `?`, `\?`, `[`, `\[`, `]`, `\]`).Replace(s)
}

// generateCacheKey 生成单条记录的缓存 key
// tenantID/userID/viewMask 作为 key 维度：不同租户、同租户内不同访问者
// （隐私规则可能对同一行返回不同结果）、不同字段掩码的缓存互不串扰。
// 示例: "user:t:42:u:7:m:hash123:id:123"
func (r *Repository[DTO, ENTITY]) generateCacheKey(vc viewer.Context, id any, viewMask *fieldmaskpb.FieldMask) string {
	return fmt.Sprintf("%st:%d:u:%d:m:%s:id:%v",
		r.cacheKeyPrefix, vc.TenantID(), vc.UserID(), viewMaskFingerprint(viewMask), id)
}

// generateListCacheKey 生成列表查询的缓存 key（基于 viewer 身份 + filter + sort + page 的 hash）
// 数据权限（SELF/UNIT/USER/ALL 等）作用于查询结果，缺少 viewer 维度会使
// 同租户不同权限用户共享缓存，造成越权读到他人数据。
// 示例: "user:list:hash_abc123"
func (r *Repository[DTO, ENTITY]) generateListCacheKey(vc viewer.Context, req *paginationV1.PagingRequest) (string, error) {
	if req == nil {
		return "", errors.New("paging request is nil")
	}

	var sig strings.Builder
	sig.WriteString(r.cacheKeyPrefix)
	sig.WriteString(fmt.Sprintf("t:%d:u:%d:ou:%d:", vc.TenantID(), vc.UserID(), vc.OrgUnitID()))
	for _, ds := range vc.DataScope() {
		sig.WriteString(fmt.Sprintf("ds:%s:%v:", ds.ScopeType, ds.TargetIDs))
	}
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
	// token 先校验解码为 lastID 再入 key：既避免原始 token 直接进 key 被刷基数，
	// 也保证等价游标命中同一缓存；非法 token 直接拒绝（fail-closed，不落缓存）。
	if req.GetToken() != "" {
		lastID, ok := pagination.VerifyAndDecode(req.GetToken(), pagination.TokenSecret())
		if !ok {
			return "", errors.New("invalid pagination token")
		}
		sig.WriteString(fmt.Sprintf("tlast:%d:", lastID))
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

// invalidateCache 删除单条缓存（写操作后调用）
// key 含 userID/viewMask 维度，需按模式清除该记录在所有用户下的副本；
// id 中的 Redis 通配符被转义，防止字符串主键超范围误删。
func (r *Repository[DTO, ENTITY]) invalidateCache(ctx context.Context, id any) {
	if r.cacheSupportSingle == nil {
		return
	}

	if r.cacheRedisClient != nil {
		pattern := r.cacheKeyPrefix + "t:*:u:*:m:*:id:" + escapeScanPattern(fmt.Sprintf("%v", id))
		var keys []string
		iter := r.cacheRedisClient.Scan(ctx, 0, pattern, 100).Iterator()
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		if err := iter.Err(); err == nil && len(keys) > 0 {
			_ = r.cacheRedisClient.Del(ctx, keys...).Err()
		}
	} else {
		// 无原始 client 时退化为删除当前访问者的副本
		vc := viewer.MustFromContext(ctx)
		_ = r.cacheSupportSingle.Cache.Del(ctx, r.generateCacheKey(vc, id, nil))
	}
	// 注意：列表缓存的 key 生成依赖于查询参数，无法直接删除
	// 生产环境建议使用 Redis 的 key pattern 删除（如 "user:list:*"），但要谨慎使用以避免误删
}

// GetWithCache 带缓存的单条查询
// 优先读缓存，未命中则回源数据库 + 写入缓存
// 用法:
//
//	dto, err := repo.WithCache(redisCache, "user:", 10*time.Minute).
//	             GetWithCache(ctx, db.Where("id = ?", 123), nil)
func (r *Repository[DTO, ENTITY]) GetWithCache(
	ctx context.Context,
	db *gorm.DB,
	viewMask *fieldmaskpb.FieldMask,
	opts ...cache.Option,
) (*DTO, error) {
	// 未配置缓存 → 降级为普通查询
	if r.cacheSupportSingle == nil {
		return r.Get(ctx, db, viewMask)
	}

	// 提取主键值用于生成 cache key（假设 ENTITY 有 ID 字段）
	// 实际应通过反射或接口获取，这里简化处理
	// 更通用方案：调用方传入 key 或 id 参数
	id := r.extractIDFromDB(db) // 需实现
	if id == nil {
		// 无法生成 key → 降级查询
		return r.Get(ctx, db, viewMask)
	}

	cacheKey := r.generateCacheKey(viewer.MustFromContext(ctx), id, viewMask)

	// 使用 CacheSupport.GetOrLoad
	dto, err := r.cacheSupportSingle.GetOrLoad(
		ctx,
		cacheKey,
		func(ctx context.Context) (*DTO, error) {
			ent, err := r.Get(ctx, db, viewMask) // 调用原有 Get 方法
			if err != nil {
				return nil, err
			}
			return ent, nil
		},
		opts..., // 支持透传 Option（如自定义 TTL）
	)

	if err != nil {
		// 区分系统错误和缓存未命中
		if errors.Is(err, cache.ErrCacheMiss) {
			// 理论上不会发生，因为 loader 会处理
			return r.Get(ctx, db, viewMask)
		}
		return nil, err
	}

	return dto, nil
}

// extractIDFromDB 从 gorm.DB 的 where 条件中提取主键值（简化版）
// ⚠️ 生产环境建议：通过反射解析 ENTITY 的 primary key tag，或要求调用方传入 id
func (r *Repository[DTO, ENTITY]) extractIDFromDB(db *gorm.DB) any {
	// 示例：解析 "id = ?" 类型的 where
	// 实际应更健壮，支持复合主键、不同字段名等
	// 这里仅做演示，建议业务层直接传 id 参数
	stmt := db.Statement
	if stmt == nil || len(stmt.Vars) == 0 {
		return nil
	}
	// 简单返回第一个参数值（假设是 id）
	return stmt.Vars[0]
}

func (r *Repository[DTO, ENTITY]) GetByIDWithCache(
	ctx context.Context,
	db *gorm.DB,
	id any,
	viewMask *fieldmaskpb.FieldMask,
	opts ...cache.Option,
) (*DTO, error) {
	if r.cacheSupportSingle == nil {
		// 降级：用 id 构造 where 查询
		return r.Get(ctx, db.Where("id = ?", id), viewMask)
	}

	cacheKey := r.generateCacheKey(viewer.MustFromContext(ctx), id, viewMask)

	dto, err := r.cacheSupportSingle.GetOrLoad(
		ctx,
		cacheKey,
		func(ctx context.Context) (*DTO, error) {
			return r.Get(ctx, db.Where("id = ?", id), viewMask)
		},
		opts...,
	)
	return dto, err
}

// ListWithPagingCache 带缓存的分页列表查询（简化版）
func (r *Repository[DTO, ENTITY]) ListWithPagingCache(
	ctx context.Context,
	db *gorm.DB,
	req *paginationV1.PagingRequest,
	opts ...cache.Option,
) (*PagingResult[DTO], error) {
	// 未配置缓存 → 降级
	if r.cacheSupportList == nil {
		result, err := r.ListWithPaging(ctx, db, req)
		if err != nil {
			return nil, err
		}
		// 适配返回类型
		return &PagingResult[DTO]{
			Items: result.Items,
			Total: result.Total,
		}, nil
	}

	cacheKey, err := r.generateListCacheKey(viewer.MustFromContext(ctx), req)
	if err != nil {
		log.Warn(context.Background(), fmt.Sprintf("generate list cache key failed: %v, fallback to db", err))
		return r.ListWithPaging(ctx, db, req)
	}

	var cached *PagingResult[DTO]
	cached, err = r.cacheSupportList.Cache.Get(ctx, cacheKey)
	if err == nil && cached != nil {
		return cached, nil
	}

	// 使用 GetOrLoad 加载列表
	// 注意：这里缓存的是整个分页结果（包含 total）
	pageResult, err := r.cacheSupportList.GetOrLoad(
		ctx,
		cacheKey,
		func(ctx context.Context) (*PagingResult[DTO], error) {
			result, err := r.ListWithPaging(ctx, db, req)
			if err != nil {
				return nil, err
			}
			return &PagingResult[DTO]{
				Items: result.Items,
				Total: result.Total,
			}, nil
		},
		opts...,
	)

	return pageResult, err
}

// CreateWithCache 创建记录 + 失效相关缓存
func (r *Repository[DTO, ENTITY]) CreateWithCache(
	ctx context.Context,
	db *gorm.DB,
	dto *DTO,
	viewMask *fieldmaskpb.FieldMask,
) (*DTO, error) {
	// 1. 执行创建
	result, err := r.Create(ctx, db, dto, viewMask)
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
func (r *Repository[DTO, ENTITY]) UpdateWithCache(
	ctx context.Context,
	db *gorm.DB,
	dto *DTO,
	updateMask *fieldmaskpb.FieldMask,
) (*DTO, error) {
	// 1. 执行更新
	result, err := r.Update(ctx, db, dto, updateMask)
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
func (r *Repository[DTO, ENTITY]) DeleteWithCache(
	ctx context.Context,
	db *gorm.DB,
	id any,
	notSoftDelete bool,
) (int64, error) {
	// 1. 执行删除
	affected, err := r.Delete(ctx, db.Where("id = ?", id), notSoftDelete)
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
func (r *Repository[DTO, ENTITY]) extractIDFromDTO(dto *DTO) any {
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
