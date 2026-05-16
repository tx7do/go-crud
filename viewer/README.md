# Viewer Package

查看者（Viewer）上下文管理包，提供统一的身份认证、权限控制和数据范围隔离功能。这是 go-crud 项目的安全和权限基础设施层，用于在多租户系统中实现细粒度的访问控制。

## 特性

- ✅ **统一上下文接口** - Context 接口封装所有身份信息
- ✅ **多租户支持** - TenantID 租户隔离
- ✅ **组织单元隔离** - OrgUnitID 组织维度数据权限
- ✅ **权限检查** - Permissions 和 HasPermission 方法
- ✅ **角色管理** - Roles 角色列表
- ✅ **数据范围控制** - DataScope 支持 SELF/UNIT/USER/ALL/NONE 五种范围
- ✅ **Trace ID** - 请求链路追踪
- ✅ **平台/租户视图** - IsPlatformContext / IsTenantContext 判断
- ✅ **系统任务识别** - IsSystemContext 区分后台任务
- ✅ **审计日志开关** - ShouldAudit 控制是否记录审计
- ✅ **Context 注入/提取** - WithContext / FromContext / MustFromContext
- ✅ **Noop 实现** - NewNoopContext 匿名/未授权用户默认实现

## 快速开始

### 1. 安装依赖

```bash
go get github.com/tx7do/go-crud/viewer
```

### 2. 创建 Viewer Context

#### 基本用法

```go
import (
    "context"
    "github.com/tx7do/go-crud/viewer"
)

// 创建一个简单的 Viewer Context（需要实现 viewer.Context 接口）
type myViewerContext struct {
    userID      uint64
    tenantID    uint64
    orgUnitID   uint64
    permissions []string
    roles       []string
    dataScopes  []viewer.DataScope
    traceID     string
}

func (c *myViewerContext) UserID() uint64                 { return c.userID }
func (c *myViewerContext) TenantID() uint64               { return c.tenantID }
func (c *myViewerContext) OrgUnitID() uint64              { return c.orgUnitID }
func (c *myViewerContext) Permissions() []string          { return c.permissions }
func (c *myViewerContext) Roles() []string                { return c.roles }
func (c *myViewerContext) DataScope() []viewer.DataScope  { return c.dataScopes }
func (c *myViewerContext) TraceID() string                { return c.traceID }
func (c *myViewerContext) HasPermission(action, resource string) bool {
    // 简单实现：检查权限列表中是否包含 "action:resource"
    perm := action + ":" + resource
    for _, p := range c.permissions {
        if p == perm {
            return true
        }
    }
    return false
}
func (c *myViewerContext) IsPlatformContext() bool { return c.tenantID == 0 }
func (c *myViewerContext) IsTenantContext() bool   { return c.tenantID > 0 }
func (c *myViewerContext) IsSystemContext() bool   { return c.userID == 0 }
func (c *myViewerContext) ShouldAudit() bool       { return true }

// 创建实例
vc := &myViewerContext{
    userID:      1001,
    tenantID:    1,
    orgUnitID:   10,
    permissions: []string{"read:user", "write:user", "delete:user"},
    roles:       []string{"admin", "manager"},
    dataScopes: []viewer.DataScope{
        {ScopeType: viewer.ScopeTypeUnit, TargetIDs: []uint64{10, 11, 12}},
    },
    traceID: "trace-12345",
}
```

---

### 3. 将 Context 注入到 request context

```go
ctx := context.Background()

// 注入 Viewer Context
ctx = viewer.WithContext(ctx, vc)

// 在后续的数据库操作或业务逻辑中使用
result, err := repo.ListWithPaging(ctx, req)
```

---

### 4. 从 request context 提取 Viewer Context

```go
// 方式 1：安全提取（返回 ok 标志）
vc, ok := viewer.FromContext(ctx)
if !ok {
    return errors.New("viewer context not found")
}

// 方式 2：强制提取（不存在则返回 NoopContext）
vc := viewer.MustFromContext(ctx)

// 使用 Viewer Context
userID := vc.UserID()
tenantID := vc.TenantID()
```

---

### 5. 权限检查

```go
vc := viewer.MustFromContext(ctx)

// 检查是否有更新用户的权限
if !vc.HasPermission("update", "user") {
    return errors.New("permission denied")
}

// 检查是否有删除订单的权限
if !vc.HasPermission("delete", "order") {
    return errors.New("permission denied")
}
```

---

### 6. 数据范围控制

#### 获取数据范围

```go
vc := viewer.MustFromContext(ctx)
dataScopes := vc.DataScope()

for _, scope := range dataScopes {
    switch scope.ScopeType {
    case viewer.ScopeTypeSelf:
        // 仅限本人数据：WHERE created_by = :user_id
        fmt.Printf("Self scope: user_id = %d\n", vc.UserID())
        
    case viewer.ScopeTypeUnit:
        // 组织维度：WHERE org_unit_id IN (:target_ids)
        fmt.Printf("Unit scope: org_unit_ids = %v\n", scope.TargetIDs)
        
    case viewer.ScopeTypeUser:
        // 指定用户：WHERE created_by IN (:target_ids)
        fmt.Printf("User scope: user_ids = %v\n", scope.TargetIDs)
        
    case viewer.ScopeTypeAll:
        // 全部数据：不添加过滤条件
        fmt.Println("All scope: no filter")
        
    case viewer.ScopeTypeNone:
        // 禁止访问：拒绝查询
        fmt.Println("None scope: access denied")
    }
}
```

#### 根据数据范围构建 SQL 过滤条件

```go
func buildDataScopeFilter(vc viewer.Context, tableName string) (string, []any) {
    dataScopes := vc.DataScope()
    if len(dataScopes) == 0 {
        return "", nil
    }
    
    var conditions []string
    var args []any
    
    for _, scope := range dataScopes {
        switch scope.ScopeType {
        case viewer.ScopeTypeSelf:
            conditions = append(conditions, fmt.Sprintf("%s.created_by = ?", tableName))
            args = append(args, vc.UserID())
            
        case viewer.ScopeTypeUnit:
            if len(scope.TargetIDs) > 0 {
                placeholders := make([]string, len(scope.TargetIDs))
                for i := range scope.TargetIDs {
                    placeholders[i] = "?"
                    args = append(args, scope.TargetIDs[i])
                }
                conditions = append(conditions, 
                    fmt.Sprintf("%s.org_unit_id IN (%s)", tableName, strings.Join(placeholders, ",")))
            }
            
        case viewer.ScopeTypeUser:
            if len(scope.TargetIDs) > 0 {
                placeholders := make([]string, len(scope.TargetIDs))
                for i := range scope.TargetIDs {
                    placeholders[i] = "?"
                    args = append(args, scope.TargetIDs[i])
                }
                conditions = append(conditions,
                    fmt.Sprintf("%s.created_by IN (%s)", tableName, strings.Join(placeholders, ",")))
            }
            
        case viewer.ScopeTypeAll:
            // 不添加过滤条件
            
        case viewer.ScopeTypeNone:
            // 返回永远为假的条件
            return "1 = 0", nil
        }
    }
    
    if len(conditions) == 0 {
        return "", nil
    }
    
    return strings.Join(conditions, " OR "), args
}
```

---

### 7. 租户隔离

```go
vc := viewer.MustFromContext(ctx)

// 判断是否为平台管理视图（tenant_id == 0）
if vc.IsPlatformContext() {
    // 平台管理员可以查看所有租户的数据
    fmt.Println("Platform admin view")
}

// 判断是否为租户业务视图（tenant_id > 0）
if vc.IsTenantContext() {
    // 租户用户只能查看自己租户的数据
    tenantID := vc.TenantID()
    fmt.Printf("Tenant view: tenant_id = %d\n", tenantID)
    
    // 自动添加租户过滤条件
    filter := fmt.Sprintf("tenant_id = %d", tenantID)
}

// 判断是否为系统后台任务（user_id == 0）
if vc.IsSystemContext() {
    // 系统任务可能不需要权限检查
    fmt.Println("System background task")
}
```

---

### 8. 审计日志控制

```go
vc := viewer.MustFromContext(ctx)

// 判断是否需要记录审计日志
if vc.ShouldAudit() {
    // 记录审计日志
    auditEntry := &audit.Entry{
        UserID:    vc.UserID(),
        TenantID:  vc.TenantID(),
        TraceID:   vc.TraceID(),
        Action:    "update",
        Resource:  "user",
        Timestamp: time.Now(),
    }
    auditor.Record(ctx, auditEntry)
}
```

---

### 9. 使用 NoopContext（匿名/未授权用户）

```go
// 创建匿名上下文
vc := viewer.NewNoopContext()

// 所有方法返回默认值
fmt.Println(vc.UserID())         // 0
fmt.Println(vc.TenantID())       // 0
fmt.Println(vc.Permissions())    // nil
fmt.Println(vc.HasPermission("read", "user")) // false
fmt.Println(vc.ShouldAudit())    // false

// 适用于：
// - 公开接口（无需登录）
// - 未找到用户信息的兜底处理
// - 测试环境
```

---

## API 参考

### Context 接口

```go
type Context interface {
    // 身份标识
    UserID() uint64      // 当前用户ID
    TenantID() uint64    // 租户ID
    OrgUnitID() uint64   // 组织单元ID
    
    // 权限与角色
    Permissions() []string           // 权限列表
    Roles() []string                 // 角色列表
    HasPermission(action, resource string) bool  // 权限检查
    
    // 数据范围
    DataScope() []DataScope  // 数据权限范围
    
    // 追踪信息
    TraceID() string  // 请求追踪ID
    
    // 上下文类型判断
    IsPlatformContext() bool  // 是否为平台管理视图（tenant_id == 0）
    IsTenantContext() bool    // 是否为租户业务视图（tenant_id > 0）
    IsSystemContext() bool    // 是否为系统后台任务（user_id == 0）
    
    // 审计控制
    ShouldAudit() bool  // 是否需要记录审计日志
}
```

### DataScope 结构

```go
type DataScope struct {
    ScopeType ScopeType `json:"st,omitempty"`  // 数据权限范围类型
    TargetIDs []uint64  `json:"ids,omitempty"` // 具体的 ID 集合
}
```

### ScopeType 枚举

```go
type ScopeType string

const (
    // 仅限本人创建/拥有的数据
    // SQL: WHERE created_by = :user_id
    ScopeTypeSelf ScopeType = "SELF"
    
    // 组织维度隔离
    // SQL: WHERE org_unit_id IN (:target_ids)
    // 如果是"本部门及下级"，TargetIDs 包含展开后的所有子 ID
    // 如果是"仅本部门"，TargetIDs 只包含当前部门 ID
    ScopeTypeUnit ScopeType = "UNIT"
    
    // 指定的用户列表
    // SQL: WHERE created_by IN (:target_ids)
    ScopeTypeUser ScopeType = "USER"
    
    // 全量放行（不注入过滤条件）
    ScopeTypeAll ScopeType = "ALL"
    
    // 禁止任何数据访问（拒绝策略）
    // SQL: 1 = 0（永远为假）
    ScopeTypeNone ScopeType = "NONE"
)
```

### Context 管理函数

```go
// WithContext 将 Context 注入 context
func WithContext(ctx context.Context, vc Context) context.Context

// FromContext 从 context 中提取 Context
func FromContext(ctx context.Context) (Context, bool)

// MustFromContext 从 context 中提取 Context，若不存在则返回 NoopContext
func MustFromContext(ctx context.Context) Context
```

### NoopContext

```go
// NewNoopContext 创建一个匿名上下文实例
func NewNoopContext() Context
```

**特点：**
- 所有 ID 返回 0
- Permissions 和 Roles 返回 nil
- HasPermission 始终返回 false
- ShouldAudit 返回 false
- 适用于匿名访问或兜底处理

---

## 数据范围详解

### ScopeTypeSelf（仅限本人）

**适用场景：**
- 个人工作台
- 我的订单
- 我的任务

**SQL 示例：**
```sql
SELECT * FROM orders WHERE created_by = 1001
```

**Go 代码：**
```go
dataScope := viewer.DataScope{
    ScopeType: viewer.ScopeTypeSelf,
}
```

---

### ScopeTypeUnit（组织维度）

**适用场景：**
- 部门经理查看本部门数据
- 区域经理查看本区域及下级区域数据

**SQL 示例：**
```sql
-- 仅本部门
SELECT * FROM orders WHERE org_unit_id = 10

-- 本部门及下级
SELECT * FROM orders WHERE org_unit_id IN (10, 11, 12, 13)
```

**Go 代码：**
```go
// 仅本部门
dataScope := viewer.DataScope{
    ScopeType: viewer.ScopeTypeUnit,
    TargetIDs: []uint64{10},
}

// 本部门及下级（需要提前展开子组织 ID）
dataScope := viewer.DataScope{
    ScopeType: viewer.ScopeTypeUnit,
    TargetIDs: []uint64{10, 11, 12, 13},
}
```

**组织树展开示例：**
```go
// 递归获取所有子组织 ID
func getAllChildOrgIDs(orgID uint64) []uint64 {
    ids := []uint64{orgID}
    children := getChildren(orgID)  // 从数据库或缓存获取子组织
    for _, child := range children {
        ids = append(ids, getAllChildOrgIDs(child.ID)...)
    }
    return ids
}

orgIDs := getAllChildOrgIDs(10)
dataScope := viewer.DataScope{
    ScopeType: viewer.ScopeTypeUnit,
    TargetIDs: orgIDs,
}
```

---

### ScopeTypeUser（指定用户）

**适用场景：**
- 主管查看指定下属的数据
- 协作成员查看协作者的数据

**SQL 示例：**
```sql
SELECT * FROM tasks WHERE created_by IN (1001, 1002, 1003)
```

**Go 代码：**
```go
dataScope := viewer.DataScope{
    ScopeType: viewer.ScopeTypeUser,
    TargetIDs: []uint64{1001, 1002, 1003},
}
```

---

### ScopeTypeAll（全量放行）

**适用场景：**
- 平台超级管理员
- 系统配置表（无租户隔离）

**SQL 示例：**
```sql
SELECT * FROM orders  -- 无过滤条件
```

**Go 代码：**
```go
dataScope := viewer.DataScope{
    ScopeType: viewer.ScopeTypeAll,
}
```

---

### ScopeTypeNone（禁止访问）

**适用场景：**
- 权限被撤销
- 黑名单用户
- 临时封禁

**SQL 示例：**
```sql
SELECT * FROM orders WHERE 1 = 0  -- 永远返回空结果
```

**Go 代码：**
```go
dataScope := viewer.DataScope{
    ScopeType: viewer.ScopeTypeNone,
}
```

---

## 最佳实践

### 1. 在中间件中注入 Viewer Context

```go
// HTTP 中间件示例
func ViewerMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        // 从 JWT Token 或 Session 中提取用户信息
        token := r.Header.Get("Authorization")
        claims, err := parseJWT(token)
        if err != nil {
            // 未登录，使用 NoopContext
            ctx := viewer.WithContext(r.Context(), viewer.NewNoopContext())
            next.ServeHTTP(w, r.WithContext(ctx))
            return
        }
        
        // 构建 Viewer Context
        vc := &myViewerContext{
            userID:      claims.UserID,
            tenantID:    claims.TenantID,
            orgUnitID:   claims.OrgUnitID,
            permissions: claims.Permissions,
            roles:       claims.Roles,
            dataScopes:  buildDataScopes(claims),
            traceID:     r.Header.Get("X-Trace-ID"),
        }
        
        // 注入到 context
        ctx := viewer.WithContext(r.Context(), vc)
        next.ServeHTTP(w, r.WithContext(ctx))
    })
}
```

---

### 2. 在 Repository 中自动应用数据范围

```go
func (r *Repository[DTO, ENTITY]) ListWithPaging(ctx context.Context, req *paginationV1.PagingRequest) (*PagingResult[DTO], error) {
    vc := viewer.MustFromContext(ctx)
    
    // 自动添加租户过滤
    if vc.IsTenantContext() {
        req.FilterExpr = addTenantFilter(req.FilterExpr, vc.TenantID())
    }
    
    // 自动添加数据范围过滤
    dataScopes := vc.DataScope()
    if len(dataScopes) > 0 {
        req.FilterExpr = addDataScopeFilter(req.FilterExpr, dataScopes, vc)
    }
    
    // 执行查询
    return r.executeList(ctx, req)
}

func addTenantFilter(expr *paginationV1.FilterExpr, tenantID uint64) *paginationV1.FilterExpr {
    if expr == nil {
        expr = &paginationV1.FilterExpr{}
    }
    
    expr.Conditions = append(expr.Conditions, &paginationV1.FilterCondition{
        Field: "tenant_id",
        Op:    paginationV1.Operator_EQ,
        Value: &paginationV1.FilterCondition_ValueOneof{
            Value: fmt.Sprintf("%d", tenantID),
        },
    })
    
    return expr
}
```

---

### 3. 权限检查装饰器

```go
// 权限检查装饰器
func RequirePermission(action, resource string) func(http.Handler) http.Handler {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            vc := viewer.MustFromContext(r.Context())
            
            if !vc.HasPermission(action, resource) {
                http.Error(w, "permission denied", http.StatusForbidden)
                return
            }
            
            next.ServeHTTP(w, r)
        })
    }
}

// 使用示例
mux.Handle("/api/users", RequirePermission("read", "user")(userHandler))
mux.Handle("/api/users/create", RequirePermission("write", "user")(createUserHandler))
mux.Handle("/api/users/delete", RequirePermission("delete", "user")(deleteUserHandler))
```

---

### 4. 多数据范围组合

```go
// 用户可以有多个数据范围（OR 关系）
dataScopes := []viewer.DataScope{
    // 本人的数据
    {ScopeType: viewer.ScopeTypeSelf},
    // 本部门的数据
    {ScopeType: viewer.ScopeTypeUnit, TargetIDs: []uint64{10}},
    // 指定协作者的数据
    {ScopeType: viewer.ScopeTypeUser, TargetIDs: []uint64{1002, 1003}},
}

// 生成的 SQL：
// WHERE created_by = 1001 
//    OR org_unit_id IN (10)
//    OR created_by IN (1002, 1003)
```

---

### 5. 平台管理员特殊处理

```go
vc := viewer.MustFromContext(ctx)

if vc.IsPlatformContext() {
    // 平台管理员：可以查看所有租户的数据
    // 不添加租户过滤条件
    fmt.Println("Platform admin: no tenant filter")
} else if vc.IsTenantContext() {
    // 租户用户：只能查看自己租户的数据
    tenantID := vc.TenantID()
    filter := fmt.Sprintf("tenant_id = %d", tenantID)
    fmt.Printf("Tenant user: %s\n", filter)
}
```

---

### 6. 系统任务跳过权限检查

```go
vc := viewer.MustFromContext(ctx)

if vc.IsSystemContext() {
    // 系统后台任务：跳过权限检查
    fmt.Println("System task: skip permission check")
    return executeTask(ctx)
}

// 普通用户：需要权限检查
if !vc.HasPermission("execute", "task") {
    return errors.New("permission denied")
}
```

---

### 7. 审计日志优化

```go
vc := viewer.MustFromContext(ctx)

// 只对重要操作记录审计日志
shouldAudit := vc.ShouldAudit() && isImportantAction(action)

if shouldAudit {
    auditEntry := &audit.Entry{
        UserID:    vc.UserID(),
        TenantID:  vc.TenantID(),
        TraceID:   vc.TraceID(),
        Action:    action,
        Resource:  resource,
        Timestamp: time.Now(),
    }
    auditor.Record(ctx, auditEntry)
}
```

---

### 8. 缓存组织树

```go
// 避免每次都查询数据库展开组织树
var orgTreeCache = sync.Map{}

func getCachedChildOrgIDs(orgID uint64) []uint64 {
    if cached, ok := orgTreeCache.Load(orgID); ok {
        return cached.([]uint64)
    }
    
    ids := getAllChildOrgIDs(orgID)
    orgTreeCache.Store(orgID, ids)
    
    // 设置过期时间（实际项目中应使用带过期的缓存）
    time.AfterFunc(5*time.Minute, func() {
        orgTreeCache.Delete(orgID)
    })
    
    return ids
}
```

---

## 与其他包的集成

### 与 Audit 包集成

```go
import (
    "github.com/tx7do/go-crud/viewer"
    "github.com/tx7do/go-crud/audit"
)

vc := viewer.MustFromContext(ctx)

// 记录审计日志
entry := &audit.Entry{
    TraceID:   vc.TraceID(),
    UserID:    vc.UserID(),
    TenantID:  vc.TenantID(),
    Username:  getUsername(vc.UserID()),
    Action:    "update",
    Resource:  "user",
    Operation: audit.Operation_UPDATE,
    Status:    audit.Status_SUCCESS,
    Timestamp: time.Now(),
}

auditor.Record(ctx, entry)
```

---

### 与 GORM 包集成

```go
import (
    "github.com/tx7do/go-crud/viewer"
    "github.com/tx7do/go-crud/gorm"
)

// 在 Repository 中自动应用数据范围
func (r *Repository[DTO, ENTITY]) applyDataScope(ctx context.Context, db *gorm.DB) *gorm.DB {
    vc := viewer.MustFromContext(ctx)
    
    // 租户隔离
    if vc.IsTenantContext() {
        db = db.Where("tenant_id = ?", vc.TenantID())
    }
    
    // 数据范围
    for _, scope := range vc.DataScope() {
        switch scope.ScopeType {
        case viewer.ScopeTypeSelf:
            db = db.Where("created_by = ?", vc.UserID())
        case viewer.ScopeTypeUnit:
            db = db.Where("org_unit_id IN ?", scope.TargetIDs)
        case viewer.ScopeTypeUser:
            db = db.Where("created_by IN ?", scope.TargetIDs)
        case viewer.ScopeTypeNone:
            db = db.Where("1 = 0")  // 禁止访问
        }
    }
    
    return db
}
```

---

### 与 Entgo 包集成

```go
import (
    "github.com/tx7do/go-crud/viewer"
    "github.com/tx7do/go-crud/entgo"
)

// 在 Query Hook 中应用数据范围
func (r *Repository[DTO, ENTITY]) applyDataScope(ctx context.Context, query *ent.UserQuery) *ent.UserQuery {
    vc := viewer.MustFromContext(ctx)
    
    // 租户隔离
    if vc.IsTenantContext() {
        query = query.Where(user.TenantIDEQ(vc.TenantID()))
    }
    
    // 数据范围
    var predicates []predicate.User
    for _, scope := range vc.DataScope() {
        switch scope.ScopeType {
        case viewer.ScopeTypeSelf:
            predicates = append(predicates, user.CreatedByEQ(vc.UserID()))
        case viewer.ScopeTypeUnit:
            predicates = append(predicates, user.OrgUnitIDIn(scope.TargetIDs...))
        case viewer.ScopeTypeUser:
            predicates = append(predicates, user.CreatedByIn(scope.TargetIDs...))
        case viewer.ScopeTypeNone:
            return query.Where(user.IDEQ(0))  // 禁止访问
        }
    }
    
    if len(predicates) > 0 {
        query = query.Where(user.Or(predicates...))
    }
    
    return query
}
```

---

### 与 MongoDB 包集成

```go
import (
    "github.com/tx7do/go-crud/viewer"
    "go.mongodb.org/mongo-driver/bson"
)

// 构建 MongoDB 过滤条件
func buildMongoFilter(ctx context.Context, baseFilter bson.M) bson.M {
    vc := viewer.MustFromContext(ctx)
    
    filter := bson.M{}
    
    // 复制基础过滤条件
    for k, v := range baseFilter {
        filter[k] = v
    }
    
    // 租户隔离
    if vc.IsTenantContext() {
        filter["tenant_id"] = vc.TenantID()
    }
    
    // 数据范围
    var orConditions []bson.M
    for _, scope := range vc.DataScope() {
        switch scope.ScopeType {
        case viewer.ScopeTypeSelf:
            orConditions = append(orConditions, bson.M{"created_by": vc.UserID()})
        case viewer.ScopeTypeUnit:
            orConditions = append(orConditions, bson.M{"org_unit_id": bson.M{"$in": scope.TargetIDs}})
        case viewer.ScopeTypeUser:
            orConditions = append(orConditions, bson.M{"created_by": bson.M{"$in": scope.TargetIDs}})
        case viewer.ScopeTypeNone:
            return bson.M{"_id": bson.M{"$exists": false}}  // 禁止访问
        }
    }
    
    if len(orConditions) > 0 {
        filter["$or"] = orConditions
    }
    
    return filter
}
```

---

## 完整示例

### 示例 1：HTTP 中间件 + Repository

```go
package main

import (
    "context"
    "net/http"
    "github.com/tx7do/go-crud/viewer"
    "github.com/tx7do/go-crud/gorm"
)

// Viewer 中间件
func ViewerMiddleware(jwtSecret string) func(http.Handler) http.Handler {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            // 从 JWT 提取用户信息
            token := r.Header.Get("Authorization")
            claims, err := verifyJWT(token, jwtSecret)
            
            var vc viewer.Context
            if err != nil {
                // 未登录
                vc = viewer.NewNoopContext()
            } else {
                // 已登录
                vc = &appViewerContext{
                    userID:      claims.UserID,
                    tenantID:    claims.TenantID,
                    orgUnitID:   claims.OrgUnitID,
                    permissions: claims.Permissions,
                    roles:       claims.Roles,
                    dataScopes:  buildDataScopes(claims),
                    traceID:     r.Header.Get("X-Trace-ID"),
                }
            }
            
            // 注入到 context
            ctx := viewer.WithContext(r.Context(), vc)
            next.ServeHTTP(w, r.WithContext(ctx))
        })
    }
}

// Handler
func listUsersHandler(repo *gorm.Repository[UserDTO, UserEntity]) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        ctx := r.Context()
        vc := viewer.MustFromContext(ctx)
        
        // 权限检查
        if !vc.HasPermission("read", "user") {
            http.Error(w, "permission denied", http.StatusForbidden)
            return
        }
        
        // 查询列表（自动应用数据范围）
        req := &paginationV1.PagingRequest{
            Page:     proto.Uint32(1),
            PageSize: proto.Uint32(10),
        }
        
        result, err := repo.ListWithPaging(ctx, req)
        if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
        }
        
        json.NewEncoder(w).Encode(result)
    }
}
```

---

### 示例 2：多租户 SaaS 应用

```go
// SaaS 应用的典型数据范围配置
func buildSaaSDataScopes(role string, userID, orgUnitID uint64) []viewer.DataScope {
    switch role {
    case "super_admin":
        // 超级管理员：查看所有数据
        return []viewer.DataScope{
            {ScopeType: viewer.ScopeTypeAll},
        }
        
    case "tenant_admin":
        // 租户管理员：查看本租户所有数据
        return []viewer.DataScope{
            {ScopeType: viewer.ScopeTypeAll},
        }
        
    case "department_manager":
        // 部门经理：查看本部门及下级数据
        childOrgIDs := getAllChildOrgIDs(orgUnitID)
        return []viewer.DataScope{
            {ScopeType: viewer.ScopeTypeUnit, TargetIDs: childOrgIDs},
        }
        
    case "team_leader":
        // 团队组长：查看本人 + 团队成员数据
        teamMemberIDs := getTeamMemberIDs(userID)
        return []viewer.DataScope{
            {ScopeType: viewer.ScopeTypeSelf},
            {ScopeType: viewer.ScopeTypeUser, TargetIDs: teamMemberIDs},
        }
        
    default:
        // 普通员工：仅查看本人数据
        return []viewer.DataScope{
            {ScopeType: viewer.ScopeTypeSelf},
        }
    }
}
```

---

## 测试

运行测试：

```bash
go test -v ./viewer/...
```

---

## 依赖

- `context` - Go 标准库 context 包
- `github.com/tx7do/go-crud/audit` - 审计日志包（可选）

---

## 架构设计

### 核心概念

```
┌─────────────────────────────────────┐
│         Viewer Context              │
├─────────────────────────────────────┤
│  Identity:                          │
│  - UserID                           │
│  - TenantID                         │
│  - OrgUnitID                        │
├─────────────────────────────────────┤
│  Authorization:                     │
│  - Permissions                      │
│  - Roles                            │
│  - HasPermission()                  │
├─────────────────────────────────────┤
│  Data Scope:                        │
│  - SELF (本人)                      │
│  - UNIT (组织)                      │
│  - USER (指定用户)                  │
│  - ALL (全部)                       │
│  - NONE (禁止)                      │
├─────────────────────────────────────┤
│  Metadata:                          │
│  - TraceID                          │
│  - IsPlatformContext()              │
│  - IsTenantContext()                │
│  - IsSystemContext()                │
│  - ShouldAudit()                    │
└─────────────────────────────────────┘
```

### 工作流程

```
HTTP Request
    ↓
JWT/Session 验证
    ↓
构建 Viewer Context
    ↓
WithContext 注入到 request context
    ↓
Handler / Repository
    ↓
FromContext 提取 Viewer Context
    ↓
┌─────────────────────────────┐
│  权限检查                    │
│  HasPermission(action, res) │
└─────────────────────────────┘
    ↓
┌─────────────────────────────┐
│  数据范围过滤                │
│  - Tenant Filter             │
│  - DataScope Filter          │
└─────────────────────────────┘
    ↓
数据库查询
    ↓
┌─────────────────────────────┐
│  审计日志（如果需要）        │
│  ShouldAudit() → Record()   │
└─────────────────────────────┘
    ↓
Response
```

---

## 常见问题 FAQ

### Q: 什么时候使用 NoopContext？

**A:** 
- 公开接口（无需登录）
- 未找到用户信息的兜底处理
- 测试环境
- 系统初始化阶段

```go
// 公开接口
vc := viewer.NewNoopContext()
ctx := viewer.WithContext(ctx, vc)
```

---

### Q: 如何实现"本部门及下级"的数据范围？

**A:** 需要提前展开组织树，将所有子组织 ID 放入 TargetIDs：

```go
func getAllChildOrgIDs(orgID uint64) []uint64 {
    ids := []uint64{orgID}
    children := getChildren(orgID)
    for _, child := range children {
        ids = append(ids, getAllChildOrgIDs(child.ID)...)
    }
    return ids
}

childOrgIDs := getAllChildOrgIDs(10)
dataScope := viewer.DataScope{
    ScopeType: viewer.ScopeTypeUnit,
    TargetIDs: childOrgIDs,  // [10, 11, 12, 13, ...]
}
```

---

### Q: 如何处理多个数据范围（OR 关系）？

**A:** DataScope 是切片，多个范围之间是 OR 关系：

```go
dataScopes := []viewer.DataScope{
    {ScopeType: viewer.ScopeTypeSelf},  // 本人
    {ScopeType: viewer.ScopeTypeUnit, TargetIDs: []uint64{10}},  // 本部门
}

// 生成的 SQL：
// WHERE created_by = 1001 OR org_unit_id IN (10)
```

---

### Q: 平台管理员和租户管理员有什么区别？

**A:** 
- **平台管理员**：`tenant_id = 0`，可以查看所有租户的数据
- **租户管理员**：`tenant_id > 0`，只能查看本租户的数据

```go
if vc.IsPlatformContext() {
    // 平台管理员：不添加租户过滤
} else if vc.IsTenantContext() {
    // 租户管理员：添加租户过滤
    filter := fmt.Sprintf("tenant_id = %d", vc.TenantID())
}
```

---

### Q: 如何在单元测试中模拟 Viewer Context？

**A:** 创建 mock 实现：

```go
type mockViewerContext struct {
    userID   uint64
    tenantID uint64
}

func (m *mockViewerContext) UserID() uint64                 { return m.userID }
func (m *mockViewerContext) TenantID() uint64               { return m.tenantID }
func (m *mockViewerContext) OrgUnitID() uint64              { return 0 }
func (m *mockViewerContext) Permissions() []string          { return []string{"read:user"} }
func (m *mockViewerContext) Roles() []string                { return []string{"admin"} }
func (m *mockViewerContext) DataScope() []viewer.DataScope  { return nil }
func (m *mockViewerContext) TraceID() string                { return "test-trace" }
func (m *mockViewerContext) HasPermission(action, resource string) bool {
    return action == "read" && resource == "user"
}
func (m *mockViewerContext) IsPlatformContext() bool { return m.tenantID == 0 }
func (m *mockViewerContext) IsTenantContext() bool   { return m.tenantID > 0 }
func (m *mockViewerContext) IsSystemContext() bool   { return m.userID == 0 }
func (m *mockViewerContext) ShouldAudit() bool       { return false }

// 测试用例
func TestListUsers(t *testing.T) {
    vc := &mockViewerContext{userID: 1001, tenantID: 1}
    ctx := viewer.WithContext(context.Background(), vc)
    
    result, err := repo.ListWithPaging(ctx, req)
    // 断言...
}
```

---

### Q: 如何优化组织树展开性能？

**A:** 
1. **缓存组织树** - 使用 Redis 或内存缓存
2. **物化路径** - 在数据库中存储完整路径（如 `/10/11/12/`）
3. **闭包表** - 使用单独的表存储祖先-后代关系

```go
// 方案 1：缓存
var orgTreeCache = redis.Client

// 方案 2：物化路径
SELECT * FROM users WHERE org_path LIKE '/10/%'

// 方案 3：闭包表
SELECT u.* FROM users u
JOIN org_closure c ON u.org_unit_id = c.descendant
WHERE c.ancestor = 10
```

---

### Q: 如何处理跨租户查询（平台管理员）？

**A:** 平台管理员不添加租户过滤条件：

```go
vc := viewer.MustFromContext(ctx)

query := db.Model(&User{})

if vc.IsTenantContext() {
    // 租户用户：添加租户过滤
    query = query.Where("tenant_id = ?", vc.TenantID())
}
// 平台管理员：不添加租户过滤，可以查询所有租户

users := query.Find(&users)
```

---

### Q: 如何记录审计日志？

**A:** 使用 `ShouldAudit()` 判断是否需要记录：

```go
vc := viewer.MustFromContext(ctx)

if vc.ShouldAudit() {
    entry := &audit.Entry{
        TraceID:   vc.TraceID(),
        UserID:    vc.UserID(),
        TenantID:  vc.TenantID(),
        Action:    "update",
        Resource:  "user",
        Timestamp: time.Now(),
    }
    auditor.Record(ctx, entry)
}
```

---

## 参考链接

- [Google AIP - Resource Annotations](https://google.aip.dev/auth/4112)
- [多租户架构模式](https://docs.microsoft.com/en-us/azure/architecture/multitenant-saas/)
- [RBAC 权限模型](https://en.wikipedia.org/wiki/Role-based_access_control)
- [数据权限设计](https://www.cnblogs.com/yunfeiyang-88/p/15398205.html)

---

## 许可证

本项目采用 MIT 许可证。
