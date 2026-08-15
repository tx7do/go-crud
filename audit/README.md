# Audit Package

统一的审计日志接口和数据结构，为数据访问层提供标准化的操作审计功能。

## 概述

本包定义了审计日志的标准接口和数据模型，支持记录 CRUD 操作的完整上下文信息，包括操作者、操作行为、数据变更、结果状态等。所有 go-crud 的数据访问实现都可以集成此审计功能，实现统一的操作追溯和安全合规。

## 特性

- ✅ **标准化接口** - 统一的 Auditor 接口，易于扩展和替换
- ✅ **完整上下文** - 记录操作者、租户、IP、UserAgent 等信息
- ✅ **数据变更追踪** - 记录操作前后的值（PreValue/PostValue）
- ✅ **全链路追踪** - 支持 TraceID，与分布式追踪系统集成
- ✅ **灵活存储** - 异步缓冲或同步写入，由实现决定
- ✅ **空实现支持** - NoopAuditor 用于测试和默认场景
- ✅ **Context 集成** - 通过 Context 传递 Auditor 实例
- ✅ **JSON 序列化** - Entry 结构支持 JSON 序列化，方便存储和查询

## 核心概念

### 1. Auditor 接口

```go
type Auditor interface {
    // Record 记录审计日志
    // 由调用者负责传入 context 和 entry
    // Auditor 内部决定是异步缓冲还是同步写入
    Record(ctx context.Context, entry *Entry) error

    // Flush 确保所有待处理的日志都被提交到最终存储
    // 应该在应用程序关闭、测试结束或需要强制持久化时调用
    Flush(ctx context.Context) error
}
```

**职责：**
- `Record()` - 记录单条审计日志
- `Flush()` - 刷新缓冲区，确保持久化

---

### 2. Entry 结构

审计日志条目，包含完整的操作上下文信息。

```go
type Entry struct {
    // --- 基础上下文 (Base Context) ---
    TraceID   string    `json:"trace_id"`   // 全链路追踪 ID
    Timestamp time.Time `json:"timestamp"`  // 发生时间（建议 UTC）

    // --- 操作者信息 (Viewer Data) ---
    UserID    uint64 `json:"user_id,omitempty"`    // 操作人 ID
    TenantID  uint64 `json:"tenant_id,omitempty"`  // 租户 ID
    Username  string `json:"username,omitempty"`   // 操作人账号名
    UserIP    string `json:"user_ip,omitempty"`    // 客户端 IP
    UserAgent string `json:"user_agent,omitempty"` // 客户端环境信息

    // --- 操作行为 (Action) ---
    Service  string `json:"service,omitempty"`  // 所属微服务名
    Module   string `json:"module,omitempty"`   // 业务模块
    Action   string `json:"action,omitempty"`   // 具体动作
    Resource string `json:"resource,omitempty"` // 操作的资源对象

    // --- 数据变更 (Data Changes) ---
    Operation Operation       `json:"operation,omitempty"`  // INSERT, UPDATE, DELETE
    TargetID  string          `json:"target_id,omitempty"`  // 被操作对象的 ID
    PreValue  json.RawMessage `json:"pre_value,omitempty"`  // 变更前的值
    PostValue json.RawMessage `json:"post_value,omitempty"` // 变更后的值

    // --- 结果状态 (Result) ---
    Status       Status `json:"status"`                  // 0-成功，1-失败
    ErrorMessage string `json:"error_message,omitempty"` // 失败原因
    CostMS       int64  `json:"cost_ms,omitempty"`       // 操作耗时（毫秒）

    // --- 扩展字段 (Metadata) ---
    Extra map[string]any `json:"extra,omitempty"` // 扩展信息
}
```

#### 字段详解

##### 基础上下文

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| `TraceID` | string | 全链路追踪 ID | `"abc123-def456"` |
| `Timestamp` | time.Time | 操作发生时间（UTC） | `2024-01-01T12:00:00Z` |

##### 操作者信息

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| `UserID` | uint64 | 操作人 ID | `1001` |
| `TenantID` | uint64 | 租户 ID（多租户系统） | `100` |
| `Username` | string | 操作人账号名（冗余存储） | `"john.doe"` |
| `UserIP` | string | 客户端 IP | `"192.168.1.100"` |
| `UserAgent` | string | 客户端环境信息 | `"Mozilla/5.0..."` |

**注意：** `Username` 冗余存储是为了防止用户删除后无法溯源。

##### 操作行为

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| `Service` | string | 所属微服务名 | `"user-service"` |
| `Module` | string | 业务模块 | `"订单"`, `"用户"`, `"权限"` |
| `Action` | string | 具体动作 | `"Create"`, `"Update"`, `"Login"`, `"Export"` |
| `Resource` | string | 操作的资源对象 | `"user_table"`, `"order_123"` |

##### 数据变更

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| `Operation` | Operation | 数据库操作类型 | `INSERT`, `UPDATE`, `DELETE`, `UPSERT` |
| `TargetID` | string | 被操作对象的 ID | `"user-001"` |
| `PreValue` | json.RawMessage | 变更前的值 | `{"name":"old"}` |
| `PostValue` | json.RawMessage | 变更后的值 | `{"name":"new"}` |

**注意：** 
- `PreValue` 和 `PostValue` 使用 JSON 格式存储
- 敏感表建议开启变更记录
- 大数据量时谨慎使用，可能影响性能

##### 结果状态

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| `Status` | Status | 状态码 | `0`-成功，`1`-失败 |
| `ErrorMessage` | string | 失败原因 | `"constraint violation"` |
| `CostMS` | int64 | 操作耗时（毫秒） | `150` |

##### 扩展字段

| 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|
| `Extra` | map[string]any | 自定义扩展信息 | `{"scope":"admin"}` |

---

### 3. Operation 枚举

```go
type Operation string

const (
    OpInsert Operation = "INSERT"  // 插入
    OpUpdate Operation = "UPDATE"  // 更新
    OpUpsert Operation = "UPSERT"  // 插入或更新
    OpDelete Operation = "DELETE"  // 删除
)
```

---

### 4. Status 枚举

```go
type Status int

const (
    StatusOK   Status = 0  // 成功
    StatusFail Status = 1  // 失败
)
```

---

## 快速开始

### 1. 安装依赖

```bash
go get github.com/tx7do/go-crud/audit
```

### 2. 实现 Auditor 接口

#### 示例：控制台输出 Auditor

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "time"

    "github.com/tx7do/go-crud/audit"
)

type ConsoleAuditor struct{}

func (a *ConsoleAuditor) Record(ctx context.Context, entry *audit.Entry) error {
    data, _ := json.MarshalIndent(entry, "", "  ")
    fmt.Printf("[AUDIT] %s\n", string(data))
    return nil
}

func (a *ConsoleAuditor) Flush(ctx context.Context) error {
    fmt.Println("[AUDIT] Flushed")
    return nil
}
```

#### 示例：文件存储 Auditor

```go
import (
    "encoding/json"
    "os"
    "sync"
)

type FileAuditor struct {
    file   *os.File
    mu     sync.Mutex
    buffer []*audit.Entry
}

func NewFileAuditor(path string) (*FileAuditor, error) {
    f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return nil, err
    }
    return &FileAuditor{file: f}, nil
}

func (a *FileAuditor) Record(ctx context.Context, entry *audit.Entry) error {
    a.mu.Lock()
    defer a.mu.Unlock()

    a.buffer = append(a.buffer, entry)

    // 达到阈值时刷新
    if len(a.buffer) >= 100 {
        return a.flushInternal()
    }
    return nil
}

func (a *FileAuditor) Flush(ctx context.Context) error {
    a.mu.Lock()
    defer a.mu.Unlock()
    return a.flushInternal()
}

func (a *FileAuditor) flushInternal() error {
    for _, entry := range a.buffer {
        data, _ := json.Marshal(entry)
        a.file.Write(append(data, '\n'))
    }
    a.buffer = a.buffer[:0]
    return nil
}
```

#### 示例：数据库存储 Auditor

```go
import (
    "database/sql"
    "encoding/json"
)

type DatabaseAuditor struct {
    db *sql.DB
}

func NewDatabaseAuditor(db *sql.DB) *DatabaseAuditor {
    return &DatabaseAuditor{db: db}
}

func (a *DatabaseAuditor) Record(ctx context.Context, entry *audit.Entry) error {
    data, _ := json.Marshal(entry)

    _, err := a.db.ExecContext(ctx,
        `INSERT INTO audit_logs (trace_id, timestamp, user_id, tenant_id, 
                                  username, user_ip, service, module, action, 
                                  resource, operation, target_id, pre_value, 
                                  post_value, status, error_message, cost_ms, extra)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        entry.TraceID, entry.Timestamp, entry.UserID, entry.TenantID,
        entry.Username, entry.UserIP, entry.Service, entry.Module, entry.Action,
        entry.Resource, entry.Operation, entry.TargetID, entry.PreValue,
        entry.PostValue, entry.Status, entry.ErrorMessage, entry.CostMS,
        mustMarshalJSON(entry.Extra),
    )
    return err
}

func (a *DatabaseAuditor) Flush(ctx context.Context) error {
    // 数据库直接写入，无需刷新
    return nil
}

func mustMarshalJSON(v any) []byte {
    data, _ := json.Marshal(v)
    return data
}
```

#### 示例：Elasticsearch Auditor

```go
import (
    "bytes"
    "encoding/json"
    "net/http"
)

type ElasticsearchAuditor struct {
    endpoint string
    index    string
    client   *http.Client
}

func NewElasticsearchAuditor(endpoint, index string) *ElasticsearchAuditor {
    return &ElasticsearchAuditor{
        endpoint: endpoint,
        index:    index,
        client:   &http.Client{},
    }
}

func (a *ElasticsearchAuditor) Record(ctx context.Context, entry *audit.Entry) error {
    data, _ := json.Marshal(entry)

    url := fmt.Sprintf("%s/%s/_doc", a.endpoint, a.index)
    req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(data))
    req.Header.Set("Content-Type", "application/json")

    resp, err := a.client.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    if resp.StatusCode >= 400 {
        return fmt.Errorf("elasticsearch returned status %d", resp.StatusCode)
    }
    return nil
}

func (a *ElasticsearchAuditor) Flush(ctx context.Context) error {
    // ES 直接写入，无需刷新
    return nil
}
```

---

### 3. 使用 Context 传递 Auditor

```go
import "github.com/tx7do/go-crud/audit"

// 创建 Auditor
auditor := &ConsoleAuditor{}

// 注入到 Context
ctx := audit.WithAuditor(context.Background(), auditor)

// 在业务逻辑中使用
func handleRequest(ctx context.Context) {
    // 从 Context 获取 Auditor
    aud, ok := audit.FromContext(ctx)
    if !ok {
        // 没有 Auditor，使用空实现
        aud = audit.NewNoopAuditor()
    }

    // 记录审计日志
    entry := &audit.Entry{
        TraceID:   "trace-123",
        Timestamp: time.Now().UTC(),
        UserID:    1001,
        Username:  "john.doe",
        Service:   "user-service",
        Module:    "用户管理",
        Action:    "Create",
        Resource:  "user_table",
        Operation: audit.OpInsert,
        TargetID:  "user-001",
        Status:    audit.StatusOK,
    }

    if err := aud.Record(ctx, entry); err != nil {
        log.Printf("Failed to record audit: %v", err)
    }
}
```

---

### 4. 便捷方法

#### MustFromContext

自动处理 Auditor 不存在的情况：

```go
// 如果 Context 中没有 Auditor，返回 NoopAuditor
aud := audit.MustFromContext(ctx)

// 直接使用，无需检查
err := aud.Record(ctx, entry)
```

#### NewNoopAuditor

空实现，用于测试或默认场景：

```go
// 不执行任何操作
noop := audit.NewNoopAuditor()
err := noop.Record(ctx, entry)  // 总是返回 nil
```

---

### 5. 设置数据变更值

#### SetPreValue / SetPostValue

```go
entry := &audit.Entry{
    Operation: audit.OpUpdate,
    TargetID:  "user-001",
}

// 设置变更前的值
oldUser := User{Name: "Old Name", Email: "old@example.com"}
if err := entry.SetPreValue(oldUser); err != nil {
    log.Fatal(err)
}

// 设置变更后的值
newUser := User{Name: "New Name", Email: "new@example.com"}
if err := entry.SetPostValue(newUser); err != nil {
    log.Fatal(err)
}

// 序列化后的 JSON
fmt.Println(string(entry.PreValue))  // {"name":"Old Name","email":"old@example.com"}
fmt.Println(string(entry.PostValue)) // {"name":"New Name","email":"new@example.com"}
```

---

## 完整示例

### 示例 1：GORM Repository 集成审计

```go
import (
    "context"
    "time"

    "github.com/tx7do/go-crud/audit"
    "github.com/tx7do/go-crud/gorm"
)

type UserRepository struct {
    repo *gorm.Repository[User, UserEntity]
}

func (r *UserRepository) CreateUser(ctx context.Context, user *User) (*User, error) {
    start := time.Now()

    // 执行创建
    created, err := r.repo.Create(ctx, db, user, nil)

    // 记录审计日志
    aud := audit.MustFromContext(ctx)
    entry := &audit.Entry{
        Timestamp: time.Now().UTC(),
        UserID:    getCurrentUserID(ctx),
        Username:  getCurrentUsername(ctx),
        UserIP:    getClientIP(ctx),
        Service:   "user-service",
        Module:    "用户管理",
        Action:    "Create",
        Resource:  "users",
        Operation: audit.OpInsert,
        TargetID:  fmt.Sprintf("%d", created.Id),
        CostMS:    time.Since(start).Milliseconds(),
    }

    if err != nil {
        entry.Status = audit.StatusFail
        entry.ErrorMessage = err.Error()
    } else {
        entry.Status = audit.StatusOK
        entry.SetPostValue(created)
    }

    // 异步记录（不阻塞主流程）
    go func() {
        if recordErr := aud.Record(ctx, entry); recordErr != nil {
            log.Printf("Audit record failed: %v", recordErr)
        }
    }()

    return created, err
}

func (r *UserRepository) UpdateUser(ctx context.Context, user *User) (*User, error) {
    start := time.Now()

    // 查询旧值
    oldUser, _ := r.repo.Get(ctx, db.Where("id = ?", user.Id), nil)

    // 执行更新
    updated, err := r.repo.Update(ctx, db, user, nil)

    // 记录审计日志
    aud := audit.MustFromContext(ctx)
    entry := &audit.Entry{
        Timestamp: time.Now().UTC(),
        UserID:    getCurrentUserID(ctx),
        Username:  getCurrentUsername(ctx),
        UserIP:    getClientIP(ctx),
        Service:   "user-service",
        Module:    "用户管理",
        Action:    "Update",
        Resource:  "users",
        Operation: audit.OpUpdate,
        TargetID:  fmt.Sprintf("%d", user.Id),
        CostMS:    time.Since(start).Milliseconds(),
    }

    if err != nil {
        entry.Status = audit.StatusFail
        entry.ErrorMessage = err.Error()
    } else {
        entry.Status = audit.StatusOK
        if oldUser != nil {
            entry.SetPreValue(oldUser)
        }
        entry.SetPostValue(updated)
    }

    go func() {
        aud.Record(ctx, entry)
    }()

    return updated, err
}
```

---

### 示例 2：中间件自动审计

```go
import (
    "net/http"
    "time"

    "github.com/tx7do/go-crud/audit"
)

// AuditMiddleware 自动记录 HTTP 请求的审计日志
func AuditMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        start := time.Now()

        // 创建审计条目
        entry := &audit.Entry{
            Timestamp: time.Now().UTC(),
            UserIP:    r.RemoteAddr,
            UserAgent: r.UserAgent(),
            Service:   "api-gateway",
            Module:    extractModule(r.URL.Path),
            Action:    r.Method,
            Resource:  r.URL.Path,
        }

        // 从请求中提取用户信息
        if userID := r.Header.Get("X-User-ID"); userID != "" {
            entry.UserID = parseUint64(userID)
        }
        if username := r.Header.Get("X-Username"); username != "" {
            entry.Username = username
        }
        if tenantID := r.Header.Get("X-Tenant-ID"); tenantID != "" {
            entry.TenantID = parseUint64(tenantID)
        }

        // 从 Context 获取 Auditor
        ctx := r.Context()
        aud := audit.MustFromContext(ctx)

        // 包装 ResponseWriter 以捕获状态码
        wrapped := &responseWriter{ResponseWriter: w}

        // 执行下一个处理器
        next.ServeHTTP(wrapped, r)

        // 填充结果信息
        entry.Status = audit.StatusOK
        if wrapped.statusCode >= 400 {
            entry.Status = audit.StatusFail
        }
        entry.CostMS = time.Since(start).Milliseconds()

        // 记录审计日志
        go func() {
            aud.Record(ctx, entry)
        }()
    })
}

type responseWriter struct {
    http.ResponseWriter
    statusCode int
}

func (w *responseWriter) WriteHeader(code int) {
    w.statusCode = code
    w.ResponseWriter.WriteHeader(code)
}
```

---

### 示例 3：批量操作审计

```go
func BatchDeleteUsers(ctx context.Context, ids []uint64) error {
    start := time.Now()
    aud := audit.MustFromContext(ctx)

    var successCount, failCount int
    var lastError error

    for _, id := range ids {
        err := deleteUser(ctx, id)
        if err != nil {
            failCount++
            lastError = err
        } else {
            successCount++
        }

        // 为每个删除操作记录审计日志
        entry := &audit.Entry{
            Timestamp: time.Now().UTC(),
            UserID:    getCurrentUserID(ctx),
            Username:  getCurrentUsername(ctx),
            Service:   "user-service",
            Module:    "用户管理",
            Action:    "BatchDelete",
            Resource:  "users",
            Operation: audit.OpDelete,
            TargetID:  fmt.Sprintf("%d", id),
            Status:    audit.StatusOK,
            CostMS:    time.Since(start).Milliseconds(),
        }

        if err != nil {
            entry.Status = audit.StatusFail
            entry.ErrorMessage = err.Error()
        }

        aud.Record(ctx, entry)
    }

    // 刷新审计日志
    aud.Flush(ctx)

    if failCount > 0 {
        return fmt.Errorf("batch delete completed with %d failures, last error: %v", failCount, lastError)
    }
    return nil
}
```

---

### 示例 4：应用程序关闭时刷新

```go
func main() {
    // 创建 Auditor
    auditor := NewFileAuditor("/var/log/audit.log")

    // 注入到 Context
    ctx := audit.WithAuditor(context.Background(), auditor)

    // 启动服务器
    server := NewServer(ctx)
    
    // 优雅关闭
    defer func() {
        // 确保所有审计日志都被刷新
        if err := auditor.Flush(ctx); err != nil {
            log.Printf("Failed to flush audit logs: %v", err)
        }
        server.Shutdown()
    }()

    server.Start()
}
```

---

## 最佳实践

### 1. 异步记录审计日志

```go
// ✅ 好的做法：异步记录，不阻塞主流程
go func() {
    if err := aud.Record(ctx, entry); err != nil {
        log.Printf("Audit record failed: %v", err)
    }
}()

// ❌ 避免：同步记录，影响性能
if err := aud.Record(ctx, entry); err != nil {
    return err  // 审计失败不应该影响主业务
}
```

---

### 2. 始终记录错误信息

```go
// ✅ 好的做法：记录失败原因
if err != nil {
    entry.Status = audit.StatusFail
    entry.ErrorMessage = err.Error()
}

// ❌ 避免：只记录状态，不记录原因
if err != nil {
    entry.Status = audit.StatusFail
}
```

---

### 3. 记录操作耗时

```go
start := time.Now()

// 执行业务逻辑
result, err := doSomething()

entry.CostMS = time.Since(start).Milliseconds()
```

**用途：**
- 性能监控
- 慢操作告警
- 容量规划

---

### 4. 敏感数据处理

```go
// ✅ 好的做法：脱敏敏感字段
user := &User{
    Name:     "John Doe",
    Password: "***",  // 脱敏
    Email:    maskEmail(user.Email),  // john***@example.com
}
entry.SetPostValue(user)

// ❌ 避免：记录明文密码
entry.SetPostValue(userWithPassword)
```

---

### 5. 合理控制 PreValue/PostValue

```go
// ✅ 好的做法：关键操作记录完整变更
if isCriticalOperation(action) {
    entry.SetPreValue(oldValue)
    entry.SetPostValue(newValue)
}

// ⚠️ 谨慎：大数据量时只记录关键字段
if dataSize > threshold {
    entry.SetPostValue(map[string]any{
        "id":    newValue.ID,
        "name":  newValue.Name,
        "count": len(newValue.Items),
    })
}
```

---

### 6. 使用 TraceID 关联日志

```go
// 从分布式追踪系统获取 TraceID
traceID := otel.GetTraceID(ctx)

entry := &audit.Entry{
    TraceID: traceID,
    // ...
}
```

**好处：**
- 关联应用日志、审计日志、追踪数据
- 完整的问题排查链路

---

### 7. 定期刷新审计日志

```go
// 定时刷新
ticker := time.NewTicker(5 * time.Second)
defer ticker.Stop()

for range ticker.C {
    if err := auditor.Flush(ctx); err != nil {
        log.Printf("Failed to flush audit logs: %v", err)
    }
}
```

---

### 8. 多租户隔离

```go
// 记录租户 ID
entry.TenantID = getCurrentTenantID(ctx)

// 在查询时按租户过滤
SELECT * FROM audit_logs WHERE tenant_id = ?
```

---

## 与其他包的集成

### GORM

```go
import "github.com/tx7do/go-crud/gorm"

// 在 Repository 中集成审计
func (r *Repository) CreateWithAudit(ctx context.Context, dto *DTO) (*DTO, error) {
    aud := audit.MustFromContext(ctx)
    
    start := time.Now()
    result, err := r.Create(ctx, db, dto, nil)
    
    entry := buildAuditEntry(ctx, "Create", audit.OpInsert, start, err, result)
    go aud.Record(ctx, entry)
    
    return result, err
}
```

### Ent

```go
import "github.com/tx7do/go-crud/entgo"

// 使用 Interceptor 自动审计
client.Use(entgo.AuditInterceptor(auditor))
```

### MongoDB

```go
import "github.com/tx7do/go-crud/mongodb"

// 在 Repository 中集成审计
func (r *Repository) InsertWithAudit(ctx context.Context, doc any) error {
    aud := audit.MustFromContext(ctx)
    
    start := time.Now()
    err := r.InsertOne(ctx, doc)
    
    entry := buildAuditEntry(ctx, "Insert", audit.OpInsert, start, err, doc)
    go aud.Record(ctx, entry)
    
    return err
}
```

---

## 数据库 Schema 示例

### PostgreSQL

```sql
CREATE TABLE audit_logs (
    id BIGSERIAL PRIMARY KEY,
    trace_id VARCHAR(255),
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    
    -- 操作者信息
    user_id BIGINT,
    tenant_id BIGINT,
    username VARCHAR(255),
    user_ip INET,
    user_agent TEXT,
    
    -- 操作行为
    service VARCHAR(255),
    module VARCHAR(255),
    action VARCHAR(255),
    resource VARCHAR(255),
    
    -- 数据变更
    operation VARCHAR(10),
    target_id VARCHAR(255),
    pre_value JSONB,
    post_value JSONB,
    
    -- 结果状态
    status SMALLINT,
    error_message TEXT,
    cost_ms BIGINT,
    
    -- 扩展字段
    extra JSONB,
    
    -- 索引
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- 常用查询索引
CREATE INDEX idx_audit_user_id ON audit_logs(user_id);
CREATE INDEX idx_audit_tenant_id ON audit_logs(tenant_id);
CREATE INDEX idx_audit_timestamp ON audit_logs(timestamp DESC);
CREATE INDEX idx_audit_operation ON audit_logs(operation);
CREATE INDEX idx_audit_resource ON audit_logs(resource);
```

### MySQL

```sql
CREATE TABLE audit_logs (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    trace_id VARCHAR(255),
    timestamp DATETIME NOT NULL,
    
    user_id BIGINT,
    tenant_id BIGINT,
    username VARCHAR(255),
    user_ip VARCHAR(45),
    user_agent TEXT,
    
    service VARCHAR(255),
    module VARCHAR(255),
    action VARCHAR(255),
    resource VARCHAR(255),
    
    operation VARCHAR(10),
    target_id VARCHAR(255),
    pre_value JSON,
    post_value JSON,
    
    status TINYINT,
    error_message TEXT,
    cost_ms BIGINT,
    
    extra JSON,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    INDEX idx_user_id (user_id),
    INDEX idx_tenant_id (tenant_id),
    INDEX idx_timestamp (timestamp DESC),
    INDEX idx_operation (operation),
    INDEX idx_resource (resource)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
```

### Elasticsearch Mapping

```json
{
  "mappings": {
    "properties": {
      "trace_id": { "type": "keyword" },
      "timestamp": { "type": "date" },
      "user_id": { "type": "long" },
      "tenant_id": { "type": "long" },
      "username": { "type": "keyword" },
      "user_ip": { "type": "ip" },
      "service": { "type": "keyword" },
      "module": { "type": "keyword" },
      "action": { "type": "keyword" },
      "resource": { "type": "keyword" },
      "operation": { "type": "keyword" },
      "target_id": { "type": "keyword" },
      "status": { "type": "integer" },
      "cost_ms": { "type": "long" },
      "extra": { "type": "object" }
    }
  }
}
```

---

## 常见查询场景

### 1. 查询用户的操作历史

```sql
SELECT * FROM audit_logs
WHERE user_id = 1001
ORDER BY timestamp DESC
LIMIT 100;
```

### 2. 查询某资源的变更历史

```sql
SELECT * FROM audit_logs
WHERE resource = 'users' AND target_id = 'user-001'
ORDER BY timestamp DESC;
```

### 3. 查询失败的操作

```sql
SELECT * FROM audit_logs
WHERE status = 1
  AND timestamp > NOW() - INTERVAL '1 hour'
ORDER BY timestamp DESC;
```

### 4. 统计各模块的操作次数

```sql
SELECT module, action, COUNT(*) as count
FROM audit_logs
WHERE timestamp > NOW() - INTERVAL '1 day'
GROUP BY module, action
ORDER BY count DESC;
```

### 5. 查询某 IP 的异常操作

```sql
SELECT * FROM audit_logs
WHERE user_ip = '192.168.1.100'
  AND status = 1
  AND timestamp > NOW() - INTERVAL '24 hours';
```

---

## 安全合规

### GDPR 合规

- **数据最小化**：只记录必要的审计信息
- **目的限制**：明确审计日志的使用目的
- **存储限制**：设置合理的保留期限（如 90 天、180 天）
- **访问控制**：严格限制审计日志的访问权限

### 数据保留策略

```sql
-- 删除 180 天前的审计日志
DELETE FROM audit_logs
WHERE timestamp < NOW() - INTERVAL '180 days';
```

---

## 性能优化

### 1. 批量写入

```go
type BufferedAuditor struct {
    buffer []*audit.Entry
    mu     sync.Mutex
}

func (a *BufferedAuditor) Record(ctx context.Context, entry *audit.Entry) error {
    a.mu.Lock()
    a.buffer = append(a.buffer, entry)
    
    if len(a.buffer) >= 1000 {
        go a.flush()
    }
    a.mu.Unlock()
    return nil
}
```

### 2. 异步处理

```go
// 使用通道缓冲
ch := make(chan *audit.Entry, 10000)

go func() {
    for entry := range ch {
        writeToStorage(entry)
    }
}()
```

### 3. 分区表

```sql
-- PostgreSQL 按月分区
CREATE TABLE audit_logs_2024_01 PARTITION OF audit_logs
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
```

---

## 测试

### 单元测试

```go
func TestConsoleAuditor(t *testing.T) {
    auditor := &ConsoleAuditor{}
    ctx := context.Background()

    entry := &audit.Entry{
        Timestamp: time.Now().UTC(),
        UserID:    1001,
        Action:    "Test",
        Status:    audit.StatusOK,
    }

    err := auditor.Record(ctx, entry)
    assert.NoError(t, err)

    err = auditor.Flush(ctx)
    assert.NoError(t, err)
}
```

### 使用 NoopAuditor

```go
func TestBusinessLogic(t *testing.T) {
    // 测试时不需要真实的审计
    ctx := audit.WithAuditor(context.Background(), audit.NewNoopAuditor())
    
    // 执行业务逻辑
    result := doSomething(ctx)
    
    assert.NotNil(t, result)
}
```

---

## API 参考

### 接口

#### Auditor

```go
type Auditor interface {
    Record(ctx context.Context, entry *Entry) error
    Flush(ctx context.Context) error
}
```

### 函数

- `WithAuditor(ctx, auditor)` - 将 Auditor 注入 Context
- `FromContext(ctx)` - 从 Context 提取 Auditor
- `MustFromContext(ctx)` - 从 Context 提取 Auditor，不存在则返回 NoopAuditor
- `NewNoopAuditor()` - 创建空实现 Auditor

### 方法

#### Entry

- `SetPreValue(v)` - 设置变更前的值（自动 JSON 序列化）
- `SetPostValue(v)` - 设置变更后的值（自动 JSON 序列化）

### 常量

#### Operation

- `OpInsert` - 插入操作
- `OpUpdate` - 更新操作
- `OpUpsert` - 插入或更新操作
- `OpDelete` - 删除操作

#### Status

- `StatusOK` - 成功（0）
- `StatusFail` - 失败（1）

---

## 相关资源

- [OpenTelemetry 追踪](https://opentelemetry.io/)
- [GDPR 合规指南](https://gdpr.eu/)

---

## 许可证

本项目采用 MIT 许可证。
