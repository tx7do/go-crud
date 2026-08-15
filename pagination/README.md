# Pagination Package

分页、过滤和排序的核心工具包，提供统一的分页接口、灵活的过滤表达式解析和多种排序格式转换。这是 go-crud 项目的基础设施层，被所有数据访问层包（GORM、Entgo、MongoDB、ClickHouse、Doris 等）使用。

## 特性

- ✅ **三种分页方式** - Page-Based、Offset-Based、Token-Based
- ✅ **Paginator 接口** - 统一的分页器抽象
- ✅ **结构化过滤** - 支持 JSON 和 Google AIP 两种过滤格式
- ✅ **29+ 种操作符** - 丰富的查询操作符（等于、大于、包含、IN 等）
- ✅ **复杂逻辑嵌套** - 支持 AND/OR 多层嵌套
- ✅ **灵活排序** - 支持 JSON 数组和 Google AIP 两种排序格式
- ✅ **字段掩码支持** - FieldMask 选择返回字段
- ✅ **工具函数** - 类型转换、过滤条件清理等实用工具
- ✅ **Protocol Buffers** - 基于 protobuf 的标准化定义

## 快速开始

### 1. 安装依赖

```bash
go get github.com/tx7do/go-crud/pagination
```

### 2. 使用 Paginator

#### Page-Based Paginator（页码分页）

```go
import "github.com/tx7do/go-crud/pagination/paginator"

// 创建分页器
p := paginator.NewPagePaginator(1, 10) // 第 1 页，每页 10 条

// 设置总数
p.SetTotal(100)

// 获取分页信息
fmt.Printf("Page: %d, Size: %d\n", p.Page(), p.Size())           // Page: 1, Size: 10
fmt.Printf("Offset: %d, Limit: %d\n", p.Offset(), p.Limit())     // Offset: 0, Limit: 10
fmt.Printf("Total: %d, TotalPages: %d\n", p.Total(), p.TotalPages()) // Total: 100, TotalPages: 10
fmt.Printf("HasNext: %v, HasPrev: %v\n", p.HasNext(), p.HasPrev())   // HasNext: true, HasPrev: false

// 链式设置
p.WithPage(2).WithSize(20)
fmt.Printf("New Page: %d, New Size: %d\n", p.Page(), p.Size()) // New Page: 2, New Size: 20
```

#### Offset-Based Paginator（偏移量分页）

```go
import "github.com/tx7do/go-crud/pagination/paginator"

// 创建分页器
p := paginator.NewOffsetPaginator(0, 10) // 从第 0 条开始，取 10 条

// 设置总数
p.SetTotal(100)

// 获取分页信息
fmt.Printf("Offset: %d, Limit: %d\n", p.Offset(), p.Limit()) // Offset: 0, Limit: 10
fmt.Printf("Total: %d\n", p.Total())                          // Total: 100
fmt.Printf("HasNext: %v\n", p.HasNext())                      // HasNext: true

// 跳转到下一页
p.WithOffset(10).WithLimit(10)
fmt.Printf("New Offset: %d\n", p.Offset()) // New Offset: 10
```

#### Token-Based Paginator（游标分页）

```go
import "github.com/tx7do/go-crud/pagination/paginator"

// 创建分页器
p := paginator.NewTokenPaginator()

// 设置 token
p.SetToken("eyJpZCI6MTAwfQ==") // Base64 编码的游标
p.SetNextToken("eyJpZCI6MjAwfQ==")

// 获取 token
fmt.Printf("Token: %s\n", p.Token())         // Token: eyJpZCI6MTAwfQ==
fmt.Printf("NextToken: %s\n", p.NextToken()) // NextToken: eyJpZCI6MjAwfQ==
```

---

### 3. 使用过滤器转换器

#### JSON 格式过滤

```go
import (
    "github.com/tx7do/go-crud/pagination/filter"
)

converter := filter.NewQueryStringConverter()

// 简单等于查询
expr, err := converter.Convert(`{"user_id": 1001}`)
// 等价于：user_id = 1001

// 带操作符的查询
expr, err = converter.Convert(`{"age__gte": 18, "status": "active"}`)
// 等价于：age >= 18 AND status = 'active'

// 复杂嵌套查询
jsonQuery := `{
    "$and": [
        {"dept_id": 1},
        {"$or": [
            {"age__gte": 18},
            {"role": "admin"}
        ]}
    ]
}`
expr, err = converter.Convert(jsonQuery)
// 等价于：dept_id = 1 AND (age >= 18 OR role = 'admin')
```

#### Google AIP 格式过滤

```go
converter := filter.NewFilterStringConverter()

// AIP 格式过滤字符串
filterStr := `user_id=1001 AND age>=18 AND status="active"`
expr, err := converter.Convert(filterStr)
```

#### 从 PagingRequest 自动转换

```go
import paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
import "github.com/tx7do/go-crud/pagination/filter"

req := &paginationV1.PagingRequest{
    Query: `{"age__gte": 18}`,
}

// 自动识别并转换（支持 FilterExpr、Query、Filter 三种来源）
expr, err := filter.ConvertFilterByPagingRequest(req)
```

---

### 4. 使用排序转换器

#### JSON 数组格式排序

```go
import "github.com/tx7do/go-crud/pagination/sorting"

converter := sorting.NewOrderByStringConverter()

// JSON 数组格式
orderByJSON := `["created_at", "-updated_at"]`
sortings, err := converter.Convert(orderByJSON)
// 结果：
// - created_at ASC
// - updated_at DESC
```

#### Google AIP 格式排序

```go
// AIP 格式字符串
orderByAIP := "created_at desc, updated_at asc"
sortings, err := converter.Convert(orderByAIP)
// 结果：
// - created_at DESC
// - updated_at ASC
```

---

### 5. 使用工具函数

#### 类型转换

```go
import "github.com/tx7do/go-crud/pagination"

// AnyToStructValue - 任意值转 structpb.Value
sv := pagination.AnyToStructValue(123)
sv = pagination.AnyToStructValue("hello")
sv = pagination.AnyToStructValue([]string{"a", "b"})

// StructValueToString - structpb.Value 转字符串
str := pagination.StructValueToString(sv)

// AnyToString - 任意值转字符串
str = pagination.AnyToString(123)      // "123"
str = pagination.AnyToString(true)     // "true"
str = pagination.AnyToString(nil)      // ""
```

#### 过滤条件清理

```go
import paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"

// 移除指定字段的过滤条件
expr := &paginationV1.FilterExpr{
    Conditions: []*paginationV1.FilterCondition{
        {Field: "user_id", Op: paginationV1.Operator_EQ, Value: ...},
        {Field: "password", Op: paginationV1.Operator_EQ, Value: ...}, // 敏感字段
    },
}

// 移除 password 字段
excluded := pagination.RemoveExcludedConditions(expr, []string{"password"})
// expr.Conditions 现在只包含 user_id 条件
// excluded 包含被移除的 password 条件
```

---

## API 参考

### Paginator 接口

```go
type Paginator interface {
    Mode() PaginateMode
    
    // Page/Size 风格
    Page() int
    Size() int
    
    // Offset/Limit 风格
    Offset() int
    Limit() int
    
    // Token 风格
    Token() string
    NextToken() string
    PrevToken() string
    SetToken(token string)
    SetNextToken(token string)
    SetPrevToken(token string)
    
    // 统计相关
    Total() int64
    SetTotal(total int64)
    TotalPages() int
    HasNext() bool
    HasPrev() bool
    
    // 链式设置
    WithPage(page int) Paginator
    WithSize(size int) Paginator
    WithOffset(offset int) Paginator
    WithLimit(limit int) Paginator
    WithToken(token string) Paginator
}
```

### PaginateMode 枚举

```go
const (
    ModePage   PaginateMode = iota  // 页码分页
    ModeOffset                       // 偏移量分页
    ModeToken                        // 游标分页
)
```

### Paginator 实现

#### PagePaginator

```go
func NewPagePaginator(page, size int) Paginator
func NewPagePaginatorWithDefault() Paginator  // 默认第 1 页，每页 10 条
```

**特点：**
- 自动校正 page < 1 为 1
- 自动校正 size < 1 为 1
- 计算 TotalPages = ceil(Total / Size)
- HasNext = Page < TotalPages
- HasPrev = Page > 1

#### OffsetPaginator

```go
func NewOffsetPaginator(offset, limit int) Paginator
func NewOffsetPaginatorWithDefault() Paginator  // 默认 offset=0, limit=10
```

**特点：**
- 自动校正 offset < 0 为 0
- 自动校正 limit < 1 为 1
- HasNext = Offset + Limit < Total
- HasPrev = Offset > 0

#### TokenPaginator

```go
func NewTokenPaginator() Paginator
```

**特点：**
- 基于游标的分页，适合大数据集
- Token 通常是 Base64 编码的最后一个记录的 ID
- 不支持 Total 和 TotalPages（因为不需要遍历全部数据）

---

### 过滤器转换器

#### QueryStringConverter（JSON 格式）

```go
func NewQueryStringConverter() *QueryStringConverter
func (c *QueryStringConverter) Convert(query string) (*FilterExpr, error)
```

**支持的格式：**
- 简单对象：`{"field": value}`
- 带操作符：`{"field__op": value}`
- 逻辑组合：`{"$and": [...], "$or": [...]}`
- 嵌套字段：`{"user.profile.age__gte": 18}`

#### FilterStringConverter（Google AIP 格式）

```go
func NewFilterStringConverter() *FilterStringConverter
func (c *FilterStringConverter) Convert(filter string) (*FilterExpr, error)
```

**支持的格式：**
- 简单条件：`field=value`
- 比较操作：`field>=value`, `field<value`
- 逻辑组合：`field1=value1 AND field2=value2`
- 括号分组：`(field1=value1 OR field2=value2) AND field3=value3`

#### 便捷函数

```go
func ConvertFilterByPagingRequest(req *PagingRequest) (*FilterExpr, error)
func ConvertFilterByPaginationRequest(req *PaginationRequest) (*FilterExpr, error)
```

**优先级：**
1. 如果 `req.FilterExpr` 不为空，直接返回
2. 如果 `req.Query` 不为空，使用 QueryStringConverter 转换
3. 如果 `req.Filter` 不为空，使用 FilterStringConverter 转换
4. 否则返回 nil

---

### 排序转换器

#### OrderByStringConverter

```go
func NewOrderByStringConverter() *OrderByStringConverter
func (c *OrderByStringConverter) Convert(orderBy string) ([]*Sorting, error)
func (c *OrderByStringConverter) ParseJsonString(orderByJson string) ([]*Sorting, error)
func (c *OrderByStringConverter) ParseAIPString(orderByString string) ([]*Sorting, error)
```

**JSON 格式规则：**
- 升序：`"field_name"`
- 降序：`"-field_name"`（前缀 `-`）
- 示例：`["created_at", "-updated_at", "name"]`

**AIP 格式规则：**
- 升序：`field_name` 或 `field_name asc`
- 降序：`field_name desc`
- 示例：`created_at desc, updated_at asc, name`

---

### 工具函数

#### 类型转换

```go
func AnyToStructValue(v any) *structpb.Value
func StructValueToString(sv *structpb.Value) string
func AnyToString(v any) string
```

**AnyToStructValue：**
- 将任意 Go 值转换为 protobuf `structpb.Value`
- 支持：string、number、bool、nil、list、struct
- 失败时返回 nil

**StructValueToString：**
- 将 `structpb.Value` 转换为字符串
- StringValue → 直接返回
- NumberValue/BoolValue → fmt.Sprintf
- ListValue/StructValue → JSON 序列化
- NullValue → 空字符串

**AnyToString：**
- 将任意 Go 值转换为字符串
- 支持：string、*string、fmt.Stringer、[]byte、其他类型（fmt.Sprintf）
- nil → 空字符串

#### 过滤条件清理

```go
func RemoveExcludedConditions(filterExpr *FilterExpr, excludeFields []string) []*FilterCondition
func ClearFilterExprByFieldNames(expr *FilterExpr, fieldName string)
func FilterFields(filterExpr *FilterExpr, excludeFields []string) []*FilterCondition
```

**RemoveExcludedConditions：**
- 从 FilterExpr 中移除指定字段的条件
- 返回被移除的条件列表
- 就地修改原 FilterExpr

**ClearFilterExprByFieldNames：**
- 从 FilterExpr 中移除指定字段的所有条件
- 递归处理嵌套的 Groups
- 无返回值（就地修改）

**FilterFields：**
- 与 RemoveExcludedConditions 功能相同（别名）

---

## 分页方式对比

| 特性 | Page-Based | Offset-Based | Token-Based |
|------|-----------|--------------|-------------|
| 适用场景 | 传统分页 UI | API 接口 | 大数据集、无限滚动 |
| 参数 | page, size | offset, limit | token, limit |
| 支持 Total | ✅ | ✅ | ❌ |
| 支持 TotalPages | ✅ | ❌ | ❌ |
| 支持 HasNext | ✅ | ✅ | ✅（通过 NextToken） |
| 支持 HasPrev | ✅ | ✅ | ✅（通过 PrevToken） |
| 性能（深分页） | 差（OFFSET 大） | 差（OFFSET 大） | 优（索引扫描） |
| 实现复杂度 | 简单 | 简单 | 中等（需要生成 token） |
| 示例 | `?page=2&size=10` | `?offset=10&limit=10` | `?token=abc&limit=10` |

**推荐使用：**
- **Page-Based**：传统 Web 应用，需要显示总页数
- **Offset-Based**：RESTful API，简单直观
- **Token-Based**：大数据集（百万级）、实时数据、无限滚动

---

## 过滤操作符完整列表

### 比较操作符

| 操作符 | 含义 | 示例 | SQL 等价 |
|--------|------|------|----------|
| `eq` | 等于 | `{"age__eq": 18}` | `age = 18` |
| `ne` | 不等于 | `{"age__ne": 18}` | `age != 18` |
| `gt` | 大于 | `{"age__gt": 18}` | `age > 18` |
| `gte` | 大于等于 | `{"age__gte": 18}` | `age >= 18` |
| `lt` | 小于 | `{"age__lt": 18}` | `age < 18` |
| `lte` | 小于等于 | `{"age__lte": 18}` | `age <= 18` |

### 范围操作符

| 操作符 | 含义 | 示例 | SQL 等价 |
|--------|------|------|----------|
| `in` | 在列表中 | `{"age__in": [18, 20, 25]}` | `age IN (18, 20, 25)` |
| `not_in` | 不在列表中 | `{"age__not_in": [18, 20]}` | `age NOT IN (18, 20)` |
| `between` | 在范围内 | `{"age__between": [18, 30]}` | `age BETWEEN 18 AND 30` |
| `not_between` | 不在范围内 | `{"age__not_between": [18, 30]}` | `age NOT BETWEEN 18 AND 30` |

### 字符串操作符

| 操作符 | 含义 | 示例 | SQL 等价 |
|--------|------|------|----------|
| `contains` | 包含 | `{"name__contains": "张"}` | `name LIKE '%张%'` |
| `icontains` | 包含（不区分大小写） | `{"name__icontains": "zhang"}` | `LOWER(name) LIKE '%zhang%'` |
| `startswith` | 以...开头 | `{"name__startswith": "张"}` | `name LIKE '张%'` |
| `istartswith` | 以...开头（不区分大小写） | `{"name__istartswith": "zhang"}` | `LOWER(name) LIKE 'zhang%'` |
| `endswith` | 以...结尾 | `{"name__endswith": "三"}` | `name LIKE '%三'` |
| `iendswith` | 以...结尾（不区分大小写） | `{"name__iendswith": "san"}` | `LOWER(name) LIKE '%san'` |
| `regex` | 正则匹配 | `{"phone__regex": "^1[3-9]\\d{9}$"}` | `name REGEXP '...'` |

### NULL 检查

| 操作符 | 含义 | 示例 | SQL 等价 |
|--------|------|------|----------|
| `is_null` | 为空 | `{"email__is_null": true}` | `email IS NULL` |
| `is_not_null` | 不为空 | `{"email__is_not_null": true}` | `email IS NOT NULL` |

### 布尔操作符

| 操作符 | 含义 | 示例 | SQL 等价 |
|--------|------|------|----------|
| `is_true` | 为真 | `{"active__is_true": true}` | `active = TRUE` |
| `is_false` | 为假 | `{"active__is_false": true}` | `active = FALSE` |

### 日期/时间操作符

| 操作符 | 含义 | 示例 | SQL 等价 |
|--------|------|------|----------|
| `date` | 日期部分 | `{"created_at__date": "2024-01-01"}` | `DATE(created_at) = '2024-01-01'` |
| `year` | 年份 | `{"created_at__year": 2024}` | `YEAR(created_at) = 2024` |
| `month` | 月份 | `{"created_at__month": 1}` | `MONTH(created_at) = 1` |
| `day` | 日 | `{"created_at__day": 15}` | `DAY(created_at) = 15` |
| `hour` | 小时 | `{"created_at__hour": 12}` | `HOUR(created_at) = 12` |

**注意：** 默认操作符（无后缀）等价于 `eq`。

---

## 最佳实践

### 1. 选择合适的分页方式

```go
// ✅ 好的做法：根据场景选择分页方式

// 传统 Web 应用 → Page-Based
p := paginator.NewPagePaginator(page, pageSize)

// RESTful API → Offset-Based
p := paginator.NewOffsetPaginator(offset, limit)

// 大数据集（百万级）→ Token-Based
p := paginator.NewTokenPaginator()
p.SetToken(lastRecordID)
```

---

### 2. 限制每页大小

```go
// ✅ 好的做法：限制最大每页大小
maxPageSize := 100
if size > maxPageSize {
    size = maxPageSize
}
p := paginator.NewPagePaginator(page, size)

// ❌ 避免：不限制每页大小，可能导致内存溢出
p := paginator.NewPagePaginator(page, 10000)  // 危险！
```

---

### 3. 使用索引优化深分页

```go
// ✅ 好的做法：深分页使用 Token-Based
// Token 包含最后一条记录的 ID，下次查询从该 ID 之后开始
SELECT * FROM users WHERE id > :last_id ORDER BY id LIMIT 10

// ❌ 避免：深分页使用 OFFSET
SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 100000  // 慢！
```

**性能对比（100 万条数据）：**
- Page 1: OFFSET 0 → ~1ms
- Page 1000: OFFSET 9990 → ~50ms
- Page 10000: OFFSET 99990 → ~500ms
- Token-Based: 始终 ~1ms

---

### 4. 安全的过滤条件

```go
// ✅ 好的做法：移除敏感字段的过滤条件
excluded := pagination.RemoveExcludedConditions(expr, []string{
    "password",
    "secret_key",
    "internal_flag",
})

// ❌ 避免：允许用户过滤敏感字段
// 可能导致信息泄露
```

---

### 5. 验证排序字段

```go
// ✅ 好的做法：白名单验证排序字段
allowedSortFields := map[string]bool{
    "created_at": true,
    "updated_at": true,
    "name":       true,
}

for _, s := range sortings {
    if !allowedSortFields[s.Field] {
        return errors.New("invalid sort field")
    }
}

// ❌ 避免：直接使用用户输入的排序字段
// 可能导致 SQL 注入或性能问题
```

---

### 6. 处理空结果

```go
// ✅ 好的做法：检查是否有下一页
if p.HasNext() {
    nextToken := generateToken(lastRecord)
    response.NextToken = nextToken
}

// ❌ 避免：不检查边界条件
// 可能返回空的下一页
```

---

### 7. 缓存 Total 计数

```go
// ✅ 好的做法：对于大数据集，缓存 Total 计数
total, err := cache.Get("users_total")
if err != nil {
    total, _ = repo.Count(ctx, filter)
    cache.Set("users_total", total, 5*time.Minute)
}
p.SetTotal(total)

// ❌ 避免：每次都执行 COUNT 查询
// COUNT 在大表上很慢
```

---

### 8. 使用 FieldMask 减少数据传输

```go
// ✅ 好的做法：只返回需要的字段
fieldMask := &fieldmaskpb.FieldMask{
    Paths: []string{"id", "name", "email"},
}
req.FieldMask = fieldMask

// ❌ 避免：返回所有字段
// 浪费带宽和内存
```

---

## 与其他包的集成

### GORM

```go
import "github.com/tx7do/go-crud/gorm"

repo := gorm.NewRepository[UserDTO, UserEntity](db, mapper, logger)

// 使用 PagingRequest
result, err := repo.ListWithPaging(ctx, req)
```

### Entgo

```go
import "github.com/tx7do/go-crud/entgo"

repo := entgo.NewRepository[UserDTO, UserEntity](client, mapper, logger)

// 使用 PagingRequest
result, err := repo.ListWithPaging(ctx, req)
```

### MongoDB

```go
import "github.com/tx7do/go-crud/mongodb"

repo := mongodb.NewRepository[UserDTO, UserDocument](client, "users", mapper, logger)

// 使用 PagingRequest
results, total, err := repo.ListWithPaging(ctx, req)
```

### ClickHouse

```go
import "github.com/tx7do/go-crud/clickhouse"

repo := clickhouse.NewRepository[EventDTO, EventEntity](client, mapper, "events", logger)

// 使用 PagingRequest
result, err := repo.ListWithPaging(ctx, req)
```

### Doris

```go
import "github.com/tx7do/go-crud/doris"

repo := doris.NewRepository[EventDTO, EventEntity](client, mapper, "events", logger)

// 使用 PagingRequest
result, err := repo.ListWithPaging(ctx, req)
```

---

## 测试

运行测试：

```bash
go test -v ./pagination/...
```

运行特定测试：

```bash
go test -v ./pagination -run TestPaginator
go test -v ./pagination/filter -run TestConverter
go test -v ./pagination/sorting -run TestOrderBy
```

---

## 示例项目

查看完整示例：
- [utils_test.go](./utils_test.go) - 工具函数测试示例
- [filter/converter_test.go](./filter/converter_test.go) - 过滤器转换器测试
- [filter/query_string_converter_test.go](./filter/query_string_converter_test.go) - JSON 格式转换测试
- [filter/filter_string_converter_test.go](./filter/filter_string_converter_test.go) - AIP 格式转换测试
- [sorting/order_by_string_converter_test.go](./sorting/order_by_string_converter_test.go) - 排序转换测试

---

## 依赖

- `google.golang.org/protobuf` - Protocol Buffers
- `go.einride.tech/aip` - Google AIP 规范库
- `github.com/tx7do/go-utils/stringcase` - 字符串转换工具
- `github.com/tx7do/go-crud/api` - Protobuf 定义（PagingRequest、FilterExpr、Sorting 等）

---

## 架构设计

### 分层结构

```
pagination/
├── paginator.go          # Paginator 接口定义
├── utils.go              # 通用工具函数
├── paginator/            # 分页器实现
│   ├── page_paginator.go
│   ├── offset_paginator.go
│   └── token_paginator.go
├── filter/               # 过滤器转换器
│   ├── converter.go              # 统一转换入口
│   ├── query_string_converter.go # JSON 格式
│   ├── filter_string_converter.go # AIP 格式
│   └── operator_converter.go     # 操作符转换
└── sorting/              # 排序转换器
    └── order_by_string_converter.go
```

### 工作流程

```
前端请求
    ↓
PagingRequest (Protobuf)
    ↓
┌─────────────────────────────┐
│  Filter Conversion          │
│  - QueryStringConverter     │ → FilterExpr
│  - FilterStringConverter    │
└─────────────────────────────┘
    ↓
┌─────────────────────────────┐
│  Sorting Conversion         │
│  - OrderByStringConverter   │ → Sorting[]
└─────────────────────────────┘
    ↓
┌─────────────────────────────┐
│  Paginator                  │
│  - PagePaginator            │ → OFFSET/LIMIT
│  - OffsetPaginator          │ → OFFSET/LIMIT
│  - TokenPaginator           │ → Cursor
└─────────────────────────────┘
    ↓
数据库查询
    ↓
结果返回
```

---

## 常见问题 FAQ

### Q: 什么时候使用 Page-Based，什么时候使用 Token-Based？

**A:** 
- **Page-Based**：传统 Web 应用，需要显示"第 X 页，共 Y 页"，数据量小（< 10 万）
- **Token-Based**：大数据集（> 100 万）、实时数据、无限滚动、移动端 App

**性能对比（100 万条数据）：**
- Page 10000: ~500ms（OFFSET 99990）
- Token-Based: ~1ms（索引扫描）

---

### Q: 如何生成 Token？

**A:** Token 通常是最后一条记录的唯一标识符的 Base64 编码：

```go
import "encoding/base64"

// 简单方式：直接编码 ID
func generateToken(id uint64) string {
    data := []byte(fmt.Sprintf("%d", id))
    return base64.StdEncoding.EncodeToString(data)
}

// 复杂方式：编码多个字段
func generateToken(id uint64, createdAt time.Time) string {
    data := map[string]any{
        "id":         id,
        "created_at": createdAt.Unix(),
    }
    jsonData, _ := json.Marshal(data)
    return base64.StdEncoding.EncodeToString(jsonData)
}
```

---

### Q: 如何处理复杂的嵌套过滤条件？

**A:** 使用 JSON 格式的 `$and` 和 `$or`：

```json
{
    "$and": [
        {"dept_id": 1},
        {"$or": [
            {"age__gte": 18},
            {"role": "admin"}
        ]},
        {"status": "active"}
    ]
}
```

等价 SQL：
```sql
WHERE dept_id = 1 
  AND (age >= 18 OR role = 'admin')
  AND status = 'active'
```

---

### Q: 如何防止 SQL 注入？

**A:** 
1. **不要直接拼接用户输入** - 使用参数化查询
2. **验证排序字段** - 使用白名单
3. **移除敏感字段** - 使用 `RemoveExcludedConditions`
4. **限制操作符** - 只允许安全的操作符

```go
// ✅ 安全：参数化查询
db.Where("age >= ?", age).Find(&users)

// ❌ 危险：直接拼接
db.Where(fmt.Sprintf("age >= %s", userInput)).Find(&users)
```

---

### Q: 如何实现全文搜索？

**A:** 使用 `contains` 或 `regex` 操作符：

```go
// 简单全文搜索
query := `{"title__icontains": "keyword"}`

// 多字段搜索
query := `{
    "$or": [
        {"title__icontains": "keyword"},
        {"content__icontains": "keyword"}
    ]
}`

// 正则搜索
query := `{"phone__regex": "^1[3-9]\\d{9}$"}`
```

**注意：** 对于大规模全文搜索，建议使用 Elasticsearch 或 OpenSearch。

---

### Q: 如何优化 COUNT 查询性能？

**A:** 
1. **缓存 Total** - 对于变化不频繁的数据
2. **使用近似计数** - PostgreSQL 可以使用 `reltuples`
3. **省略 Total** - 对于 Token-Based 分页，可以不返回 Total

```go
// 缓存 Total
total, err := cache.Get("users_total")
if err != nil {
    total, _ = db.Model(&User{}).Count()
    cache.Set("users_total", total, 5*time.Minute)
}

// 近似计数（PostgreSQL）
var total int64
db.Raw("SELECT reltuples FROM pg_class WHERE relname = 'users'").Scan(&total)
```

---

### Q: 如何处理时区问题？

**A:** 在过滤条件中使用 UTC 时间：

```go
// ✅ 好的做法：使用 UTC 时间
now := time.Now().UTC()
query := fmt.Sprintf(`{"created_at__gte": "%s"}`, now.Format(time.RFC3339))

// ❌ 避免：使用本地时间
now := time.Now()  // 可能因服务器时区不同而产生歧义
```

---

## 参考链接

- [Google AIP 规范](https://google.aip.dev/)
- [Protocol Buffers](https://developers.google.com/protocol-buffers)
- [Django Field lookups](https://docs.djangoproject.com/en/stable/ref/models/querysets/#field-lookups)
- [Tortoise ORM](https://tortoise.github.io/query.html#filtering)

---

## 许可证

本项目采用 MIT 许可证。
