# API Package

统一的分页、过滤、排序 Protocol Buffers 定义，为所有数据访问层提供标准化的请求和响应结构。

## 概述

本包定义了通用的分页查询协议，支持多种分页方式、复杂的过滤表达式、灵活的排序规则和字段选择。所有 go-crud 的数据访问实现（GORM、Ent、MongoDB、InfluxDB 等）都使用这套统一的 API 定义。

## 特性

- ✅ **多种分页方式** - 页码分页、偏移量分页、令牌分页、不分页
- ✅ **复杂过滤表达式** - 支持 AND/OR 嵌套、29 种操作符
- ✅ **灵活排序规则** - 支持多字段排序、升序/降序
- ✅ **字段掩码** - 精确控制返回字段，提升查询性能
- ✅ **OpenAPI 集成** - 自动生成 OpenAPI v3 文档
- ✅ **Google AIP 兼容** - 遵循 Google API 改进提案规范
- ✅ **类型安全** - Protocol Buffers 强类型定义
- ✅ **跨语言支持** - 可生成 Go、Java、Python、TypeScript 等代码

## 快速开始

### 1. 安装依赖

```bash
go get github.com/tx7do/go-crud/api
```

### 2. 在 Protobuf 中使用

在你的 `.proto` 文件中导入分页定义：

```protobuf
syntax = "proto3";

package myservice;

import "pagination/v1/pagination.proto";

message ListUsersRequest {
  // 使用通用分页请求
  pagination.PaginationRequest pagination = 1;
}

message ListUsersResponse {
  // 使用通用分页响应
  pagination.PaginationResponse response = 1;
}

service UserService {
  rpc ListUsers(ListUsersRequest) returns (ListUsersResponse);
}
```

### 3. 在 Go 代码中使用

```go
import (
    paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

// 创建分页请求
page := uint32(1)
pageSize := uint32(10)

req := &paginationV1.PagingRequest{
    Page:     &page,
    PageSize: &pageSize,
}

// 添加排序
sorting := []*paginationV1.Sorting{
    {
        Field:     "created_at",
        Direction: paginationV1.Sorting_DESC,
    },
}
req.Sorting = sorting

// 添加字段掩码
fieldMask := &fieldmaskpb.FieldMask{
    Paths: []string{"id", "name", "email"},
}
req.FieldMask = fieldMask
```

## 核心概念

### 1. 分页方式（Pagination Types）

#### 页码分页（Page-Based）

最直观的分页方式，适合传统 Web 应用。

```protobuf
message PageBasedPagination {
  uint32 page = 1;      // 当前页码（从1开始）
  uint32 page_size = 2; // 每页条数
}
```

**使用示例：**

```go
req := &paginationV1.PaginationRequest{
    PaginationType: &paginationV1.PaginationRequest_PageBased{
        PageBased: &paginationV1.PageBasedPagination{
            Page:     1,
            PageSize: 10,
        },
    },
}
```

**优点：**
- 用户友好，易于理解
- 可以跳转到任意页

**缺点：**
- 深分页性能差（OFFSET 大时）
- 数据变化时可能出现重复或遗漏

---

#### 偏移量分页（Offset-Based）

基于跳过记录数的分页，适合 API 场景。

```protobuf
message OffsetBasedPagination {
  uint64 offset = 1; // 跳过的记录数（从0开始）
  uint32 limit = 2;  // 最多返回的记录数
}
```

**使用示例：**

```go
req := &paginationV1.PaginationRequest{
    PaginationType: &paginationV1.PaginationRequest_OffsetBased{
        OffsetBased: &paginationV1.OffsetBasedPagination{
            Offset: 0,
            Limit:  10,
        },
    },
}
```

**优点：**
- 简单直接
- 数据库原生支持

**缺点：**
- 深分页性能差
- 数据变化时不稳定

---

#### 令牌分页（Token-Based）

基于游标的分页，性能最优，适合大数据集。

```protobuf
message TokenBasedPagination {
  string token = 1;     // 上一页最后一条记录的游标
  uint32 page_size = 2; // 每页条数
}
```

**使用示例：**

```go
// 第一页
req := &paginationV1.PaginationRequest{
    PaginationType: &paginationV1.PaginationRequest_TokenBased{
        TokenBased: &paginationV1.TokenBasedPagination{
            Token:    "",  // 首次请求为空
            PageSize: 10,
        },
    },
}

// 后续页（使用上一页返回的 next_token）
resp := service.ListItems(req)
nextReq := &paginationV1.PaginationRequest{
    PaginationType: &paginationV1.PaginationRequest_TokenBased{
        TokenBased: &paginationV1.TokenBasedPagination{
            Token:    resp.Meta.NextToken,
            PageSize: 10,
        },
    },
}
```

**优点：**
- 性能最优（使用索引扫描）
- 数据变化时稳定（不会重复或遗漏）
- 适合无限滚动

**缺点：**
- 不能跳转到任意页
- 实现复杂度较高

---

#### 不分页（No Paging）

获取所有数据，谨慎使用。

```protobuf
message NoPaging {}
```

**使用示例：**

```go
req := &paginationV1.PaginationRequest{
    PaginationType: &paginationV1.PaginationRequest_NoPaging{
        NoPaging: &paginationV1.NoPaging{},
    },
}
```

---

### 2. 过滤表达式（Filter Expressions）

#### 简单过滤条件

```protobuf
message FilterCondition {
  string field = 1;           // 字段名
  Operator op = 2;            // 操作符
  string value = 3;           // 单值
  repeated string values = 4; // 多值（用于 IN 操作符）
  optional DatePart date_part = 5; // 日期时间部分
  optional string json_path = 6;   // JSON 路径
}
```

#### 操作符（Operators）

支持 29 种操作符：

| 类别 | 操作符 | 说明 | SQL 映射 |
|------|--------|------|----------|
| **基本比较** | `EQ` | 等于 | `=` |
| | `NEQ` | 不等于 | `!=` |
| | `GT` | 大于 | `>` |
| | `GTE` | 大于等于 | `>=` |
| | `LT` | 小于 | `<` |
| | `LTE` | 小于等于 | `<=` |
| **模糊匹配** | `LIKE` | 模糊匹配 | `LIKE` |
| | `ILIKE` | 不区分大小写模糊 | `ILIKE` |
| | `NOT_LIKE` | 不匹配 | `NOT LIKE` |
| **集合操作** | `IN` | 在集合中 | `IN (...)` |
| | `NIN` | 不在集合中 | `NOT IN (...)` |
| **空值判断** | `IS_NULL` | 为空 | `IS NULL` |
| | `IS_NOT_NULL` | 不为空 | `IS NOT NULL` |
| **范围与正则** | `BETWEEN` | 范围内 | `BETWEEN ... AND ...` |
| | `REGEXP` | 正则匹配 | `REGEXP` |
| | `IREGEXP` | 不区分大小写正则 | `~*` |
| **字符串操作** | `CONTAINS` | 包含 | `LIKE %v%` |
| | `STARTS_WITH` | 前缀 | `LIKE v%` |
| | `ENDS_WITH` | 后缀 | `LIKE %v` |
| | `ICONTAINS` | 不区分大小写包含 | `ILIKE %v%` |
| | `ISTARTS_WITH` | 不区分大小写前缀 | `ILIKE v%` |
| | `IENDS_WITH` | 不区分大小写后缀 | `ILIKE %v` |
| **JSON/数组** | `JSON_CONTAINS` | JSON 包含 | `@>` |
| | `ARRAY_CONTAINS` | 数组包含 | `ANY()` |
| | `EXISTS` | 存在性 | `EXISTS` |
| **搜索** | `SEARCH` | 全文搜索 | 自定义 |
| | `EXACT` | 精确匹配 | `=` |
| | `IEXACT` | 不区分大小写精确 | `LOWER() =` |

**使用示例：**

```go
// 年龄大于 18
condition := &paginationV1.FilterCondition{
    Field: "age",
    Op:    paginationV1.Operator_GT,
    Value: &paginationV1.FilterCondition_ValueOneof{
        Value: "18",
    },
}

// 姓名包含 "John"
condition := &paginationV1.FilterCondition{
    Field: "name",
    Op:    paginationV1.Operator_CONTAINS,
    Value: &paginationV1.FilterCondition_ValueOneof{
        Value: "John",
    },
}

// 状态在 [active, pending] 中
condition := &paginationV1.FilterCondition{
    Field:  "status",
    Op:     paginationV1.Operator_IN,
    Values: []string{"active", "pending"},
}
```

---

#### 复杂过滤表达式

支持 AND/OR 嵌套，构建复杂查询。

```protobuf
message FilterExpr {
  ExprType type = 1;                    // AND 或 OR
  repeated FilterCondition conditions = 2; // 条件列表
  repeated FilterExpr groups = 3;       // 子表达式列表
}
```

**使用示例：**

```go
// (age > 18 AND status = 'active') OR (role = 'admin')
expr := &paginationV1.FilterExpr{
    Type: paginationV1.ExprType_OR,
    Groups: []*paginationV1.FilterExpr{
        {
            Type: paginationV1.ExprType_AND,
            Conditions: []*paginationV1.FilterCondition{
                {
                    Field: "age",
                    Op:    paginationV1.Operator_GT,
                    Value: &paginationV1.FilterCondition_ValueOneof{Value: "18"},
                },
                {
                    Field: "status",
                    Op:    paginationV1.Operator_EQ,
                    Value: &paginationV1.FilterCondition_ValueOneof{Value: "active"},
                },
            },
        },
        {
            Type: paginationV1.ExprType_AND,
            Conditions: []*paginationV1.FilterCondition{
                {
                    Field: "role",
                    Op:    paginationV1.Operator_EQ,
                    Value: &paginationV1.FilterCondition_ValueOneof{Value: "admin"},
                },
            },
        },
    },
}

req := &paginationV1.PagingRequest{
    FilteringType: &paginationV1.PagingRequest_FilterExpr{
        FilterExpr: expr,
    },
}
```

生成的 SQL：

```sql
WHERE (age > 18 AND status = 'active') OR (role = 'admin')
```

---

#### 日期时间部分（DatePart）

对日期时间字段的部分进行过滤。

```protobuf
enum DatePart {
  DATE = 1;        // 日期
  YEAR = 2;        // 年
  MONTH = 5;       // 月
  DAY = 9;         // 日
  HOUR = 11;       // 小时
  MINUTE = 12;     // 分钟
  SECOND = 13;     // 秒
  // ... 更多
}
```

**使用示例：**

```go
// 查询 2024 年的数据
condition := &paginationV1.FilterCondition{
    Field:    "created_at",
    Op:       paginationV1.Operator_EQ,
    Value:    &paginationV1.FilterCondition_ValueOneof{Value: "2024"},
    DatePart: paginationV1.DatePart_YEAR.Enum(),
}
```

生成的 SQL：

```sql
WHERE EXTRACT(YEAR FROM created_at) = 2024
```

---

### 3. 排序规则（Sorting）

```protobuf
message Sorting {
  enum Direction {
    ASC = 0;   // 升序
    DESC = 1;  // 降序
  }
  
  string field = 1;        // 排序字段
  Direction direction = 2; // 排序方向
}
```

**使用示例：**

```go
// 按创建时间降序，再按 ID 升序
sorting := []*paginationV1.Sorting{
    {
        Field:     "created_at",
        Direction: paginationV1.Sorting_DESC,
    },
    {
        Field:     "id",
        Direction: paginationV1.Sorting_ASC,
    },
}

req := &paginationV1.PagingRequest{
    Sorting: sorting,
}
```

生成的 SQL：

```sql
ORDER BY created_at DESC, id ASC
```

---

### 4. 字段掩码（Field Mask）

控制 SELECT 中的字段，提升查询性能。

```protobuf
optional google.protobuf.FieldMask field_mask = 30;
```

**使用示例：**

```go
import "google.golang.org/protobuf/types/known/fieldmaskpb"

// 只返回 id, name, email 字段
fieldMask := &fieldmaskpb.FieldMask{
    Paths: []string{"id", "name", "email"},
}

req := &paginationV1.PagingRequest{
    FieldMask: fieldMask,
}
```

生成的 SQL：

```sql
SELECT id, name, email FROM users ...
```

---

## 完整示例

### 示例 1：简单分页查询

```go
import (
    paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
    "google.golang.org/protobuf/types/known/fieldmaskpb"
)

// 构建请求
page := uint32(1)
pageSize := uint32(10)

req := &paginationV1.PagingRequest{
    Page:     &page,
    PageSize: &pageSize,
    Sorting: []*paginationV1.Sorting{
        {
            Field:     "created_at",
            Direction: paginationV1.Sorting_DESC,
        },
    },
    FieldMask: &fieldmaskpb.FieldMask{
        Paths: []string{"id", "name", "email"},
    },
}

// 调用服务
resp, err := userService.ListUsers(ctx, req)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Total: %d, Items: %d\n", resp.Total, len(resp.Items))
```

---

### 示例 2：复杂过滤查询

```go
// 查询条件：(age > 18 AND status = 'active') AND (name LIKE '%John%')
filterExpr := &paginationV1.FilterExpr{
    Type: paginationV1.ExprType_AND,
    Conditions: []*paginationV1.FilterCondition{
        {
            Field: "age",
            Op:    paginationV1.Operator_GT,
            Value: &paginationV1.FilterCondition_ValueOneof{Value: "18"},
        },
        {
            Field: "status",
            Op:    paginationV1.Operator_EQ,
            Value: &paginationV1.FilterCondition_ValueOneof{Value: "active"},
        },
        {
            Field: "name",
            Op:    paginationV1.Operator_CONTAINS,
            Value: &paginationV1.FilterCondition_ValueOneof{Value: "John"},
        },
    },
}

req := &paginationV1.PagingRequest{
    Page:          &page,
    PageSize:      &pageSize,
    FilteringType: &paginationV1.PagingRequest_FilterExpr{
        FilterExpr: filterExpr,
    },
}
```

---

### 示例 3：令牌分页（推荐用于大数据集）

```go
// 第一页
req := &paginationV1.PaginationRequest{
    PaginationType: &paginationV1.PaginationRequest_TokenBased{
        TokenBased: &paginationV1.TokenBasedPagination{
            Token:    "",
            PageSize: 10,
        },
    },
    Sorting: []*paginationV1.Sorting{
        {
            Field:     "created_at",
            Direction: paginationV1.Sorting_DESC,
        },
        {
            Field:     "id",
            Direction: paginationV1.Sorting_DESC,
        },
    },
}

// 循环获取所有数据
for {
    resp, err := service.ListItems(ctx, req)
    if err != nil {
        log.Fatal(err)
    }
    
    // 处理数据
    for _, item := range resp.Data {
        processItem(item)
    }
    
    // 检查是否有下一页
    if resp.Meta.NextToken == "" {
        break
    }
    
    // 准备下一页请求
    req.PaginationType = &paginationV1.PaginationRequest_TokenBased{
        TokenBased: &paginationV1.TokenBasedPagination{
            Token:    resp.Meta.NextToken,
            PageSize: 10,
        },
    }
}
```

---

### 示例 4：日期时间过滤

```go
// 查询 2024 年 1 月的数据
filterExpr := &paginationV1.FilterExpr{
    Type: paginationV1.ExprType_AND,
    Conditions: []*paginationV1.FilterCondition{
        {
            Field:    "created_at",
            Op:       paginationV1.Operator_EQ,
            Value:    &paginationV1.FilterCondition_ValueOneof{Value: "2024"},
            DatePart: paginationV1.DatePart_YEAR.Enum(),
        },
        {
            Field:    "created_at",
            Op:       paginationV1.Operator_EQ,
            Value:    &paginationV1.FilterCondition_ValueOneof{Value: "1"},
            DatePart: paginationV1.DatePart_MONTH.Enum(),
        },
    },
}

req := &paginationV1.PagingRequest{
    FilteringType: &paginationV1.PagingRequest_FilterExpr{
        FilterExpr: filterExpr,
    },
}
```

---

## Protocol Buffers 编译

### 使用 Buf

本项目使用 [Buf](https://buf.build/) 进行 Protocol Buffers 管理。

#### 安装 Buf

```bash
# macOS
brew install bufbuild/buf/buf

# Linux
curl -sSL https://github.com/bufbuild/buf/releases/latest/download/buf-Linux-x86_64 -o buf
chmod +x buf
sudo mv buf /usr/local/bin
```

#### 生成 Go 代码

```bash
cd api
buf generate
```

生成的代码位于 `gen/go/pagination/v1/` 目录。

#### 配置文件

**buf.yaml** - 模块配置：

```yaml
version: v2

modules:
  - path: protos
    lint:
      use:
        - STANDARD
    breaking:
      use:
        - FILE

deps:
  - 'buf.build/gnostic/gnostic'

lint:
  use:
    - DEFAULT
```

**buf.gen.yaml** - 代码生成配置：

```yaml
version: v1

managed:
  enabled: true
  optimize_for: SPEED

  go_package_prefix:
    default: github.com/tx7do/go-crud/api/gen/go
    except:
      - 'buf.build/googleapis/googleapis'
      - 'buf.build/gnostic/gnostic'

plugins:
  - name: go
    out: gen/go
    opt: paths=source_relative
```

---

## 最佳实践

### 1. 选择合适的分页方式

| 场景 | 推荐方式 | 原因 |
|------|---------|------|
| 传统 Web 分页 | Page-Based | 用户友好，可跳转 |
| RESTful API | Offset-Based | 简单直接 |
| 大数据集/无限滚动 | Token-Based | 性能最优，稳定 |
| 导出全部数据 | No Paging | 谨慎使用，注意内存 |

---

### 2. 始终指定排序

```go
// ✅ 好的做法：明确指定排序
req.Sorting = []*paginationV1.Sorting{
    {Field: "created_at", Direction: paginationV1.Sorting_DESC},
    {Field: "id", Direction: paginationV1.Sorting_DESC},
}

// ❌ 避免：不指定排序，结果不稳定
req.Sorting = nil
```

**原因：** 没有排序时，数据库返回顺序不确定，可能导致分页结果重复或遗漏。

---

### 3. 限制页大小

```go
// 服务端验证
maxPageSize := 100
if req.GetPageSize() > maxPageSize {
    return errors.New("page size too large")
}
```

**原因：** 防止恶意请求导致性能问题。

---

### 4. 使用字段掩码优化性能

```go
// ✅ 只查询需要的字段
fieldMask := &fieldmaskpb.FieldMask{
    Paths: []string{"id", "name"},
}

// ❌ 查询所有字段（浪费带宽和内存）
fieldMask := nil
```

---

### 5. 参数化查询防止注入

```go
// ✅ 使用 FilterExpr（自动参数化）
filterExpr := &paginationV1.FilterExpr{
    Conditions: []*paginationV1.FilterCondition{
        {
            Field: "name",
            Op:    paginationV1.Operator_EQ,
            Value: &paginationV1.FilterCondition_ValueOneof{Value: userInput},
        },
    },
}

// ❌ 避免：手动拼接 SQL
query := fmt.Sprintf("WHERE name = '%s'", userInput) // SQL 注入风险！
```

---

### 6. 深分页优化

对于深分页（如第 10000 页），建议使用令牌分页或游标分页：

```go
// ✅ 令牌分页（性能好）
req := &paginationV1.PaginationRequest{
    PaginationType: &paginationV1.PaginationRequest_TokenBased{
        TokenBased: &paginationV1.TokenBasedPagination{
            Token:    lastToken,
            PageSize: 10,
        },
    },
}

// ❌ 页码分页（深分页性能差）
req := &paginationV1.PaginationRequest{
    Page:     proto.Uint32(10000),
    PageSize: proto.Uint32(10),
}
```

---

### 7. 合理使用过滤操作符

```go
// ✅ 使用前缀匹配（可利用索引）
condition := &paginationV1.FilterCondition{
    Field: "name",
    Op:    paginationV1.Operator_STARTS_WITH,
    Value: &paginationV1.FilterCondition_ValueOneof{Value: "John"},
}

// ⚠️ 谨慎使用包含匹配（无法利用索引）
condition := &paginationV1.FilterCondition{
    Field: "name",
    Op:    paginationV1.Operator_CONTAINS,
    Value: &paginationV1.FilterCondition_ValueOneof{Value: "John"},
}
```

---

## API 参考

### 消息类型

#### 分页请求

- `PagingRequest` - 简化版分页请求（向后兼容）
- `PaginationRequest` - 通用分页请求（推荐）

#### 分页响应

- `PagingResponse` - 简化版分页响应
- `PaginationResponse` - 通用分页响应
- `PaginationResponseMeta` - 分页元数据

#### 分页方式

- `PageBasedPagination` - 页码分页
- `OffsetBasedPagination` - 偏移量分页
- `TokenBasedPagination` - 令牌分页
- `NoPaging` - 不分页

#### 过滤

- `FilterCondition` - 过滤条件
- `FilterExpr` - 过滤表达式
- `Operator` - 操作符枚举（29 种）
- `ExprType` - 表达式类型（AND/OR）
- `DatePart` - 日期时间部分枚举

#### 排序

- `Sorting` - 排序规则
- `Sorting.Direction` - 排序方向（ASC/DESC）

---

### 枚举值

#### Operator（操作符）

```
OPERATOR_UNSPECIFIED = 0
EQ = 1              // =
NEQ = 2             // !=
GT = 3              // >
GTE = 4             // >=
LT = 5              // <
LTE = 6             // <=
LIKE = 7            // LIKE
ILIKE = 8           // ILIKE
NOT_LIKE = 9        // NOT LIKE
IN = 10             // IN
NIN = 11            // NOT IN
IS_NULL = 12        // IS NULL
IS_NOT_NULL = 13    // IS NOT NULL
BETWEEN = 14        // BETWEEN
REGEXP = 15         // REGEXP
IREGEXP = 16        // IREGEXP
CONTAINS = 17       // CONTAINS
STARTS_WITH = 18    // STARTS WITH
ENDS_WITH = 19      // ENDS WITH
ICONTAINS = 20      // ICONTAINS
ISTARTS_WITH = 21   // ISTARTS WITH
IENDS_WITH = 22     // IENDS WITH
JSON_CONTAINS = 23  // JSON CONTAINS
ARRAY_CONTAINS = 24 // ARRAY CONTAINS
EXISTS = 25         // EXISTS
SEARCH = 26         // SEARCH
EXACT = 27          // EXACT
IEXACT = 28         // IEXACT
```

#### DatePart（日期时间部分）

```
DATE_PART_UNSPECIFIED = 0
DATE = 1        // 日期
YEAR = 2        // 年
ISO_YEAR = 3    // ISO 年
QUARTER = 4     // 季度
MONTH = 5       // 月
WEEK = 6        // 周
WEEK_DAY = 7    // 星期几
ISO_WEEK_DAY = 8 // ISO 星期几
DAY = 9         // 日
TIME = 10       // 时间
HOUR = 11       // 小时
MINUTE = 12     // 分钟
SECOND = 13     // 秒
MICROSECOND = 14 // 微秒
```

---

## 与其他包的集成

### GORM

```go
import "github.com/tx7do/go-crud/gorm"

repo := gorm.NewRepository[User, UserEntity](mapper)

// 直接使用 PagingRequest
result, err := repo.ListWithPaging(ctx, db, req)
```

### Ent

```go
import "github.com/tx7do/go-crud/entgo"

repo := entgo.NewRepository[...](mapper)

// 使用 PaginationRequest
result, err := repo.ListWithPagination(ctx, builder, countBuilder, req)
```

### MongoDB

```go
import "github.com/tx7do/go-crud/mongodb"

repo := mongodb.NewRepository[...](client, collection)

result, total, err := repo.ListWithPaging(ctx, req)
```

### InfluxDB

```go
import "github.com/tx7do/go-crud/influxdb"

repo := influxdb.NewRepository[...](client, measurement)

results, total, err := repo.ListWithPaging(ctx, req)
```

---

## 版本兼容性

本包遵循语义化版本（SemVer）：

- **主版本号**：不兼容的 API 修改
- **次版本号**：向后兼容的功能新增
- **修订号**：向后兼容的问题修正

Breaking Changes 会通过 Buf 的 breaking check 进行检测。

---

## 测试

运行测试：

```bash
cd api
go test -v ./...
```

---

## 依赖

- `google.golang.org/protobuf` - Protocol Buffers Go 实现
- `github.com/google/gnostic` - OpenAPI v3 支持
- `buf.build/gnostic/gnostic` - Buf 远程依赖

---

## 相关资源

- [Protocol Buffers 官方文档](https://developers.google.com/protocol-buffers)
- [Buf 官方文档](https://docs.buf.build/)
- [Google API 改进提案（AIP）](https://google.aip.dev/)
- [OpenAPI Specification](https://swagger.io/specification/)

---

## 许可证

本项目采用 MIT 许可证。
