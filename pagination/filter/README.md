# 分页过滤条件解析器

## 介绍

分页过滤条件解析器是一款用于处理分页请求中过滤参数的工具，核心能力是将前端传递的过滤条件转换为后端可识别的查询逻辑，从而实现数据的精准筛选与分页展示。

该解析器支持两种主流过滤格式，覆盖不同开发场景需求：

- **JSON格式**：语法简洁、支持复杂逻辑嵌套，适配前端表单过滤场景；
- **Google AIP表达式**：遵循Google API设计规范，适配标准化API接口场景。

## JSON格式

过滤条件以JSON对象/数组形式传递，通过“字段名+操作符”的组合实现多样化查询，无显式操作符时默认执行“等于”逻辑。

### 基础语法示例

以下为常用基础过滤写法，清晰对应查询语义与等价操作：

| 写法                                 | 含义                | 等价操作                |
|------------------------------------|-------------------|---------------------|
| `{"deptId": 1}`                    | 部门 ID 等于 1        | `{"deptId__eq": 1}` |
| `{"entryTime__gte": "2024-01-01"}` | 入职时间≥2024-01-01   | -（显式操作符）            |
| `{"userName__icontains": "张"}`     | 用户名包含 “张”（不区分大小写） | -（显式操作符）            |

### 逻辑组合规则

支持`$and`/`$or`关键字实现逻辑组合，数组内可嵌套基础条件或其他逻辑节点，满足复杂查询场景。

#### 场景1：纯AND组合

需求：部门 ID=1 且 入职时间≥2024-01-01 且 用户名含 “张”

```json
{
  "query": [
    {
      "deptId": 1
    },
    {
      "entryTime__gte": "2024-01-01"
    },
    {
      "userName__icontains": "张"
    }
  ]
}
```

或者使用`$and`显式写法：

```json
{
  "query": {
    "$and": [
      {
        "deptId": 1
      },
      {
        "entryTime__gte": "2024-01-01"
      },
      {
        "userName__icontains": "张"
      }
    ]
  }
}
```

说明：顶层数组默认等价于$and逻辑，简化常规多条件“且”查询写法。

#### 场景2：纯OR组合

需求：部门 ID=1 或 部门 ID=2 或 用户名含 “张”

```json
{
  "query": {
    "$or": [
      {
        "deptId": 1
      },
      {
        "deptId": 2
      },
      {
        "userName__icontains": "张"
      }
    ]
  }
}
```

#### 场景3：AND嵌套OR

需求：部门 ID=1 且（入职时间≥2024-01-01 或 用户名含 “张”）

```json
{
  "query": {
    "$and": [
      {
        "deptId": 1
      },
      {
        "$or": [
          {
            "entryTime__gte": "2024-01-01"
          },
          {
            "userName__icontains": "张"
          }
        ]
      }
    ]
  }
}
```

#### 场景4：OR嵌套AND

需求：（部门 ID=1 且 入职时间≥2024-01-01） 或 （部门 ID=2 且 用户名含 “张”）

```json
{
  "query": {
    "$or": [
      {
        "$and": [
          {
            "deptId": 1
          },
          {
            "entryTime__gte": "2024-01-01"
          }
        ]
      },
      {
        "$and": [
          {
            "deptId": 2
          },
          {
            "userName__icontains": "张"
          }
        ]
      }
    ]
  }
}
```

#### 场景5：多层嵌套

需求：部门 ID=1 且（（入职时间≥2024-01-01 且 入职时间≤2024-12-31）或 用户名含 “张”）且 状态 = active

```json
{
  "query": {
    "$and": [
      {
        "deptId": 1
      },
      {
        "$or": [
          {
            "$and": [
              {
                "entryTime__gte": "2024-01-01"
              },
              {
                "entryTime__lte": "2024-12-31"
              }
            ]
          },
          {
            "userName__icontains": "张"
          }
        ]
      },
      {
        "status": "active"
      }
    ]
  }
}
```

### 查找类型（操作符）规范

操作符设计参考Python主流ORM（[Tortoise ORM][2]、[Django Field lookups][3]
），采用双下划线__分割字段名与操作符，支持JSON嵌套字段查询，语法规则如下：

```text
{字段名}__{查找类型} : {值}
{字段名}.{JSON字段名}__{查找类型} : {值}
```

#### 通用查找类型

| 查找类型        | 示例                                                            | SQL                                                                                                                                                                                                                       | 备注                                                                                                            |
|-------------|---------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| not         | `{"name__not" : "tom"}`                                       | `WHERE NOT ("name" = "tom")`                                                                                                                                                                                              |                                                                                                               |
| in          | `{"name__in" : "[\"tom\", \"jimmy\"]"}`                       | `WHERE name IN ("tom", "jimmy")`                                                                                                                                                                                          |                                                                                                               |
| not_in      | `{"name__not_in" : "[\"tom\", \"jimmy\"]"}`                   | `WHERE name NOT IN ("tom", "jimmy")`                                                                                                                                                                                      |                                                                                                               |
| gte         | `{"create_time__gte" : "2023-10-25"}`                         | `WHERE "create_time" >= "2023-10-25"`                                                                                                                                                                                     |                                                                                                               |
| gt          | `{"create_time__gt" : "2023-10-25"}`                          | `WHERE "create_time" > "2023-10-25"`                                                                                                                                                                                      |                                                                                                               |
| lte         | `{"create_time__lte" : "2023-10-25"}`                         | `WHERE "create_time" <= "2023-10-25"`                                                                                                                                                                                     |                                                                                                               |
| lt          | `{"create_time__lt" : "2023-10-25"}`                          | `WHERE "create_time" < "2023-10-25"`                                                                                                                                                                                      |                                                                                                               |
| range       | `{"create_time__range" : "[\"2023-10-25\", \"2024-10-25\"]"}` | `WHERE "create_time" BETWEEN "2023-10-25" AND "2024-10-25"` <br>或<br> `WHERE "create_time" >= "2023-10-25" AND "create_time" <= "2024-10-25"`                                                                             | 需要注意的是: <br>1. 有些数据库的BETWEEN实现的开闭区间可能不一样。<br>2. 日期`2005-01-01`会被隐式转换为：`2005-01-01 00:00:00`，两个日期一致就会导致查询不到数据。 |
| isnull      | `{"name__isnull" : "True"}`                                   | `WHERE name IS NULL`                                                                                                                                                                                                      |                                                                                                               |
| not_isnull  | `{"name__not_isnull" : "False"}`                              | `WHERE name IS NOT NULL`                                                                                                                                                                                                  |                                                                                                               |
| contains    | `{"name__contains" : "L"}`                                    | `WHERE name LIKE '%L%';`                                                                                                                                                                                                  |                                                                                                               |
| icontains   | `{"name__icontains" : "L"}`                                   | `WHERE name ILIKE '%L%';`                                                                                                                                                                                                 |                                                                                                               |
| startswith  | `{"name__startswith" : "La"}`                                 | `WHERE name LIKE 'La%';`                                                                                                                                                                                                  |                                                                                                               |
| istartswith | `{"name__istartswith" : "La"}`                                | `WHERE name ILIKE 'La%';`                                                                                                                                                                                                 |                                                                                                               |
| endswith    | `{"name__endswith" : "a"}`                                    | `WHERE name LIKE '%a';`                                                                                                                                                                                                   |                                                                                                               |
| iendswith   | `{"name__iendswith" : "a"}`                                   | `WHERE name ILIKE '%a';`                                                                                                                                                                                                  |                                                                                                               |
| exact       | `{"name__exact" : "a"}`                                       | `WHERE name LIKE 'a';`                                                                                                                                                                                                    |                                                                                                               |
| iexact      | `{"name__iexact" : "a"}`                                      | `WHERE name ILIKE 'a';`                                                                                                                                                                                                   |                                                                                                               |
| regex       | `{"title__regex" : "^(An?\|The) +"}`                          | MySQL: `WHERE title REGEXP BINARY '^(An?\|The) +'`  <br> Oracle: `WHERE REGEXP_LIKE(title, '^(An?\|The) +', 'c');`  <br> PostgreSQL: `WHERE title ~ '^(An?\|The) +';`  <br> SQLite: `WHERE title REGEXP '^(An?\|The) +';` |                                                                                                               |
| iregex      | `{"title__iregex" : "^(an?\|the) +"}`                         | MySQL: `WHERE title REGEXP '^(an?\|the) +'`  <br> Oracle: `WHERE REGEXP_LIKE(title, '^(an?\|the) +', 'i');`  <br> PostgreSQL: `WHERE title ~* '^(an?\|the) +';`  <br> SQLite: `WHERE title REGEXP '(?i)^(an?\|the) +';`   |                                                                                                               |
| search      |                                                               |                                                                                                                                                                                                                           |                                                                                                               |

#### 日期时间提取类查找类型

支持从日期时间字段中提取指定维度值进行查询，适配时间维度筛选场景：

| 查找类型         | 示例                                   | SQL                                               | 备注                   |
|--------------|--------------------------------------|---------------------------------------------------|----------------------|
| date         | `{"pub_date__date" : "2023-01-01"}`  | `WHERE DATE(pub_date) = '2023-01-01'`             |                      |
| year         | `{"pub_date__year" : "2023"}`        | `WHERE EXTRACT('YEAR' FROM pub_date) = '2023'`    | 哪一年                  |
| iso_year     | `{"pub_date__iso_year" : "2023"}`    | `WHERE EXTRACT('ISOYEAR' FROM pub_date) = '2023'` | ISO 8601 一年中的周数      |
| month        | `{"pub_date__month" : "12"}`         | `WHERE EXTRACT('MONTH' FROM pub_date) = '12'`     | 月份，1-12              |
| day          | `{"pub_date__day" : "3"}`            | `WHERE EXTRACT('DAY' FROM pub_date) = '3'`        | 该月的某天(1-31)          |
| week         | `{"pub_date__week" : "7"}`           | `WHERE EXTRACT('WEEK' FROM pub_date) = '7'`       | ISO 8601 周编号 一年中的周数	 |
| week_day     | `{"pub_date__week_day" : "tom"}`     | ``                                                | 星期几                  |
| iso_week_day | `{"pub_date__iso_week_day" : "tom"}` | ``                                                |                      |
| quarter      | `{"pub_date__quarter" : "1"}`        | `WHERE EXTRACT('QUARTER' FROM pub_date) = '1'`    | 一年中的季度	              |
| time         | `{"pub_date__time" : "12:59:59"}`    | ``                                                |                      |
| hour         | `{"pub_date__hour" : "12"}`          | `WHERE EXTRACT('HOUR' FROM pub_date) = '12'`      | 小时(0-23)             |
| minute       | `{"pub_date__minute" : "59"}`        | `WHERE EXTRACT('MINUTE' FROM pub_date) = '59'`    | 分钟 (0-59)            |
| second       | `{"pub_date__second" : "59"}`        | `WHERE EXTRACT('SECOND' FROM pub_date) = '59'`    | 秒 (0-59)             |

AND

## Google AIP表达式

遵循[AIP-160 Filtering][1]规范，以字符串形式传递过滤条件，适配标准化API接口设计需求。

### 逻辑运算符

支持二元逻辑运算符，用于组合多个条件：

| 运算符   | 示例            | 说明                           |
|-------|---------------|------------------------------|
| `AND` | `a AND b`     | 当`a`和`b`均为真时，表达式结果为真         |
| `OR`  | `a OR b OR c` | 当`a`、`b`、`c`中任意一个为真时，表达式结果为真 |

> 注意：与多数编程语言不同，`OR`运算符优先级高于`AND`。例如`a AND b OR c`等价于`a AND (b OR c)`，建议使用显式括号明确逻辑优先级，避免歧义。

### 否定运算符

| 运算符   | 示例      | 说明              |
|-------|---------|-----------------|
| `NOT` | `NOT a` | 当`a`为假时，表达式结果为真 |
| `-`   | `-a`    | 与`NOT a`等价，简写形式 |

### 比较运算符

| 运算符  | 示例           | 说明                                    |
|------|--------------|---------------------------------------|
| `=`  | `a = true`   | 当`a`的值为真时，表达式结果为真                     |
| `!=` | `a != 42`    | 当`a`的值不等于`42`时，表达式结果为真                |
| `<`  | `a < 42`     | 当`a`为数值且小于`42`时，表达式结果为真               |
| `>`  | `a > "foo"`  | 当`a`为字符串且按词法顺序排在`"foo"`之后时，表达式结果为真    |
| `<=` | `a <= "foo"` | 当`a`为字符串且是`"foo"`或按词法顺序排在其之前时，表达式结果为真 |
| `=>` | `a >= 42`    | 当`a`为数值且大于等于`42`时，表达式结果为真             |

> 注意：与大多数编程语言不同，字段名称必须出现在比较运算符的左侧；右侧只接受字面值和逻辑运算符。

由于过滤器被当作查询字符串接受，因此会进行类型转换，将字符串转换为相应的强类型值：

- 枚举类型，需要枚举类型的字符串表示形式（区分大小写）。
- 布尔值，需要满足`true`、`false`字面值要求。
- 数字类型，接受标准的整数或浮点数表示形式。对于浮点数，支持指数（例如`2.997e9`）。
- [持续时间 Duration][4]
  ，需要以数字形式表示，后跟一个`s`后缀（表示秒）。例如：`20s`，`1.2s`。
- [时间戳 Timestamp][5]
  ，需要采用[RFC-3339][6]格式的字符串（例如 `2012-04-21T11:30:00-04:00`）。支持 UTC 偏移量。

### 遍历算子

通过`.`（点运算符）实现消息、映射或结构的嵌套遍历：

| 示例              | 说明                                  |
|-----------------|-------------------------------------|
| `a.b = true`    | 当`a`包含布尔字段`b`且值为真时，表达式结果为真          |
| `a.b > 42`      | 当`a`包含数值字段`b`且值大于42时，表达式结果为真        |
| `a.b.c = "foo"` | 当`a.b`包含字符串字段`c`且值为`"foo"`时，表达式结果为真 |

### 存在运算符

通过`:`（冒号运算符）表示“存在匹配”，适用于集合（重复字段、映射）及消息类型，行为随数据类型略有差异：

#### 重复字段查询

用于判断重复结构中是否包含匹配元素：

| 示例         | 说明                                 |
|------------|------------------------------------|
| `r:42`     | 当重复字段r中包含42时，表达式结果为真               |
| `r.foo:42` | 当重复字段r中存在元素e，且e的foo字段值为42时，表达式结果为真 |

# 参考资料

- [AIP-160 Filtering （Google官方API过滤规范）][1]
- [Tortoise ORM Filtering（ORM过滤语法参考）][2]
- [Django Field lookups（ORM字段查找规则参考）][3]

[1]:(https://google.aip.dev/160)

[2]:(https://tortoise.github.io/query.html#filtering)

[3]:(https://docs.djangoproject.com/en/4.2/ref/models/querysets/#field-lookups)

[4]:(https://github.com/protocolbuffers/protobuf/blob/master/src/google/protobuf/duration.proto)

[5]:(https://github.com/protocolbuffers/protobuf/blob/master/src/google/protobuf/timestamp.proto)

[6]:(https://tools.ietf.org/html/rfc3339)