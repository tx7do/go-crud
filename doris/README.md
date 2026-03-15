# Apache Doris — 简介与使用说明

本文档对 Apache Doris 做一个简短介绍，并说明如何通过本仓库提供的 Go 客户端连接与使用 Doris（包括 SQL、Stream Load、session/事务与批量插入等常见场景）。

如果你只是想快速启动一个本地 Doris 集群，可以参考下面的 Docker 部署示例（已保留在文档末尾）。

## 什么是 Doris

Apache Doris（前身为 Apache Incubator 中的 Doris，企业名为 Palo）是一款面向实时分析（OLAP）的分布式列式数据库，主要特点包括：

- 兼容 MySQL 协议：支持通过 MySQL 客户端或驱动连接（便于集成现有工具）。
- 高性能分析：支持列式存储、向量化执行、MPP 调度（多副本分布式查询引擎）以提高分析查询吞吐。
- 多种写入方式：支持批量导入、Stream Load（实时导入）、Broker Load 等多种数据接入方式。
- 丰富的部署模式：单机快速试验、集群部署（FE/BE）及云环境部署方案。

适合用例：交互式分析、仪表盘、近实时的 OLAP 场景，例如日志/事件分析、时序分析与 BI 报表。

## 架构概览

- FE（Frontend）：负责元数据管理、解析 SQL、全局调度，提供对外的 SQL/HTTP 接口（例如 Stream Load 的 HTTP 接口通常暴露在 FE 上）。
- BE（Backend）：负责数据存储（列式存储）、实际的查询执行（扫描、聚合等）。

通常一个集群会有多个 FE（高可用）和多个 BE（水平扩展存储和计算）。

## 关键特性

- MySQL 协议支持 -> 可用 MySQL 驱动/客户端访问。
- Stream Load -> 通过 HTTP 将 CSV/JSON/其他格式数据实时推送到表中（常用于近实时导入）。
- 支持会话级变量（SET SESSION ...），可以控制 query plan、内存限制、时区等执行行为。
- 支持事务语义（有限度，主要用于小范围原子操作，注意并非全部场景下的强一致 OLTP）。

## 本仓库的 Doris 客户端（Go）概览

本仓库在 `doris` 包中实现了一个轻量级的 Doris 客户端，目标是方便在 Go 应用中：

- 使用 database/sql / sqlx 进行 SQL 查询/批量插入。
- 使用 HTTP 的 Stream Load 接口上传大量数据。
- 管理 session 变量（SET SESSION ...）以及在单连接上设置 session 并执行事务（保证这些 session 对事务内的查询生效）。

主要接口（摘选）：

- NewClient(opts...) (*Client, error) — 创建客户端。
- Exec / Get / Select / BatchInsert — 基本 SQL 操作与批量插入支持。
- StreamLoad(ctx, db, table, params, data) — 使用 FE 的 Stream Load HTTP 接口上传数据。
- SetSessionVars(ctx, vars) — 在连接池上执行多个 SET SESSION。
- RunWithSession(ctx, vars, fn) — 在同一物理连接上设置 session，执行回调（保证 session 对回调内可见）。
- BeginTxWithSession(ctx, vars, opts) (*TxWithConn, error) — 在同一连接上设置 session 并开启事务，返回需要 Commit/Rollback 的事务对象。
- WithTxWithSession(ctx, vars, opts, fn) — 便捷方法：在同一连接上设置 session、开始事务并在回调中执行，自动 Commit/Rollback。

示例（使用本仓库实现的客户端）：

```go
// 创建 client（示例，注意替换 DSN）
cli, err := doris.NewClient(doris.WithDSN("user:pass@tcp(host:3306)/dbname"))
defer cli.Close()

// 批量插入
cols := []string{"id", "name"}
rows := [][]interface{}{{1, "alice"}, {2, "bob"}}
_, err = cli.BatchInsert(ctx, "posts", cols, rows)

// Stream Load（向 FE 的 /api/{db}/{table}/_stream_load 上传）
params := map[string]string{"columns": "id,name", "format": "csv", "label": "label_123"}
data := strings.NewReader("1,alice\n2,bob\n")
body, status, err := cli.StreamLoad(ctx, "mydb", "mytable", params, data)

// 在同一连接/事务中设置 session 并执行
vars := map[string]string{"exec_mem_limit": "4G", "time_zone": "Asia/Shanghai"}
err = cli.WithTxWithSession(ctx, vars, nil, func(tx *sql.Tx) error {
    // 在事务内执行受 session 影响的语句
    _, err := tx.ExecContext(ctx, "INSERT INTO ...")
    return err
})
```

> 注意：当你在连接池上直接执行 `SET SESSION` 时，SET 仅影响执行该 SET 的物理连接；如果你需要保证接下来的一系列语句（尤其是事务内）都受该设置影响，请通过 `RunWithSession` / `BeginTxWithSession` / `WithTxWithSession` 等 API 在同一连接上设置并执行。

## 常见 session 变量示例

下面是一些常见的 session 变量（可通过 `SetSessionVars` 或在单连接上 `SET`）：

- `SET enable_profile = true;` — 开启查询分析。
- `SET sql_select_limit = 10000;` — 限制默认返回的最大行数。
- `SET exec_mem_limit = 4G;` — 限制单次查询使用的内存（可带单位）。
- `SET time_zone = 'Asia/Shanghai';` — 设置时区（字符串需要引号）。

在我们的客户端中，SetSessionVars 会对变量名做白名单校验，并对值进行安全格式化：

- 布尔值 `true/false` 不会加引号。
- 类似 `4G` 的数值（带单位）会按原样使用。
- 普通字符串会被单引号包裹并对内部单引号进行转义。

## 运维与性能建议

- 对大规模导入推荐使用 Stream Load 或 Broker Load，Stream Load 适合近实时小批量导入。
- 合理设置 `exec_mem_limit` 与 `sql_select_limit` 避免单查询占用过多资源。
- 使用多个 BE 节点分担存储与查询任务以获得更好扩展性。

## Docker 快速部署

下面给出快速启动 FE/BE 的示例（同仓库原始内容）：

### Docker run

```bash
docker pull apache/doris:be-4.0.4
docker pull apache/doris:fe-4.0.4

docker network create --driver bridge --subnet=172.20.80.0/24 doris-network

mkdir -p /data/fe/{doris-meta,conf,log}
mkdir -p /data/be/{storage,conf,log}
chmod -R 777 /data/fe /data/be

docker run -itd \
    --name=doris-fe \
    --env FE_SERVERS="fe1:172.20.80.2:9010" \
    --env FE_ID=1 \
    -p 8030:8030 \
    -p 9030:9030 \
    -p 9010:9010 \
    -v /data/fe/doris-meta:/opt/apache-doris/fe/doris-meta \
    -v /data/fe/conf:/opt/apache-doris/fe/conf \
    -v /data/fe/log:/opt/apache-doris/fe/log \
    --network=doris-network \
    --ip=172.20.80.2 \
    apache/doris:fe-4.0.4

docker run -itd \
    --name=doris-be \
    --env FE_SERVERS="fe1:172.20.80.2:9010" \
    --env BE_ADDR="172.20.80.3:9050" \
    -p 8040:8040 \
    -p 9050:9050 \
    -v /data/be/storage:/opt/apache-doris/be/storage \
    -v /data/be/conf:/opt/apache-doris/be/conf \
    -v /data/be/log:/opt/apache-doris/be/log \
    --network=doris-network \
    --ip=172.20.80.3 \
    apache/doris:be-4.0.4
```

### Docker compose

```yaml
version: "3"
networks:
  custom_network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.80.0/24

services:
  fe:
    image: apache/doris:fe-${DORIS_QUICK_START_VERSION}
    hostname: fe
    ports:
      - 8030:8030
      - 9030:9030
      - 9010:9010
    environment:
      - FE_SERVERS=fe1:172.20.80.2:9010
      - FE_ID=1
    networks:
      custom_network:
        ipv4_address: 172.20.80.2

  be:
    image: apache/doris:be-${DORIS_QUICK_START_VERSION}
    hostname: be
    ports:
      - 8040:8040
      - 9050:9050
    environment:
      - FE_SERVERS=fe1:172.20.80.2:9010
      - BE_ADDR=172.20.80.3:9050
    depends_on:
      - fe
    networks:
      custom_network:
        ipv4_address: 172.20.80.3
```

## 参考链接

- 官方网站与文档：https://doris.apache.org/
- Stream Load 文档（FE HTTP 接口）：https://doris.apache.org/docs/zh-CN/latest/administrator-guide/import/stream-load/
- Doris GitHub：https://github.com/apache/doris

----

以上为增强后的 `doris` 模块说明和快速使用指南；如果你希望我把 README 中的 Go 示例改为更完整的示例程序（包含完整 imports 和运行步骤），或加入 Stream Load 的示例 HTTP 请求/返回解析示例，我可以继续补充。 
``` 
