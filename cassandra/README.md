# Cassandra

本模块基于 [gocql](https://github.com/gocql/gocql) 封装 Cassandra 的通用 DAL 仓库。

## ScyllaDB 兼容性

[ScyllaDB](https://www.scylladb.com/) 与 Cassandra 使用同一套 CQL 二进制协议，
本模块（gocql 驱动）**无需任何改动即可直连 ScyllaDB**——把 `WithHosts` 指向
ScyllaDB 节点即可。两点差异需要知晓：

- Scylla 官方维护的驱动是其 gocql 分叉（shard-aware 连接路由等性能优化）；
  本模块按生态通用性选择上游 gocql，连接 Scylla 功能完整，但不含分片感知优化。
- Scylla 不支持 Cassandra 的全部特性（如物化视图等），以其官方兼容矩阵为准。

## 租户隔离

实体嵌入 `cassandra/mixin.TenantID` 后自动启用租户行级强制（缺身份
fail-closed、平台/系统视图放行），语义与其余引擎模块一致。
