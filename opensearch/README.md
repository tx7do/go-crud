# OpenSearch

## 概念对比

| ES存储结构   | RDBMS存储结构 |
|----------|-----------|
| Index    | 表         |
| Document | 行         |
| Field    | 表字段       |
| Mapping  | 表结构定义     |

## mapping

- 动态映射（dynamic mapping）
- 显式映射（explicit mapping）
- 严格映射（strict mappings）

## Docker部署

```bash
docker pull opensearchproject/opensearch:latest
docker pull opensearchproject/opensearch-dashboards:latest

docker run -itd \
    --name opensearch-node \
    --network=app-tier \
    -p 9200:9200 \
    -p 9600:9600 \
    -e "discovery.type=single-node" \
    -e plugins.security.disabled=false \
    -e plugins.security.ssl.http.enabled=false \
    -e OPENSEARCH_INITIAL_ADMIN_PASSWORD=@Abcd#123456 \
    opensearchproject/opensearch:latest

docker run -itd \
  --name opensearch-dashboards \
  --network app-tier \
  --link opensearch-node \
  -p 5601:5601 \
  -e OPENSEARCH_HOSTS=http://opensearch-node:9200 \
  opensearchproject/opensearch-dashboards:latest
```

Dashboard：<http://localhost:5601/>  
账号密码： `admin` / `@Abcd#123456`
