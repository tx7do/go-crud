# ElasticSearch

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
# LTS
docker pull elasticsearch:8.19.14
docker pull elasticsearch:9.3.3

docker run -itd \
    --name elasticsearch-node \
    --network=app-tier \
    -p 9200:9200 \
    -p 9300:9300 \
    -e "discovery.type=single-node" \
    -e "DISABLE_SECURITY_PLUGIN=true" \
    -e OPENSEARCH_INITIAL_ADMIN_PASSWORD=@Abcd#123456 \
    elasticsearch:8.19.14
```
