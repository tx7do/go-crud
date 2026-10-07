REM 零伪版本门禁：模块目录内的 replace => ../xxx 会掩盖悬空版本，go mod tidy 会把
REM 类内 require 记成 v0.0.0-00010101000000-000000000000；该版本号下游永远无法解析，
REM replace 只对主模块生效，消费者 go get 必失败（milvus/weaviate v0.0.1 前车之鉴）。
findstr /s /m /c:"v0.0.0-00010101000000-000000000000" *.go.mod >NUL 2>&1
if %errorlevel%==0 (
    echo [ERROR] 检测到 go.mod 零伪版本 require，先落真实已发布版本号再打标。命中文件：
    findstr /s /m /c:"v0.0.0-00010101000000-000000000000" *.go.mod
    exit /b 1
)

git tag api/v0.0.7 --force
git tag pagination/v0.0.16 --force
git tag viewer/v0.0.7 --force
git tag audit/v0.0.3 --force
git tag cache/v0.0.2 --force

git tag entgo/v0.0.55 --force
git tag gorm/v0.0.24 --force

git tag cassandra/v0.0.6 --force
git tag elasticsearch/v0.0.12 --force
git tag opensearch/v0.0.8 --force
git tag clickhouse/v0.0.21 --force
git tag influxdb/v0.0.15 --force
git tag mongodb/v0.0.16 --force
git tag doris/v0.0.19 --force

git tag qdrant/v0.0.1 --force
git tag milvus/v0.0.2 --force
git tag weaviate/v0.0.2 --force
git tag neo4j/v0.0.1 --force
git tag vector/v0.0.1 --force

git push origin --tags
