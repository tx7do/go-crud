::指定起始文件夹
set DIR=%cd%

go get all
go mod tidy

cd %DIR%\api
go get all
go mod tidy

cd %DIR%\pagination
go get all
go mod tidy

cd %DIR%\entgo
go get all
go mod tidy

cd %DIR%\gorm
go get all
go mod tidy

cd %DIR%\cassandra
go get all
go mod tidy

cd %DIR%\clickhouse
go get all
go mod tidy

cd %DIR%\elasticsearch
go get all
go mod tidy

cd %DIR%\influxdb
go get all
go mod tidy

cd %DIR%\mongodb
go get all
go mod tidy
