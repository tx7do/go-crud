module github.com/tx7do/go-crud/doris

go 1.25.0

replace github.com/tx7do/go-crud/api => ../api

replace github.com/tx7do/go-crud/pagination => ../pagination

require (
	github.com/DATA-DOG/go-sqlmock v1.5.2
	github.com/go-kratos/kratos/v2 v2.9.2
	github.com/go-sql-driver/mysql v1.8.1
	github.com/jmoiron/sqlx v1.4.0
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
)
