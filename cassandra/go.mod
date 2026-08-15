module github.com/tx7do/go-crud/cassandra

go 1.25.0

replace github.com/tx7do/go-crud => ../

require (
	github.com/gocql/gocql v1.7.0
	github.com/tx7do/go-crud/log v0.0.0-00010101000000-000000000000
)

require (
	github.com/golang/snappy v1.0.0 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/tx7do/go-wind v0.0.1 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
)

replace github.com/tx7do/go-crud/log => ../log
