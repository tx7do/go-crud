module github.com/tx7do/go-crud/cassandra

go 1.25.0

replace github.com/tx7do/go-crud => ../

replace github.com/tx7do/go-crud/viewer => ../viewer

require (
	github.com/gocql/gocql v1.7.0
	github.com/stretchr/testify v1.11.1
	github.com/tx7do/go-crud/viewer v0.0.7
	github.com/tx7do/go-utils/mapper v0.0.3
	github.com/tx7do/go-wind v0.0.3
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/jinzhu/copier v0.4.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
