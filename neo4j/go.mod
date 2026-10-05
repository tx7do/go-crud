module github.com/tx7do/go-crud/neo4j

go 1.26.3

replace github.com/tx7do/go-crud/viewer => ../viewer

require (
	github.com/neo4j/neo4j-go-driver/v5 v5.28.5
	github.com/stretchr/testify v1.12.1
	github.com/tx7do/go-crud/viewer v0.0.7
	github.com/tx7do/go-utils/mapper v0.0.3
	github.com/tx7do/go-wind v0.0.3
)

require (
	github.com/jinzhu/copier v0.4.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
)
