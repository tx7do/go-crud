module github.com/tx7do/go-crud/qdrant

go 1.26.3

replace github.com/tx7do/go-crud/vector => ../vector

replace github.com/tx7do/go-crud/viewer => ../viewer

require (
	github.com/qdrant/go-client v1.19.3
	github.com/stretchr/testify v1.12.1
	github.com/tx7do/go-crud/vector v0.0.1
	github.com/tx7do/go-crud/viewer v0.0.7
	github.com/tx7do/go-utils/mapper v0.0.3
	github.com/tx7do/go-wind v0.0.2
)

require (
	github.com/jinzhu/copier v0.4.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260706201446-f0a921348800 // indirect
	google.golang.org/grpc v1.84.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
