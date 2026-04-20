module github.com/tx7do/go-crud/opensearch

go 1.25.0

replace github.com/tx7do/go-crud/api => ../api

replace github.com/tx7do/go-crud/pagination => ../pagination

require (
	github.com/go-kratos/kratos/v2 v2.9.2
	github.com/opensearch-project/opensearch-go/v4 v4.6.0
	github.com/stretchr/testify v1.11.1
	github.com/tx7do/go-crud/api v0.0.7
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/google/gnostic v0.7.1 // indirect
	github.com/google/gnostic-models v0.7.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/sys v0.43.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260414002931-afd174a4e478 // indirect
	google.golang.org/grpc v1.80.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
