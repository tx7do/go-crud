module github.com/tx7do/go-crud/pagination

go 1.24.11

require (
	github.com/go-kratos/kratos/v2 v2.9.2
	github.com/google/go-cmp v0.7.0
	github.com/tx7do/go-crud/api v0.0.2
	github.com/tx7do/go-utils v1.1.34
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/google/gnostic v0.7.1 // indirect
	github.com/google/gnostic-models v0.7.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
)

replace github.com/tx7do/go-crud => ../api
