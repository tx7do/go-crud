module github.com/tx7do/go-crud/clickhouse

go 1.26.3

replace github.com/tx7do/go-crud/api => ../api

replace github.com/tx7do/go-crud/pagination => ../pagination

require (
	github.com/ClickHouse/clickhouse-go/v2 v2.48.0
	github.com/stretchr/testify v1.11.1
	github.com/tx7do/go-crud/api v0.0.7
	github.com/tx7do/go-crud/pagination v0.0.15
	github.com/tx7do/go-utils v1.1.40
	github.com/tx7do/go-utils/mapper v0.0.3
	github.com/tx7do/go-wind v0.0.2
	github.com/tx7do/go-wind-plugins/encoding v0.0.1
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/ClickHouse/ch-go v0.74.0 // indirect
	github.com/andybalholm/brotli v1.2.2 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.8.0 // indirect
	github.com/google/gnostic v0.7.1 // indirect
	github.com/google/gnostic-models v0.7.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jinzhu/copier v0.4.0 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/paulmach/orb v0.13.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.28 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/segmentio/asm v1.2.1 // indirect
	github.com/shopspring/decimal v1.4.0 // indirect
	github.com/tx7do/go-wind-plugins/encoding/json v0.0.1 // indirect
	go.einride.tech/aip v0.86.3 // indirect
	go.opentelemetry.io/otel v1.45.0 // indirect
	go.opentelemetry.io/otel/trace v1.45.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/sys v0.47.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260810153831-ec0a7760b754 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260810153831-ec0a7760b754 // indirect
	google.golang.org/grpc v1.83.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/tx7do/go-crud => ../
