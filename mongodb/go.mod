module github.com/tx7do/go-crud/mongodb

go 1.26.3

replace github.com/tx7do/go-crud/api => ../api

replace github.com/tx7do/go-crud/pagination => ../pagination

require (
	github.com/stretchr/testify v1.11.1
	github.com/tx7do/go-crud/api v0.0.7
	github.com/tx7do/go-crud/pagination v0.0.15
	github.com/tx7do/go-utils v1.1.40
	github.com/tx7do/go-utils/mapper v0.0.3
	github.com/tx7do/go-wind v0.0.2
	github.com/tx7do/go-wind-plugins/encoding v0.0.1
	github.com/tx7do/go-wind-plugins/encoding/json v0.0.1
	go.mongodb.org/mongo-driver/v2 v2.8.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/google/gnostic v0.7.1 // indirect
	github.com/google/gnostic-models v0.7.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/jinzhu/copier v0.4.0 // indirect
	github.com/klauspost/compress v1.19.2 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/xdg-go/pbkdf2 v1.0.0 // indirect
	github.com/xdg-go/scram v1.2.0 // indirect
	github.com/xdg-go/stringprep v1.0.4 // indirect
	github.com/youmark/pkcs8 v0.0.0-20240726163527-a2c0da244d78 // indirect
	go.einride.tech/aip v0.86.3 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/crypto v0.55.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/text v0.41.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20260810153831-ec0a7760b754 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260810153831-ec0a7760b754 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/tx7do/go-crud => ../
