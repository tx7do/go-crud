package milvus

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/milvus-io/milvus-sdk-go/v2/entity"

	"github.com/tx7do/go-crud/vector"
)

// ─────────────────────────────────────────────────────────────────────────────
// 实体 → Milvus schema 映射。
//
// 背景：官方 SDK 的 ParseSchemaAny（schema 解析）与 parseCandidates（读侧
// 行解析）均跳过匿名嵌入字段，而 mixin（如 TenantID）必须匿名嵌入才能
// 提升 viewer.ScopedModel 方法。因此本模块自带拍平遍历（与 SDK 写侧的
// reflectValueCandi 语义一致）：
//   - 字段名：milvus 标签 name（NAME 键）优先，否则字段名；标签 "-" 跳过；
//   - 匿名嵌入结构体拍平（mixin 的 tenant_id 由此进入 schema，并被标记为
//     partition key —— Milvus 官方推荐的多租户隔离实践）；
//   - 主键：字段名 id/uuid（大小写不敏感）或标签 primary_key 的字段，
//     int64 → Int64 主键，string → VarChar 主键，其余类型报错；
//   - 向量字段：仅接受裸 []float32（FloatVector，维度取自
//     CreateVectorCollection 的 dims 参数；命名 float32 切片类型不被 SDK
//     列构造器接受，报错提示改用 []float32）；
//   - 标量：bool/int8-64/float32/float64/string（VarChar，max_length 固定
//     65535 —— Milvus 的上限）；无符号整数一律报错（无对应类型，且 SDK
//     列构造器按严格类型断言，静默截断比报错更糟）；其余类型跳过。
//
// 插入路径复用官方 entity.AnyToColumns(rows, schema)：其行值提取
// （reflectValueCandi）同样拍平匿名嵌入并按标签名映射，与本构建器产出的
// schema 字段集一致。
// ─────────────────────────────────────────────────────────────────────────────

// varcharMaxLength Milvus VarChar 字段 max_length 的固定取值（引擎上限）。
const varcharMaxLength = 65535

// fieldSpec 实体字段到 Milvus schema 字段的映射描述。
type fieldSpec struct {
	path     []int        // 反射字段索引路径（匿名嵌入拍平后指向实际字段）
	typ      reflect.Type // 字段类型（仅用于向量字段的精确类型核对）
	name     string       // 生效名（milvus 标签名或字段名）
	kind     reflect.Kind // 字段类别
	isPK     bool
	isTenant bool
	isVector bool
}

// collectFieldSpecs 拍平遍历实体类型，产出字段映射表。
func collectFieldSpecs(rt reflect.Type) ([]fieldSpec, error) {
	var out []fieldSpec
	var walk func(rt reflect.Type, prefix []int) error
	walk = func(rt reflect.Type, prefix []int) error {
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			// 匿名嵌入结构体：拍平（mixin 依赖此行为）。
			if f.Anonymous && f.Type.Kind() == reflect.Struct {
				next := append(append([]int{}, prefix...), i)
				if err := walk(f.Type, next); err != nil {
					return err
				}
				continue
			}
			if !f.IsExported() {
				continue
			}
			if f.Tag.Get(entity.MilvusTag) == entity.MilvusSkipTagValue {
				continue
			}
			tagSettings := entity.ParseTagSetting(f.Tag.Get(entity.MilvusTag), entity.MilvusTagSep)
			name := f.Name
			if tn, has := tagSettings[entity.MilvusTagName]; has && tn != "" {
				name = tn
			}
			spec := fieldSpec{
				path: append(append([]int{}, prefix...), i),
				typ:  f.Type,
				name: name,
				kind: f.Type.Kind(),
			}
			ln := strings.ToLower(f.Name)
			_, tagPK := tagSettings[entity.MilvusPrimaryKey]
			spec.isPK = tagPK || ln == "id" || ln == "uuid"
			spec.isTenant = name == "tenant_id"
			spec.isVector = f.Type.Kind() == reflect.Slice && f.Type.Elem().Kind() == reflect.Float32
			out = append(out, spec)
		}
		return nil
	}
	if err := walk(rt, nil); err != nil {
		return nil, err
	}
	return out, nil
}

// scalarFieldTypes 标量类型映射；无符号整数刻意缺席（报错而非截断）。
var scalarFieldTypes = map[reflect.Kind]entity.FieldType{
	reflect.Bool:    entity.FieldTypeBool,
	reflect.Int8:    entity.FieldTypeInt8,
	reflect.Int16:   entity.FieldTypeInt16,
	reflect.Int32:   entity.FieldTypeInt32,
	reflect.Int64:   entity.FieldTypeInt64,
	reflect.Float32: entity.FieldTypeFloat,
	reflect.Float64: entity.FieldTypeDouble,
	reflect.String:  entity.FieldTypeVarChar,
}

func isUnsignedKind(k reflect.Kind) bool {
	switch k {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return true
	}
	return false
}

// buildSchema 按实体映射构建 Milvus schema。
func buildSchema[ENTITY any](collection string, dims int) (*entity.Schema, error) {
	if collection == "" {
		return nil, ErrInvalidRequest
	}
	if dims <= 0 {
		return nil, fmt.Errorf("%w: dims must be positive, got %d", ErrInvalidRequest, dims)
	}

	var e ENTITY
	specs, err := collectFieldSpecs(reflect.TypeOf(&e).Elem())
	if err != nil {
		return nil, err
	}

	schema := entity.NewSchema().WithName(collection)
	for _, spec := range specs {
		field := entity.NewField().WithName(spec.name)
		switch {
		case spec.isPK:
			switch {
			case spec.kind == reflect.Int64:
				field = field.WithDataType(entity.FieldTypeInt64)
			case spec.kind == reflect.String:
				field = field.WithDataType(entity.FieldTypeVarChar).WithMaxLength(varcharMaxLength)
			default:
				return nil, fmt.Errorf("%w: primary key field %q must be int64 or string", ErrInvalidPointID, spec.name)
			}
			field = field.WithIsPrimaryKey(true).WithIsAutoID(false)
		case spec.isTenant:
			if spec.kind != reflect.Int64 {
				return nil, fmt.Errorf("%w: tenant field must be int64 (embed milvus mixin.TenantID)", ErrSchemaBuildFailed)
			}
			// tenant_id 标记为 partition key：按租户值路由到独立分区，
			// 隔离与过滤性能均获益（Milvus 官方多租户实践）。
			field = field.WithDataType(entity.FieldTypeInt64).WithIsPartitionKey(true)
		case spec.isVector:
			if spec.typ.String() != "[]float32" {
				return nil, fmt.Errorf("%w: vector field %q must be plain []float32 (named float32 slice types are rejected by the SDK column builders)", ErrSchemaBuildFailed, spec.name)
			}
			field = field.WithDataType(entity.FieldTypeFloatVector).WithDim(int64(dims))
		default:
			dt, ok := scalarFieldTypes[spec.kind]
			if !ok {
				if isUnsignedKind(spec.kind) {
					return nil, fmt.Errorf("%w: unsigned field %q has no Milvus counterpart; use a signed type", ErrSchemaBuildFailed, spec.name)
				}
				continue // 无对应的字段（结构体/映射/其它切片等）不进 schema
			}
			field = field.WithDataType(dt)
			if dt == entity.FieldTypeVarChar {
				field = field.WithMaxLength(varcharMaxLength)
			}
		}
		schema = schema.WithField(field)
	}

	if schema.PKField() == nil {
		return nil, fmt.Errorf("%w: entity has no int64/string id/uuid field", ErrInvalidPointID)
	}
	if len(vectorFieldNames(schema)) == 0 {
		return nil, fmt.Errorf("%w: entity has no []float32 vector field", ErrSchemaBuildFailed)
	}
	return schema, nil
}

// vectorFieldNames 返回 schema 中的全部 FloatVector 字段名。
func vectorFieldNames(schema *entity.Schema) []string {
	var out []string
	for _, f := range schema.Fields {
		if f.DataType == entity.FieldTypeFloatVector {
			out = append(out, f.Name)
		}
	}
	return out
}

// resolveVectorFieldSpecs 按字段映射表解析向量检索的目标向量字段：
// 显式给定且在映射表内 → 用之；未给定且恰有一个 → 默认该字段；其余报错。
func resolveVectorFieldSpecs(specs []fieldSpec, requested string) (string, error) {
	var names []string
	for _, spec := range specs {
		if spec.isVector {
			names = append(names, spec.name)
		}
	}
	if requested != "" {
		for _, n := range names {
			if n == requested {
				return requested, nil
			}
		}
		return "", fmt.Errorf("%w: vector field %q not in schema", ErrInvalidVectorQuery, requested)
	}
	if len(names) == 1 {
		return names[0], nil
	}
	return "", fmt.Errorf("%w: multiple vector fields in schema, query.Field is required", ErrInvalidVectorQuery)
}

// milvusMetrics 统一度量枚举 → Milvus 度量枚举。
// 未指定（空串）按 cosine 处理，与其余引擎约定一致。
var milvusMetrics = map[vector.DistanceMetric]entity.MetricType{
	"":                      entity.COSINE,
	vector.MetricCosine:     entity.COSINE,
	vector.MetricEuclidean:  entity.L2,
	vector.MetricDotProduct: entity.IP,
}

// milvusScoreToScore 把 Milvus 原生分数换算为统一语义的相似度分
// （L2 为距离，其余为相似度，直接透传）。
func milvusScoreToScore(metric vector.DistanceMetric, raw float32) float64 {
	if metric == vector.MetricEuclidean {
		return vector.DistanceToScore(vector.MetricEuclidean, float64(raw))
	}
	return float64(raw)
}
