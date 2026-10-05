package weaviate

import (
	"reflect"
	"regexp"

	"github.com/weaviate/weaviate/entities/models"

	"github.com/tx7do/go-crud/vector"
)

// ─────────────────────────────────────────────────────────────────────────────
// 实体 → Weaviate class schema 映射。
//
// class（集合）必须先于数据创建：属性按实体反射构建（拍平匿名嵌入），
// 向量字段不进属性（向量经数据通道写入），ID 通道字段不进属性
// （UUID 是对象身份）。属性类型映射：
//   string → text；bool → boolean；有符号整数 → int；float → number；
//   []string → text[]；[]int → int[]；[]float → number[]；[]bool → boolean[]。
//   无符号整数报错（weaviate int 为 int64，静默截断比报错更糟）。
//
// 类名/属性名校验：weaviate 类名须大写字母开头；两者均以 GraphQL 名称
// 出现在本模块生成的查询文本中（属性名会进 Get 字段列表），一律用保守
// 白名单校验，杜绝经字段名的注入面。
// ─────────────────────────────────────────────────────────────────────────────

// classNamePattern weaviate 类名约束（大写字母开头，字母/数字/下划线）。
var classNamePattern = regexp.MustCompile(`^[A-Z][A-Za-z0-9_]*$`)

// propertyNamePattern 属性名约束（小写字母/下划线开头）。weaviate 的
// GraphQL 读侧会把大写开头的属性名自动小写化（"Title"→"title"），强制
// 小写开头保证 REST 写入、GraphQL 查询与返回三处命名一致。
var propertyNamePattern = regexp.MustCompile(`^[a-z_][A-Za-z0-9_]*$`)

// weaviateDistances 统一度量枚举 → weaviate 距离度量（建库期固定）。
// 未指定（空串）按 cosine 处理，与其余引擎约定一致。
var weaviateDistances = map[vector.DistanceMetric]string{
	"":                      "cosine",
	vector.MetricCosine:     "cosine",
	vector.MetricDotProduct: "dot",
	vector.MetricEuclidean:  "l2-squared",
}

// weaviateDistanceToScore 把 weaviate 原生距离换算为统一语义的相似度分：
// cosine 距离 = 1 - 余弦相似度 → 还原为 cos；dot 距离 = -点积 → 取负；
// l2-squared 距离按统一欧氏换算 1/(1+d)。
func weaviateDistanceToScore(metric vector.DistanceMetric, raw float64) float64 {
	switch metric {
	case vector.MetricDotProduct:
		return -raw
	case vector.MetricEuclidean:
		return vector.DistanceToScore(vector.MetricEuclidean, raw)
	default:
		return 1 - raw
	}
}

// propSpec 实体字段到 weaviate 属性 / 通道的映射描述。
type propSpec struct {
	name     string       // 生效名（json 标签名或字段名）
	kind     reflect.Kind // 字段类别
	typ      reflect.Type // 字段类型（列表元素的类型判断用）
	isPK     bool         // ID 通道字段（不进属性）
	isVector bool         // 向量字段（不进属性）
}

// collectFieldSpecs 拍平遍历实体类型，产出字段映射表。
func collectFieldSpecs(rt reflect.Type) ([]propSpec, error) {
	var out []propSpec
	var walk func(rt reflect.Type) error
	walk = func(rt reflect.Type) error {
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			// 匿名嵌入结构体：拍平（mixin 依赖此行为）。
			if f.Anonymous && f.Type.Kind() == reflect.Struct {
				if err := walk(f.Type); err != nil {
					return err
				}
				continue
			}
			if !f.IsExported() {
				continue
			}
			name := f.Name
			if tag, ok := f.Tag.Lookup("json"); ok {
				t := parseJSONTagName(tag)
				if t == "-" {
					continue
				}
				if t != "" {
					name = t
				}
			}
			spec := propSpec{
				name:     name,
				kind:     f.Type.Kind(),
				typ:      f.Type,
				isPK:     isIDField(f) && f.Type.Kind() == reflect.String,
				isVector: isVectorField(f),
			}
			out = append(out, spec)
		}
		return nil
	}
	if err := walk(rt); err != nil {
		return nil, err
	}
	return out, nil
}

// weaviatePropTypes 属性类型映射；无符号整数刻意缺席（报错而非截断）。
var weaviatePropTypes = map[reflect.Kind]string{
	reflect.Bool:    "boolean",
	reflect.Int:     "int",
	reflect.Int8:    "int",
	reflect.Int16:   "int",
	reflect.Int32:   "int",
	reflect.Int64:   "int",
	reflect.Float32: "number",
	reflect.Float64: "number",
	reflect.String:  "text",
}

var weaviateSlicePropTypes = map[reflect.Kind]string{
	reflect.Bool:    "boolean[]",
	reflect.Int:     "int[]",
	reflect.Int8:    "int[]",
	reflect.Int16:   "int[]",
	reflect.Int32:   "int[]",
	reflect.Int64:   "int[]",
	reflect.Float32: "number[]",
	reflect.Float64: "number[]",
	reflect.String:  "text[]",
}

func isUnsignedKind(k reflect.Kind) bool {
	switch k {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return true
	}
	return false
}

// buildClass 按实体映射构建 weaviate class schema。
// 向量维度无需声明（weaviate 向量维度由数据决定）；度量写入
// vectorIndexConfig.distance（建库期固定，查询期不可换）。
func buildClass[ENTITY any](className string, metric vector.DistanceMetric) (*models.Class, error) {
	if !classNamePattern.MatchString(className) {
		return nil, ErrInvalidRequest
	}
	// 与其余引擎约定一致：未指定度量时按 cosine 处理。
	if metric == "" {
		metric = vector.MetricCosine
	}
	distance, ok := weaviateDistances[metric]
	if !ok {
		return nil, ErrInvalidRequest
	}

	var e ENTITY
	specs, err := collectFieldSpecs(reflect.TypeOf(&e).Elem())
	if err != nil {
		return nil, err
	}

	class := &models.Class{
		Class:             className,
		Vectorizer:        "none", // 向量由调用方给定，不做文本向量化
		VectorIndexConfig: map[string]any{"distance": distance},
	}
	for _, spec := range specs {
		if spec.isPK || spec.isVector {
			continue
		}
		if !propertyNamePattern.MatchString(spec.name) {
			return nil, ErrInvalidRequest
		}
		kind := spec.kind
		if kind == reflect.Pointer {
			// 指针字段（如 mixin 的 *uint32 租户）按元素类型映射属性；
			// 服务端会按写入 JSON 值自动推断未声明属性的类型（浮点），
			// 漏声明会让后续 valueInt 过滤被拒，故此处必须显式落 schema。
			kind = spec.typ.Elem().Kind()
		}
		var dt string
		switch {
		case kind == reflect.Slice && spec.kind == reflect.Pointer:
			return nil, ErrInvalidRequest // 指针切片不进 schema
		case spec.kind == reflect.Slice:
			dt, ok = weaviateSlicePropTypes[spec.typ.Elem().Kind()]
			if !ok {
				if isUnsignedKind(spec.typ.Elem().Kind()) {
					return nil, ErrInvalidRequest
				}
				continue // 无对应类型（映射/结构体切片等）不进 schema
			}
		default:
			dt, ok = weaviatePropTypes[kind]
			if !ok {
				if isUnsignedKind(kind) {
					// 指针承载的无符号（如 mixin 的 *uint32 租户）值域含于
					// int64，映射为 int；裸无符号字段报错而非截断。
					if spec.kind != reflect.Pointer {
						return nil, ErrInvalidRequest
					}
					dt = "int"
				} else {
					continue
				}
			}
		}
		class.Properties = append(class.Properties, &models.Property{
			Name:     spec.name,
			DataType: []string{dt},
			// tenant_id 参与每查询的租户过滤，确保可过滤索引。
			IndexFilterable: boolPtr(spec.name == "tenant_id"),
		})
	}
	return class, nil
}

func boolPtr(v bool) *bool {
	return &v
}

// propFieldNames 返回映射表中可查询的属性名（GraphQL Get 字段列表）。
func propFieldNames(specs []propSpec) []string {
	out := make([]string, 0, len(specs))
	for _, spec := range specs {
		if spec.isPK || spec.isVector {
			continue
		}
		out = append(out, spec.name)
	}
	return out
}

// resolveSpecsOf 缓存过的实体映射表查询入口（泛型包装）。
func resolveSpecsOf[ENTITY any]() ([]propSpec, error) {
	var e ENTITY
	return collectFieldSpecs(reflect.TypeOf(&e).Elem())
}
