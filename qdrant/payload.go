package qdrant

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	qdrant "github.com/qdrant/go-client/qdrant"
)

// ─────────────────────────────────────────────────────────────────────────────
// 实体 ↔ Qdrant 载荷转换。
//
// Qdrant 的载荷是 JSON 语义的树（qdrant.Value，json_with_int 方言：整数与
// 浮点分离，保留 int64 精度）：
//   - 编码（entity → payload）：反射遍历导出字段，字段名取 json 标签（无标签
//     取字段名），嵌入的匿名结构体字段拍平（与 mixin 嵌入配合），
//     基本类型/切片/映射直接映射，保持整数类型不经过 JSON 浮点化；
//   - 解码（payload → entity）：Value 树 → any 树 → JSON 字节 → json.Unmarshal
//     进实体（JSON 反序列化按目标字段类型还原整数，指针字段按 nil/值处理）。
// ─────────────────────────────────────────────────────────────────────────────

// payloadFromEntity 把实体导出字段转换为 Qdrant 载荷映射。
func payloadFromEntity(v any) (map[string]any, error) {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, fmt.Errorf("%w: nil entity", ErrPayloadConversion)
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%w: entity must be struct, got %s", ErrPayloadConversion, rv.Kind())
	}
	return structToPayloadMap(rv)
}

// structToPayloadMap 拍平遍历结构体字段（含匿名嵌入结构体）。
func structToPayloadMap(rv reflect.Value) (map[string]any, error) {
	rt := rv.Type()
	out := make(map[string]any, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if !f.IsExported() {
			continue
		}
		// 匿名嵌入结构体：字段拍平进父映射（mixin 嵌入依赖此行为）。
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			nested, err := structToPayloadMap(rv.Field(i))
			if err != nil {
				return nil, err
			}
			for k, v := range nested {
				out[k] = v
			}
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
		// 向量字段（[]float32 / vector.Float32Vector）不进载荷：
		// 它们经 Vectors 通道写入（见 pointFromEntity）。
		if isVectorField(f) {
			continue
		}
		val, err := valueToPayload(rv.Field(i))
		if err != nil {
			return nil, err
		}
		if val != nil {
			out[name] = val
		}
	}
	return out, nil
}

// valueToPayload 把单个字段值转换为载荷可表达的 Go 值。
// 不支持的类型返回 nil（字段被跳过），保持与 Qdrant 载荷能力一致。
func valueToPayload(rv reflect.Value) (any, error) {
	switch rv.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.String:
		return rv.Interface(), nil
	case reflect.Pointer:
		if rv.IsNil() {
			return nil, nil
		}
		return valueToPayload(rv.Elem())
	case reflect.Slice:
		// 向量字段不进载荷（见 isVectorField）。
		if rv.Type().Elem().Kind() == reflect.Float32 {
			return nil, nil
		}
		out := make([]any, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			item, err := valueToPayload(rv.Index(i))
			if err != nil {
				return nil, err
			}
			if item != nil {
				out = append(out, item)
			}
		}
		return out, nil
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return nil, nil
		}
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			val, err := valueToPayload(iter.Value())
			if err != nil {
				return nil, err
			}
			if val != nil {
				out[iter.Key().String()] = val
			}
		}
		return out, nil
	default:
		// 结构体、接口、通道、函数等不进入载荷。
		return nil, nil
	}
}

// parseJSONTagName 取 json 标签的名称部分（忽略选项，"-" 表示跳过返回空）。
func parseJSONTagName(tag string) string {
	for i := 0; i < len(tag); i++ {
		if tag[i] == ',' {
			return tag[:i]
		}
	}
	return tag
}

// entityFromPayload 把 Qdrant 载荷还原进实体（Value 树 → JSON → 实体）。
func entityFromPayload(payload map[string]*qdrant.Value, dst any) error {
	if len(payload) == 0 {
		return nil
	}
	native := make(map[string]any, len(payload))
	for k, v := range payload {
		native[k] = valueToNative(v)
	}
	raw, err := json.Marshal(native)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrPayloadConversion, err)
	}
	if err = json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("%w: %v", ErrPayloadConversion, err)
	}
	return nil
}

// valueToNative 把 qdrant.Value 树转换为原生 Go 值树。
func valueToNative(v *qdrant.Value) any {
	if v == nil || v.Kind == nil {
		return nil
	}
	switch kind := v.Kind.(type) {
	case *qdrant.Value_IntegerValue:
		return kind.IntegerValue
	case *qdrant.Value_DoubleValue:
		return kind.DoubleValue
	case *qdrant.Value_StringValue:
		return kind.StringValue
	case *qdrant.Value_BoolValue:
		return kind.BoolValue
	case *qdrant.Value_StructValue:
		nested := make(map[string]any, len(kind.StructValue.Fields))
		for k, fv := range kind.StructValue.Fields {
			nested[k] = valueToNative(fv)
		}
		return nested
	case *qdrant.Value_ListValue:
		items := make([]any, 0, len(kind.ListValue.Values))
		for _, item := range kind.ListValue.Values {
			items = append(items, valueToNative(item))
		}
		return items
	default:
		return nil
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// 点结构组装：实体 → qdrant.PointStruct。
//
// 实体字段的三条通道：
//   - ID 字段（字段名 id/uuid，数值或字符串类型）→ PointId
//     （数值 → NewIDNum，UUID 字符串 → NewIDUUID）；
//   - 向量字段（[]float32 / vector.Float32Vector）→ Vectors（NewVectors），
//     多个向量字段按声明顺序拼接为单一匿名向量；
//   - 其余导出字段 → Payload（见 structToPayloadMap）。
//
// 缺 ID 字段或缺向量字段均报错：向量集合的点两者必备。
// ─────────────────────────────────────────────────────────────────────────────

var floatSliceType = reflect.TypeOf([]float32(nil))

// isVectorField 报告字段是否为向量字段（元素为 float32 的切片，
// 含命名类型 vector.Float32Vector）。
func isVectorField(f reflect.StructField) bool {
	return f.Type.Kind() == reflect.Slice && f.Type.Elem().Kind() == reflect.Float32
}

// pointFromEntity 把实体组装为 Qdrant 点结构。
func pointFromEntity(v any) (*qdrant.PointStruct, error) {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, fmt.Errorf("%w: nil entity", ErrPayloadConversion)
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%w: entity must be struct, got %s", ErrPayloadConversion, rv.Kind())
	}

	id, err := entityPointID(rv)
	if err != nil {
		return nil, err
	}
	vecs, err := entityVectors(rv)
	if err != nil {
		return nil, err
	}

	payloadMap, err := structToPayloadMap(rv)
	if err != nil {
		return nil, err
	}
	payload, err := qdrant.TryValueMap(payloadMap)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrPayloadConversion, err)
	}

	return &qdrant.PointStruct{
		Id:      id,
		Payload: payload,
		Vectors: qdrant.NewVectors(vecs...),
	}, nil
}

// entityPointID 从实体的 id/uuid 字段提取点 ID。
// 仅接受非负数值（→ 数值 ID）与字符串（→ UUID）两类；其余类型或缺失字段报错。
func entityPointID(rv reflect.Value) (*qdrant.PointId, error) {
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if !f.IsExported() {
			continue
		}
		ln := strings.ToLower(f.Name)
		if ln != "id" && ln != "uuid" {
			continue
		}
		fv := rv.Field(i)
		for fv.Kind() == reflect.Pointer {
			if fv.IsNil() {
				return nil, fmt.Errorf("%w: nil id field", ErrInvalidPointID)
			}
			fv = fv.Elem()
		}
		switch fv.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			n := fv.Int()
			if n < 0 {
				return nil, fmt.Errorf("%w: negative point id", ErrInvalidPointID)
			}
			return qdrant.NewIDNum(uint64(n)), nil
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return qdrant.NewIDNum(fv.Uint()), nil
		case reflect.String:
			return qdrant.NewIDUUID(fv.String()), nil
		default:
			return nil, fmt.Errorf("%w: unsupported point id field kind %s", ErrInvalidPointID, fv.Kind())
		}
	}
	return nil, fmt.Errorf("%w: entity has no id/uuid field", ErrInvalidPointID)
}

// entityVectors 收集实体的全部向量字段（[]float32 / vector.Float32Vector），
// 按字段声明顺序拼接为单一向量。命名向量类型与 []float32 底层一致，直接转换。
func entityVectors(rv reflect.Value) ([]float32, error) {
	rt := rv.Type()
	var out []float32
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if !f.IsExported() || !isVectorField(f) {
			continue
		}
		fv := rv.Field(i)
		if fv.Type() != floatSliceType {
			fv = fv.Convert(floatSliceType)
		}
		out = append(out, fv.Interface().([]float32)...)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: entity has no vector field", ErrInvalidRequest)
	}
	return out, nil
}
