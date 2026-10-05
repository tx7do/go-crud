package weaviate

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

// ─────────────────────────────────────────────────────────────────────────────
// 实体 ↔ Weaviate 对象属性转换。
//
// Weaviate 的对象属性是 JSON 语义的（服务端按声明的属性类型校验）：
//   - 编码（entity → properties）：反射遍历导出字段，字段名取 json 标签
//     （无标签取字段名），嵌入的匿名结构体字段拍平（与 mixin 嵌入配合），
//     基本类型/切片/映射直接映射；
//   - 解码（properties → entity）：属性映射 → JSON 字节 → json.Unmarshal
//     进实体（按目标字段类型还原，指针字段按 nil/值处理）；
//   - ID 通道：字段名 id/uuid（大小写不敏感）的 string 字段承载对象 UUID，
//     不进属性表——UUID 由调用方给定或服务端生成，经 Create 返回值 /
//     GetByUUID / Query 的 _additional.id 回读；
//   - 向量字段（元素为 float32 的切片）不进属性表：经 WithVector 通道写入。
// ─────────────────────────────────────────────────────────────────────────────

var floatSliceType = reflect.TypeOf([]float32(nil))

// isVectorField 报告字段是否为向量字段（元素为 float32 的切片）。
func isVectorField(f reflect.StructField) bool {
	return f.Type.Kind() == reflect.Slice && f.Type.Elem().Kind() == reflect.Float32
}

// isIDField 报告字段是否为 UUID 通道字段（字段名 id/uuid，大小写不敏感）。
func isIDField(f reflect.StructField) bool {
	ln := strings.ToLower(f.Name)
	return ln == "id" || ln == "uuid"
}

// idFieldValue 读取实体 UUID 通道字段的值（缺失或空返回空串）。
func idFieldValue(v any) string {
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return ""
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return ""
	}
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if !f.IsExported() || !isIDField(f) {
			continue
		}
		fv := rv.Field(i)
		if fv.Kind() != reflect.String {
			continue
		}
		return fv.String()
	}
	return ""
}

// setIDFieldValue 把对象 UUID 写回实体的 ID 通道字段（仅 string 类型字段）。
func setIDFieldValue(dst any, id string) {
	rv := reflect.ValueOf(dst)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return
	}
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if !f.IsExported() || !isIDField(f) {
			continue
		}
		fv := rv.Field(i)
		if fv.Kind() == reflect.String && fv.CanSet() {
			fv.SetString(id)
		}
		return
	}
}

// propsFromEntity 把实体导出字段转换为属性映射（ID/向量通道字段不进表）。
func propsFromEntity(v any) (map[string]any, error) {
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
	return structToPropsMap(rv)
}

// structToPropsMap 拍平遍历结构体字段（含匿名嵌入结构体）。
func structToPropsMap(rv reflect.Value) (map[string]any, error) {
	rt := rv.Type()
	out := make(map[string]any, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		if !f.IsExported() {
			continue
		}
		// 匿名嵌入结构体：字段拍平进父映射（mixin 嵌入依赖此行为）。
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			nested, err := structToPropsMap(rv.Field(i))
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
		// ID 通道与向量通道不进属性表。
		if isIDField(f) || isVectorField(f) {
			continue
		}
		val := valueToProps(rv.Field(i))
		if val != nil {
			out[name] = val
		}
	}
	return out, nil
}

// valueToProps 把单个字段值转换为属性可表达的 Go 值。
// 不支持的类型返回 nil（字段被跳过）。
func valueToProps(rv reflect.Value) any {
	switch rv.Kind() {
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64, reflect.String:
		return rv.Interface()
	case reflect.Pointer:
		if rv.IsNil() {
			return nil
		}
		return valueToProps(rv.Elem())
	case reflect.Slice:
		// 向量字段不进属性表。
		if rv.Type().Elem().Kind() == reflect.Float32 {
			return nil
		}
		out := make([]any, 0, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			if item := valueToProps(rv.Index(i)); item != nil {
				out = append(out, item)
			}
		}
		return out
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return nil
		}
		out := make(map[string]any, rv.Len())
		iter := rv.MapRange()
		for iter.Next() {
			if val := valueToProps(iter.Value()); val != nil {
				out[iter.Key().String()] = val
			}
		}
		return out
	default:
		// 结构体、接口、通道、函数等不进入属性表。
		return nil
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

// entityFromProps 把属性映射还原进实体（属性映射 → JSON → 实体）。
func entityFromProps(props map[string]any, dst any) error {
	if len(props) == 0 {
		return nil
	}
	raw, err := json.Marshal(props)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrPayloadConversion, err)
	}
	if err = json.Unmarshal(raw, dst); err != nil {
		return fmt.Errorf("%w: %v", ErrPayloadConversion, err)
	}
	return nil
}

// entityVectors 收集实体的全部向量字段，按声明顺序拼接为单一向量。
func entityVectors(v any) ([]float32, error) {
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
