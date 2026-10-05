package neo4j

import (
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
)

// ─────────────────────────────────────────────────────────────────────────────
// 实体 ↔ 节点属性映射。
//
// 节点在 Cypher 中以 `n` 引用，属性经 $参数 整体传递。本映射器的通道约定：
//   - 生效名：neo4j 标签 name（NAME 键）优先，否则字段名；标签 "-" 跳过；
//   - 匿名嵌入结构体拍平（mixin 的 tenant_id 由此进入属性表）；
//   - element id 通道：字段名 uuid / element_id（大小写不敏感）的 string
//     字段承载服务端生成的节点身份（elementId(n)），该字段不进属性表——
//     身份由服务端分配，不是可写属性；
//   - 可编码值：bool / string / float32|float64（无损提升为 float64）/
//     各宽度有符号整数（窄化为 int64）；无符号整数做范围校验（超 int64
//     跳过）；基本类型的 slice 逐元素同规则转换。
//
// Neo4j 属性系统只收标量与标量数组——映射/结构体/指针等字段不进属性表；
// 读侧为精确类型还原：驱动属性值仅有 int64 / float64 / string / bool /
// []any，仅回填同类型字段——int8/int16/int32/uint* 与 float32 字段可写
// 不可读（驱动无对应值类型，详见模块 README）。
// ─────────────────────────────────────────────────────────────────────────────

// 属性映射标签键与取值。
const (
	propTag     = "neo4j"
	propTagSkip = "-"
	propTagName = "name"
)

// propSpec 实体字段到节点属性 / 身份通道的映射描述。
type propSpec struct {
	path     []int        // 反射字段索引路径（匿名嵌入拍平后指向实际字段）
	name     string       // 生效名（neo4j 标签名或字段名）
	kind     reflect.Kind // 字段类别
	isTenant bool         // tenant_id 属性字段（由 mixin 写入）
	isID     bool         // element id 通道字段（服务端身份，不进属性表）
}

// collectPropSpecs 拍平遍历实体类型，产出字段映射表。
func collectPropSpecs(rt reflect.Type) []propSpec {
	var out []propSpec
	var walk func(rt reflect.Type, prefix []int)
	walk = func(rt reflect.Type, prefix []int) {
		for i := 0; i < rt.NumField(); i++ {
			f := rt.Field(i)
			// 匿名嵌入结构体：拍平（mixin 依赖此行为）。
			if f.Anonymous && f.Type.Kind() == reflect.Struct {
				walk(f.Type, append(append([]int{}, prefix...), i))
				continue
			}
			if !f.IsExported() {
				continue
			}
			if f.Tag.Get(propTag) == propTagSkip {
				continue
			}
			name := f.Name
			if tn, has := parseTagSetting(f.Tag.Get(propTag))[propTagName]; has && tn != "" {
				name = tn
			}
			ln := strings.ToLower(f.Name)
			out = append(out, propSpec{
				path:     append(append([]int{}, prefix...), i),
				name:     name,
				kind:     f.Type.Kind(),
				isTenant: name == "tenant_id",
				isID:     ln == "uuid" || ln == "element_id" || ln == "elementid",
			})
		}
	}
	walk(rt, nil)
	return out
}

// parseTagSetting 解析 k:v,k:v 形式的标签设置。
func parseTagSetting(tag string) map[string]string {
	out := map[string]string{}
	for _, seg := range strings.Split(tag, ",") {
		k, v, ok := strings.Cut(seg, ":")
		if ok {
			out[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return out
}

// ─────────────────────────────────────────────────────────────────────────────
// 编码：实体 → 属性表。
// ─────────────────────────────────────────────────────────────────────────────

// structToProperties 把实体拍平为节点属性表。
// 不可编码字段（映射/结构体、指针、非基本元素、超界无符号、元素表列中
// 的失效元素）整体跳过；element id 通道字段不进属性表。
func structToProperties[ENTITY any](ent *ENTITY) map[string]any {
	props := map[string]any{}
	if ent == nil {
		return props
	}
	var e ENTITY
	specs := collectPropSpecs(reflect.TypeOf(&e).Elem())
	rv := reflect.ValueOf(ent).Elem()
	for _, spec := range specs {
		if spec.isID {
			continue
		}
		fv := rv.FieldByIndex(spec.path)
		if spec.kind != reflect.Slice {
			if v, ok := scalarToPropertyValue(fv); ok {
				props[spec.name] = v
			}
			continue
		}
		if v, ok := sliceToPropertyValue(fv); ok {
			props[spec.name] = v
		}
	}
	return props
}

// scalarToPropertyValue 基本类型字段值 → 驱动属性值。
// 整数窄化为 int64（无符号做范围校验防回绕）；float32 无损提升为
// float64；其余（含平台宽度 int/uint/uintptr）不可编码。
func scalarToPropertyValue(fv reflect.Value) (any, bool) {
	switch fv.Kind() {
	case reflect.Bool:
		return fv.Bool(), true
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return fv.Int(), true
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if fv.Uint() > math.MaxInt64 {
			return nil, false
		}
		return int64(fv.Uint()), true
	case reflect.Float32, reflect.Float64:
		return fv.Float(), true
	case reflect.String:
		return fv.String(), true
	}
	return nil, false
}

// sliceToPropertyValue 基本类型切片 → 属性列表（任一元素不可编码则整体跳过）。
func sliceToPropertyValue(fv reflect.Value) (any, bool) {
	n := fv.Len()
	out := make([]any, n)
	for i := 0; i < n; i++ {
		v, ok := scalarToPropertyValue(fv.Index(i))
		if !ok {
			return nil, false
		}
		out[i] = v
	}
	return out, true
}

// ─────────────────────────────────────────────────────────────────────────────
// 解码：节点 → 实体。
// ─────────────────────────────────────────────────────────────────────────────

// propertiesToEntity 把节点的属性与 element id 还原进实体。
// 属性缺失或类型不符的字段保持零值（跳过而非报错，容忍 schema 演进）；
// element id 仅回填 string 类型的身份通道字段。
func propertiesToEntity[ENTITY any](node neo4j.Node, dst *ENTITY) error {
	if dst == nil {
		return fmt.Errorf("%w: nil entity destination", ErrPayloadConversion)
	}
	var e ENTITY
	specs := collectPropSpecs(reflect.TypeOf(&e).Elem())
	rv := reflect.ValueOf(dst).Elem()
	fillElementID(specs, rv, node.ElementId)
	for _, spec := range specs {
		if spec.isID {
			continue
		}
		v, ok := node.Props[spec.name]
		if !ok {
			continue
		}
		setFieldValue(rv.FieldByIndex(spec.path), v)
	}
	return nil
}

// setElementID 把 element id 写入实体的身份通道字段（仅 string 类型字段）。
func setElementID[ENTITY any](ent *ENTITY, id string) {
	if ent == nil {
		return
	}
	var e ENTITY
	specs := collectPropSpecs(reflect.TypeOf(&e).Elem())
	fillElementID(specs, reflect.ValueOf(ent).Elem(), id)
}

// fillElementID 按 map 表把 element id 填入身份通道字段。
func fillElementID(specs []propSpec, rv reflect.Value, id string) {
	if !rv.IsValid() {
		return
	}
	for _, spec := range specs {
		if !spec.isID {
			continue
		}
		if fv := rv.FieldByIndex(spec.path); fv.CanSet() && fv.Kind() == reflect.String {
			fv.SetString(id)
		}
	}
}

// setFieldValue 按属性值类型填充目标字段（精确类型一致；属性值类型与
// 字段类型不符时跳过）。列表按元素级精确类型还原。
func setFieldValue(fv reflect.Value, v any) {
	if !fv.CanSet() {
		return
	}
	if vals, ok := v.([]any); ok && fv.Kind() == reflect.Slice {
		setSliceValue(fv, vals)
		return
	}
	setScalarValue(fv, v)
}

// setScalarValue 标量属性值 → 字段（精确类型一致才回填）。
func setScalarValue(fv reflect.Value, v any) bool {
	switch val := v.(type) {
	case bool:
		if fv.Kind() == reflect.Bool {
			fv.SetBool(val)
			return true
		}
	case int64:
		if fv.Kind() == reflect.Int64 {
			fv.SetInt(val)
			return true
		}
	case float64:
		if fv.Kind() == reflect.Float64 {
			fv.SetFloat(val)
			return true
		}
	case string:
		if fv.Kind() == reflect.String {
			fv.SetString(val)
			return true
		}
	}
	return false
}

// setSliceValue 列表属性值 → 切片字段（元素级精确类型；任一元素不符整体跳过）。
func setSliceValue(fv reflect.Value, vals []any) {
	elem := fv.Type().Elem()
	out := reflect.MakeSlice(fv.Type(), 0, len(vals))
	for _, v := range vals {
		ev := reflect.New(elem).Elem()
		if !setScalarValue(ev, v) {
			return
		}
		out = reflect.Append(out, ev)
	}
	fv.Set(out)
}
