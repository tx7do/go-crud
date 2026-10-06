package cassandra

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

// ─────────────────────────────────────────────────────────────────────────────
// 实体 ↔ CQL 行映射。
//
// 列名经 cql 标签控制（NAME 键优先于字段名，"-" 跳过；列名一律经标识符
// 白名单校验——列名会拼进语句文本，杜绝注入面）。通道约定：
//   - 主键通道：字段名 id/uuid（大小写不敏感）的字段承载行主键（简单主键，
//     单一分区键），不进普通列清单；
//   - 普通列：bool / 各宽度有符号整数 / float32|float64 / string /
//     time.Time / 基本类型切片 / string 键基本类型映射；无符号整数与
//     结构体/指针不编码（有符号整数溢出报错而非截断）；
//   - 解码为精确类型回填：gocql 按列类型产出 Go 值——bigint→int64、
//     int→int（注意不是 int32）、smallint→int16、tinyint→int8、
//     float→float32、double→float64、text/varchar→string、boolean→bool、
//     timestamp→time.Time、uuid→gocql.UUID（特判回填 string 字段）；
//     类型不符的字段保持零值（容忍 schema 演进）。
// ─────────────────────────────────────────────────────────────────────────────

// 列映射标签键与取值。
const (
	colTag     = "cql"
	colTagSkip = "-"
	colTagName = "name"
)

// identifierPattern CQL 标识符白名单（键空间/表/列名共用；名称会拼进
// 语句文本，一律先校验）。
var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// gocqlUUIDType uuid 列的驱动原生类型（解码特判用）。
var gocqlUUIDType = reflect.TypeOf(gocql.UUID{})

// columnSpec 实体字段到 CQL 列 / 主键通道的映射描述。
type columnSpec struct {
	path     []int        // 反射字段索引路径（匿名嵌入拍平后指向实际字段）
	name     string       // 生效名（cql 标签名或字段名）
	kind     reflect.Kind // 字段类别
	isPK     bool         // 主键通道字段（id/uuid，大小写不敏感）
	isTenant bool         // tenant_id 列（由 mixin 写入）
}

// collectColumnSpecs 拍平遍历实体类型，产出列映射表。
func collectColumnSpecs(rt reflect.Type) ([]columnSpec, error) {
	var out []columnSpec
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
			if f.Tag.Get(colTag) == colTagSkip {
				continue
			}
			name := f.Name
			if tn, has := parseTagSetting(f.Tag.Get(colTag))[colTagName]; has && tn != "" {
				name = tn
			}
			if !identifierPattern.MatchString(name) {
				return fmt.Errorf("%w: illegal column identifier %q", ErrInvalidRequest, name)
			}
			ln := strings.ToLower(f.Name)
			out = append(out, columnSpec{
				path:     append(append([]int{}, prefix...), i),
				name:     name,
				kind:     f.Type.Kind(),
				isPK:     ln == "id" || ln == "uuid",
				isTenant: name == "tenant_id",
			})
		}
		return nil
	}
	if err := walk(rt, nil); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: entity has no column fields", ErrInvalidRequest)
	}
	pks := 0
	for _, spec := range out {
		if spec.isPK {
			pks++
		}
	}
	if pks == 0 {
		return nil, fmt.Errorf("%w: entity has no id/uuid primary key field", ErrInvalidPointID)
	}
	return out, nil
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
// 编码：实体 → 列名与绑定参数。
// ─────────────────────────────────────────────────────────────────────────────

// columnsAndArgsFromEntity 把实体编码为普通列名与绑定参数（主键通道除外）。
// gocql 按服务端元数据的列类型绑定参数（各宽度整数均可接受并做范围校验），
// 故整数按字段自然类型传递；无符号/结构体/指针等不可编码类型报错。
func columnsAndArgsFromEntity(ent any) ([]string, []any, error) {
	specs, err := collectSpecsOf(ent)
	if err != nil {
		return nil, nil, err
	}
	rv := reflect.ValueOf(ent)
	for rv.Kind() == reflect.Pointer {
		rv = rv.Elem()
	}

	cols := make([]string, 0, len(specs))
	args := make([]any, 0, len(specs))
	for _, spec := range specs {
		if spec.isPK {
			continue
		}
		v, err := valueToArg(rv.FieldByIndex(spec.path))
		if err != nil {
			return nil, nil, err
		}
		cols = append(cols, spec.name)
		args = append(args, v)
	}
	if len(cols) == 0 {
		return nil, nil, fmt.Errorf("%w: entity has no ordinary columns", ErrInvalidRequest)
	}
	return cols, args, nil
}

// valueToArg 单个字段值 → 绑定参数。不可编码返回错误（响亮失败优于静默丢列）。
func valueToArg(fv reflect.Value) (any, error) {
	switch fv.Kind() {
	case reflect.Bool,
		reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Float32, reflect.Float64, reflect.String:
		return fv.Interface(), nil
	case reflect.Int:
		return int64(fv.Int()), nil
	case reflect.Struct:
		if fv.Type() == reflect.TypeOf(time.Time{}) {
			return fv.Interface(), nil
		}
		return nil, fmt.Errorf("%w: struct field %s is not encodable", ErrPayloadConversion, fv.Type())
	case reflect.Slice:
		if fv.Type().Elem().Kind() == reflect.Uint8 {
			return fv.Interface(), nil // []byte → blob
		}
		out := make([]any, fv.Len())
		for i := 0; i < fv.Len(); i++ {
			v, err := valueToArg(fv.Index(i))
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return out, nil
	case reflect.Map:
		if fv.Type().Key().Kind() != reflect.String {
			return nil, fmt.Errorf("%w: map key %s is not string", ErrPayloadConversion, fv.Type().Key())
		}
		out := make(map[string]any, fv.Len())
		iter := fv.MapRange()
		for iter.Next() {
			v, err := valueToArg(iter.Value())
			if err != nil {
				return nil, err
			}
			out[iter.Key().String()] = v
		}
		return out, nil
	default:
		return nil, fmt.Errorf("%w: field kind %s is not encodable", ErrPayloadConversion, fv.Kind())
	}
}

// pkValueFromEntity 读取主键通道字段的绑定值（缺失或空返回 nil）。
func pkValueFromEntity(ent any) any {
	specs, err := collectSpecsOf(ent)
	if err != nil {
		return nil
	}
	rv := reflect.ValueOf(ent)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil
		}
		rv = rv.Elem()
	}
	for _, spec := range specs {
		if !spec.isPK {
			continue
		}
		fv := rv.FieldByIndex(spec.path)
		if fv.Kind() == reflect.String && fv.String() == "" {
			return nil
		}
		if (fv.Kind() == reflect.Int64) && fv.Int() == 0 {
			return nil
		}
		return fv.Interface()
	}
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// 解码：行 → 实体。
// ─────────────────────────────────────────────────────────────────────────────

// entityFromRow 把一行「列名 → 值」还原进实体。
// 列缺失或类型不符的字段保持零值（跳过而非报错，容忍 schema 演进）；
// gocql 的 int 列产出 Go int（非 int32）、uuid 列产出 gocql.UUID
// （特判回填 string 字段）。
func entityFromRow(row map[string]any, dst any) error {
	rv := reflect.ValueOf(dst)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return fmt.Errorf("%w: nil entity destination", ErrPayloadConversion)
		}
		rv = rv.Elem()
	}
	specs, err := collectSpecsOf(dst)
	if err != nil {
		return err
	}
	// CQL 标识符大小写不敏感：服务端把未加引号的标识符折叠为小写，
	// 行键与 spec 名一律按折叠小写匹配。
	lookup := make(map[string]any, len(row))
	for k, v := range row {
		lookup[strings.ToLower(k)] = v
	}
	for _, spec := range specs {
		v, ok := lookup[strings.ToLower(spec.name)]
		if !ok {
			continue
		}
		setFieldValue(rv.FieldByIndex(spec.path), v)
	}
	return nil
}

// rowColumnValue 按折叠小写从行映射取列值（CQL 标识符大小写不敏感）。
func rowColumnValue(row map[string]any, name string) (any, bool) {
	v, ok := row[name]
	if ok {
		return v, true
	}
	v, ok = row[strings.ToLower(name)]
	return v, ok
}

// setFieldValue 按行值类型填充目标字段（精确类型一致；类型不符跳过）。
func setFieldValue(fv reflect.Value, v any) {
	if !fv.CanSet() {
		return
	}
	switch val := v.(type) {
	case bool:
		if fv.Kind() == reflect.Bool {
			fv.SetBool(val)
		}
	case int:
		if fv.Kind() == reflect.Int {
			fv.SetInt(int64(val))
		}
	case int8:
		if fv.Kind() == reflect.Int8 {
			fv.SetInt(int64(val))
		}
	case int16:
		if fv.Kind() == reflect.Int16 {
			fv.SetInt(int64(val))
		}
	case int32:
		if fv.Kind() == reflect.Int32 {
			fv.SetInt(int64(val))
		}
	case int64:
		if fv.Kind() == reflect.Int64 {
			fv.SetInt(val)
		}
	case float32:
		if fv.Kind() == reflect.Float32 {
			fv.SetFloat(float64(val))
		}
	case float64:
		if fv.Kind() == reflect.Float64 {
			fv.SetFloat(val)
		}
	case string:
		if fv.Kind() == reflect.String {
			fv.SetString(val)
		}
	case time.Time:
		if fv.Type() == reflect.TypeOf(time.Time{}) {
			fv.Set(reflect.ValueOf(val))
		}
	case gocql.UUID:
		// uuid 列的驱动原生类型：回填同类型字段或 string 字段（规范格式）。
		if fv.Type() == gocqlUUIDType {
			fv.Set(reflect.ValueOf(val))
		} else if fv.Kind() == reflect.String {
			fv.SetString(val.String())
		}
	default:
		// 列表/映射列：gocql 按元素类型产出原生切片/映射（list<text> →
		// []string），反射逐元素精确回填（同时兼容替身给出的 []any /
		// map[string]any 形态）。
		if rv := reflect.ValueOf(v); rv.Kind() == reflect.Slice && fv.Kind() == reflect.Slice {
			setSliceFrom(fv, rv)
			return
		}
		if rv := reflect.ValueOf(v); rv.Kind() == reflect.Map && fv.Kind() == reflect.Map {
			setMapFrom(fv, rv)
		}
	}
}

// setSliceFrom 列表行值 → 切片字段（元素级精确类型；任一元素不符整体跳过）。
func setSliceFrom(fv reflect.Value, src reflect.Value) {
	out := reflect.MakeSlice(fv.Type(), 0, src.Len())
	for i := 0; i < src.Len(); i++ {
		ev := reflect.New(fv.Type().Elem()).Elem()
		if !setScalarValue(ev, src.Index(i).Interface()) {
			return
		}
		out = reflect.Append(out, ev)
	}
	fv.Set(out)
}

// setMapFrom 映射行值 → 映射字段（string 键，元素级精确类型）。
func setMapFrom(fv reflect.Value, src reflect.Value) {
	if src.Type().Key().Kind() != reflect.String || fv.Type().Key().Kind() != reflect.String {
		return
	}
	out := reflect.MakeMapWithSize(fv.Type(), src.Len())
	iter := src.MapRange()
	for iter.Next() {
		ev := reflect.New(fv.Type().Elem()).Elem()
		if !setScalarValue(ev, iter.Value().Interface()) {
			return
		}
		out.SetMapIndex(iter.Key(), ev)
	}
	fv.Set(out)
}

// setScalarValue 标量行值 → 字段（精确类型一致才回填），供切片/映射复用。
func setScalarValue(fv reflect.Value, v any) bool {
	switch val := v.(type) {
	case bool:
		if fv.Kind() == reflect.Bool {
			fv.SetBool(val)
			return true
		}
	case int:
		if fv.Kind() == reflect.Int {
			fv.SetInt(int64(val))
			return true
		}
	case int8:
		if fv.Kind() == reflect.Int8 {
			fv.SetInt(int64(val))
			return true
		}
	case int16:
		if fv.Kind() == reflect.Int16 {
			fv.SetInt(int64(val))
			return true
		}
	case int32:
		if fv.Kind() == reflect.Int32 {
			fv.SetInt(int64(val))
			return true
		}
	case int64:
		if fv.Kind() == reflect.Int64 {
			fv.SetInt(val)
			return true
		}
	case float32:
		if fv.Kind() == reflect.Float32 {
			fv.SetFloat(float64(val))
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
	case time.Time:
		if fv.Type() == reflect.TypeOf(time.Time{}) {
			fv.Set(reflect.ValueOf(val))
			return true
		}
	}
	return false
}

// collectSpecsOf 实体/目标的映射表查询入口。
func collectSpecsOf(ent any) ([]columnSpec, error) {
	if ent == nil {
		return nil, fmt.Errorf("%w: nil entity", ErrInvalidRequest)
	}
	rt := reflect.TypeOf(ent)
	for rt.Kind() == reflect.Pointer {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%w: entity must be struct, got %s", ErrPayloadConversion, rt.Kind())
	}
	return collectColumnSpecs(rt)
}
