package influxdb

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
	"github.com/tx7do/go-utils/stringcase"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func BuildQuery(
	table string,
	filters map[string]any,
	operators map[string]string,
	fields []string,
) (string, []any) {
	var queryBuilder strings.Builder
	args := make([]any, 0)

	// 构建 SELECT 语句
	queryBuilder.WriteString("SELECT ")
	if len(fields) > 0 {
		queryBuilder.WriteString(strings.Join(fields, ", "))
	} else {
		queryBuilder.WriteString("*")
	}
	queryBuilder.WriteString(fmt.Sprintf(" FROM %s", table))

	// 构建 WHERE 条件
	if len(filters) > 0 {
		queryBuilder.WriteString(" WHERE ")
		var conditions []string
		var operator string
		for key, value := range filters {
			operator = "=" // 默认操作符
			if op, exists := operators[key]; exists {
				operator = op
			}
			conditions = append(conditions, fmt.Sprintf("%s %s ?", key, operator))
			args = append(args, value)
		}
		queryBuilder.WriteString(strings.Join(conditions, " AND "))
	}

	return queryBuilder.String(), args
}

func GetPointTag(point *influxdb3.Point, name string) *string {
	if point == nil {
		return nil
	}
	tagValue, ok := point.GetTag(name)
	if !ok || tagValue == "" {
		return nil
	}
	return &tagValue
}

func GetBoolPointTag(point *influxdb3.Point, name string) *bool {
	if point == nil {
		return nil
	}
	tagValue, ok := point.GetTag(name)
	if !ok || tagValue == "" {
		return nil
	}

	value := tagValue == "true"
	return &value
}

func GetUint32PointTag(point *influxdb3.Point, name string) *uint32 {
	if point == nil {
		return nil
	}
	tagValue, ok := point.GetTag(name)
	if !ok || tagValue == "" {
		return nil
	}

	value, err := strconv.ParseUint(tagValue, 10, 64)
	if err != nil {
		return nil
	}
	value32 := uint32(value)
	return &value32
}

func GetUint64PointTag(point *influxdb3.Point, name string) *uint64 {
	if point == nil {
		return nil
	}
	tagValue, ok := point.GetTag(name)
	if !ok || tagValue == "" {
		return nil
	}

	value, err := strconv.ParseUint(tagValue, 10, 64)
	if err != nil {
		return nil
	}
	return &value
}

func GetEnumPointTag[T ~int32](point *influxdb3.Point, name string, valueMap map[string]int32) *T {
	if point == nil {
		return nil
	}
	tagValue, ok := point.GetTag(name)
	if !ok || tagValue == "" {
		return nil
	}
	enumValue, exists := valueMap[tagValue]
	if !exists {
		return nil
	}

	enumType := T(enumValue)
	return &enumType
}

func GetTimestampField(point *influxdb3.Point, name string) *timestamppb.Timestamp {
	if point == nil {
		return nil
	}

	value := point.GetField(name)
	if value == nil {
		return nil
	}
	if timestamp, ok := value.(*timestamppb.Timestamp); ok {
		return timestamp
	}
	if timeValue, ok := value.(time.Time); ok {
		return timestamppb.New(timeValue)
	}
	return nil
}

func GetUint32Field(point *influxdb3.Point, name string) *uint32 {
	if point == nil {
		return nil
	}

	value := point.GetUIntegerField(name)
	if value == nil {
		return nil
	}
	uint32Value := uint32(*value)
	if uint32Value == 0 {
		return nil
	}
	return &uint32Value
}

func BoolToString(value *bool) string {
	if value == nil {
		return "false"
	}
	if *value {
		return "true"
	}
	return "false"
}

func Uint64ToString(value *uint64) string {
	if value == nil {
		return "0"
	}
	return fmt.Sprintf("%d", *value)
}

func BuildQueryWithParams(
	table string,
	filters map[string]any,
	operators map[string]string,
	fields []string,
) string {
	var queryBuilder strings.Builder

	// 构建 SELECT 语句
	queryBuilder.WriteString("SELECT ")
	if len(fields) > 0 {
		queryBuilder.WriteString(strings.Join(fields, ", "))
	} else {
		queryBuilder.WriteString("*")
	}
	queryBuilder.WriteString(fmt.Sprintf(" FROM %s", table))

	// 构建 WHERE 条件
	if len(filters) > 0 {
		var operator string
		queryBuilder.WriteString(" WHERE ")
		var conditions []string
		for key, value := range filters {
			operator = "=" // 默认操作符
			if op, exists := operators[key]; exists {
				operator = op
			}
			conditions = append(conditions, fmt.Sprintf("%s %s %v", key, operator, value))
		}
		queryBuilder.WriteString(strings.Join(conditions, " AND "))
	}

	return queryBuilder.String()
}

// ConvertAnyToPointsSafe 将 []any 逐元素断言为 []*influxdb3.Point
func ConvertAnyToPointsSafe(pts []any) ([]*influxdb3.Point, error) {
	points := make([]*influxdb3.Point, 0, len(pts))
	for i, p := range pts {
		pt, ok := p.(*influxdb3.Point)
		if !ok {
			return nil, errors.New("unsupported element type at index " + strconv.Itoa(i))
		}
		points = append(points, pt)
	}
	return points, nil
}

// numericToInt64 尝试把常见数值类型转换为 int64
func numericToInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case int:
		return int64(t), true
	case uint64:
		return int64(t), true
	case float64:
		return int64(t), true
	case float32:
		return int64(t), true
	case uint32:
		return int64(t), true
	case int32:
		return int64(t), true
	case uint:
		return int64(t), true
	default:
		return 0, false
	}
}

// StructToPoint 通用转换函数：将带 influx tag 的 struct 转为 influxdb3.Point
// 参数：s 带 tag 的 struct 实例（不能是指针）
func StructToPoint(s any) (*influxdb3.Point, error) {
	val := reflect.ValueOf(s)
	// 若传入指针，解引用（支持传入 &struct 或 struct）
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	// 确保传入的是 struct
	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("输入必须是 struct 或 struct 指针")
	}

	typ := val.Type()
	var (
		measurement string
		tags        = make(map[string]string)
		fields      = make(map[string]any)
		timestamp   time.Time
	)

	// 遍历 struct 字段，按 tag 分类
	for i := 0; i < val.NumField(); i++ {
		fieldVal := val.Field(i)
		fieldTyp := typ.Field(i)
		// 获取 influx tag（如 `influx:"tag"`）
		influxTag := fieldTyp.Tag.Get("influx")

		switch influxTag {
		case "measurement":
			// 从 struct tag 读取 measurement 名称（字段必须是 string 类型）
			if fieldVal.Kind() == reflect.String {
				measurement = fieldVal.String()
			}
		case "tag":
			// 标记为 tag：必须是 string 类型
			if fieldVal.Kind() != reflect.String {
				return nil, fmt.Errorf("字段 %s 标记为 tag，但类型不是 string", fieldTyp.Name)
			}
			tagKey := fieldTyp.Name                 // 或从 json tag 读取：strings.ToLower(fieldTyp.Tag.Get("json"))
			tagKey = stringcase.ToSnakeCase(tagKey) // 可选：转为下划线命名（如 DeviceID → device_id）
			tags[tagKey] = fieldVal.String()
		case "field":
			// 标记为 field：支持 int64/float64/bool/string/time.Time（time 会转为 UnixNano）
			fieldKey := stringcase.ToSnakeCase(fieldTyp.Name)
			switch fieldVal.Kind() {
			case reflect.Int, reflect.Int64:
				fields[fieldKey] = fieldVal.Int()
			case reflect.Float32, reflect.Float64:
				fields[fieldKey] = fieldVal.Float()
			case reflect.Bool:
				fields[fieldKey] = fieldVal.Bool()
			case reflect.String:
				fields[fieldKey] = fieldVal.String()
			case reflect.Struct:
				// 若字段是 time.Time，转为 int64（纳秒级时间戳）
				if t, ok := fieldVal.Interface().(time.Time); ok {
					fields[fieldKey] = t.UnixNano()
				} else {
					return nil, fmt.Errorf("字段 %s 标记为 field，但不支持 struct 类型", fieldTyp.Name)
				}
			default:
				return nil, fmt.Errorf("字段 %s 标记为 field，但不支持类型 %s", fieldTyp.Name, fieldVal.Kind())
			}
		case "time":
			// 标记为 time：必须是 time.Time 类型
			if t, ok := fieldVal.Interface().(time.Time); ok {
				timestamp = t
			} else {
				return nil, fmt.Errorf("字段 %s 标记为 time，但类型不是 time.Time", fieldTyp.Name)
			}
		}
	}

	// 校验必填项：measurement 和 timestamp 不能为空
	if measurement == "" {
		return nil, fmt.Errorf("struct 未指定 measurement（需添加 influx:\"measurement\" tag）")
	}
	if timestamp.IsZero() {
		// 若未指定时间，默认用当前 UTC 时间
		timestamp = time.Now().UTC()
	}

	// 创建 Point
	return influxdb3.NewPoint(measurement, tags, fields, timestamp), nil
}

// ProtoMessageToPoint 将 protobuf message 转为 influxdb3.Point。
// 参数 overrides 可选：map[key]=role，key 为 protobuf 字段的 JSON 名称（例如 "deviceId"），role 为 "measurement"/"tag"/"field"/"time"。
// 约定识别规则（在无 overrides 时）：
//   - 字段名为 "measurement" -> measurement
//   - 名称以 "_tag" 或以 "tag_" 前缀 -> tag
//   - 字段类型为 google.protobuf.Timestamp 或名为 "time"/"timestamp" -> time
//   - 其它标量 -> field
//
// example.proto:
//
// syntax = "proto3";
// import "google/protobuf/timestamp.proto";
//
//	message SensorProto {
//			string measurement = 1;           // 当作为 measurement 使用
//			string device_id = 2;             // 作为 tag
//			string location = 3;              // 作为 tag
//			double temperature = 4;           // field
//			int64 battery = 5;                // field
//			google.protobuf.Timestamp ts = 6; // time
//	}
//
// Go 使用示例（将 pb 替换为实际生成包路径，例如 `github.com/you/project/pb`）
//
// package influxdb_test
//
// import (
//
//	"fmt"
//	"time"
//
//	"github.com/InfluxCommunity/influxdb3-go/v2/influxdb3"
//	"google.golang.org/protobuf/types/known/timestamppb"
//
//	// 替换为你的 protobuf 生成包
//	pb "path/to/your/generated/pb"
//
//	// 假设 ProtoMessageToPoint 位于本模块的 influxdb 包
//	"your/module/path/influxdb"
//
// )
//
//	func ExampleProtoMessageToPoint_basic() {
//		// 构造 protobuf message
//		msg := &pb.SensorProto{
//			Measurement: "sensors",
//			DeviceId:    "dev-1",
//			Location:    "room1",
//			Temperature: 23.5,
//			Battery:     95,
//			Ts:          timestamppb.New(time.Now().UTC()),
//		}
//
//		// 不传 overrides：按约定自动分类（字段名 measurement -> measurement，ts -> time，带 tag hint 的字段 -> tag）
//		pt, err := influxdb.ProtoMessageToPoint(msg, nil)
//		if err != nil {
//			fmt.Println("err:", err)
//			return
//		}
//		// 打印示例：measurement/tags/fields/time
//		fmt.Println("measurement:", pt.GetMeasurement())
//		fmt.Println("tag device_id:", pt.GetTag("device_id"))
//		fmt.Println("field temperature:", pt.GetField("temperature"))
//	}
//
//	func ExampleProtoMessageToPoint_withOverrides() {
//		msg := &pb.SensorProto{
//			// 假设 proto 中没有 measurement 字段或你希望使用其它字段作为 measurement
//			DeviceId:    "dev-1",
//			Location:    "room1",
//			Temperature: 23.5,
//			Battery:     95,
//			Ts:          timestamppb.New(time.Now().UTC()),
//		}
//
//		// overrides 的 key 使用 protobuf 字段的 JSON 名称（例如 proto 字段 device_id 的 json 名称通常为 "deviceId"）
//		overrides := map[string]string{
//			"deviceId":    "measurement", // 把 deviceId 当作 measurement
//			"location":    "tag",         // 强制 location 为 tag
//			"battery":     "field",
//			"ts":          "time",
//			"temperature": "field",
//		}
//
//		pt, err := influxdb.ProtoMessageToPoint(msg, overrides)
//		if err != nil {
//			fmt.Println("err:", err)
//			return
//		}
//		fmt.Println("measurement:", pt.GetMeasurement())
//		fmt.Println("tags:", pt.GetTags())
//		fmt.Println("fields:", pt.GetFields())
//	}
func ProtoMessageToPoint(msg proto.Message, overrides map[string]string) (*influxdb3.Point, error) {
	if msg == nil {
		return nil, fmt.Errorf("msg is nil")
	}

	m := msg.ProtoReflect()
	var (
		measurement string
		tags        = make(map[string]string)
		fields      = make(map[string]any)
		timestamp   time.Time
	)

	// iterate fields present in the message (only set fields)
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		// skip repeated / map / unknown kinds for simplicity
		if fd.IsList() || fd.IsMap() {
			return true
		}

		jsonName := fd.JSONName()                 // protobuf JSON name, e.g. deviceId
		snake := stringcase.ToSnakeCase(jsonName) // convert to snake_case for influx keys

		// decide role: overrides -> heuristics
		role := ""
		if overrides != nil {
			if r, ok := overrides[jsonName]; ok {
				role = r
			}
		}
		if role == "" {
			// heuristics
			if jsonName == "measurement" {
				role = "measurement"
			} else if hasTagHint(jsonName) || hasTagHint(string(fd.Name())) {
				role = "tag"
			} else if isTimestampField(fd, jsonName) {
				role = "time"
			} else {
				role = "field"
			}
		}

		// convert protoreflect.Value -> go native
		var gv any
		switch fd.Kind() {
		case protoreflect.BoolKind:
			gv = v.Bool()
		case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
			gv = v.Int()
		case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			gv = v.Int()
		case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
			gv = int64(v.Uint())
		case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
			gv = v.Uint()
		case protoreflect.FloatKind, protoreflect.DoubleKind:
			gv = v.Float()
		case protoreflect.StringKind:
			gv = v.String()
		case protoreflect.EnumKind:
			gv = int32(v.Enum())
		case protoreflect.MessageKind:
			// support google.protobuf.Timestamp
			if string(fd.Message().FullName()) == "google.protobuf.Timestamp" {
				if ts, ok := v.Message().Interface().(*timestamppb.Timestamp); ok && ts != nil {
					gv = ts.AsTime()
				}
			} else {
				// skip nested messages by default
				return true
			}
		default:
			// unsupported kind, skip
			return true
		}

		// assign according to role
		switch role {
		case "measurement":
			if s, ok := gv.(string); ok {
				measurement = s
			}
		case "tag":
			tags[snake] = fmt.Sprintf("%v", gv)
		case "time":
			switch tv := gv.(type) {
			case time.Time:
				timestamp = tv
			case int64:
				// assume unix nanos if very large, else seconds
				if tv > 1e12 {
					timestamp = time.Unix(0, tv)
				} else {
					timestamp = time.Unix(tv, 0)
				}
			default:
				// try fmt -> ignore on failure
			}
		case "field":
			// keep numeric types as-is (int64/uint64/float64/bool/string/time.Time)
			fields[snake] = gv
		}

		return true
	})

	if measurement == "" {
		return nil, fmt.Errorf("measurement not found; set a field named 'measurement' or provide overrides")
	}
	if timestamp.IsZero() {
		timestamp = time.Now().UTC()
	}
	return influxdb3.NewPoint(measurement, tags, fields, timestamp), nil
}

func hasTagHint(name string) bool {
	// common heuristics: ends with "_tag" or starts with "tag_"
	if name == "" {
		return false
	}
	s := name
	return len(s) >= 4 && (s[len(s)-4:] == "_tag" || (len(s) >= 4 && s[:4] == "tag_"))
}

func isTimestampField(fd protoreflect.FieldDescriptor, jsonName string) bool {
	if jsonName == "time" || jsonName == "timestamp" || jsonName == "ts" {
		return true
	}
	if fd.Kind() == protoreflect.MessageKind && string(fd.Message().FullName()) == "google.protobuf.Timestamp" {
		return true
	}
	return false
}
