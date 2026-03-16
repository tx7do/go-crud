package clickhouse

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	timeFormat = "2006-01-02 15:04:05.000000000"
)

func structToValueArray(input any) []any {
	// 检查是否是指针类型，如果是则解引用
	val := reflect.ValueOf(input)
	if !val.IsValid() {
		return nil
	}
	if val.Kind() == reflect.Ptr {
		if val.IsNil() {
			return nil
		}
		val = val.Elem()
	}

	// 确保输入是结构体
	if val.Kind() != reflect.Struct {
		return nil
	}

	var values []any
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		// 避免访问不可导出的字段导致 panic
		if !field.CanInterface() {
			values = append(values, nil)
			continue
		}

		value := field.Interface()

		// Handle common SQL nullable types (both pointer and value forms)
		switch v := value.(type) {
		case sql.NullString:
			if v.Valid {
				values = append(values, v.String)
			} else {
				values = append(values, nil)
			}
			continue
		case *sql.NullString:
			if v != nil && v.Valid {
				values = append(values, v.String)
			} else {
				values = append(values, nil)
			}
			continue
		case sql.NullInt64:
			if v.Valid {
				values = append(values, v.Int64)
			} else {
				values = append(values, nil)
			}
			continue
		case *sql.NullInt64:
			if v != nil && v.Valid {
				values = append(values, v.Int64)
			} else {
				values = append(values, nil)
			}
			continue
		case sql.NullFloat64:
			if v.Valid {
				values = append(values, v.Float64)
			} else {
				values = append(values, nil)
			}
			continue
		case *sql.NullFloat64:
			if v != nil && v.Valid {
				values = append(values, v.Float64)
			} else {
				values = append(values, nil)
			}
			continue
		case sql.NullBool:
			if v.Valid {
				values = append(values, v.Bool)
			} else {
				values = append(values, nil)
			}
			continue
		case *sql.NullBool:
			if v != nil && v.Valid {
				values = append(values, v.Bool)
			} else {
				values = append(values, nil)
			}
			continue
		case sql.NullTime:
			if v.Valid {
				values = append(values, v.Time.Format(timeFormat))
			} else {
				values = append(values, nil)
			}
			continue
		case *sql.NullTime:
			if v != nil && v.Valid {
				values = append(values, v.Time.Format(timeFormat))
			} else {
				values = append(values, nil)
			}
			continue
		}

		// protobuf timestamp/duration handling
		switch v := value.(type) {
		case timestamppb.Timestamp:
			if v.IsValid() {
				values = append(values, v.AsTime().Format(timeFormat))
			} else {
				values = append(values, nil)
			}
			continue
		case *timestamppb.Timestamp:
			if v != nil && v.IsValid() {
				values = append(values, v.AsTime().Format(timeFormat))
			} else {
				values = append(values, nil)
			}
			continue
		case durationpb.Duration:
			if v.AsDuration() != 0 {
				values = append(values, v.AsDuration().String())
			} else {
				values = append(values, nil)
			}
			continue
		case *durationpb.Duration:
			if v != nil && v.AsDuration() != 0 {
				values = append(values, v.AsDuration().String())
			} else {
				values = append(values, nil)
			}
			continue
		}

		// Use reflect for generic handling (pointers, slices, maps, time, basic types)
		rv := reflect.ValueOf(value)

		// If original value is a pointer, handle specially.
		if rv.Kind() == reflect.Ptr {
			if rv.IsNil() {
				values = append(values, nil)
				continue
			}

			// Pointer to time.Time should be formatted as string
			if rv.Elem().Type() == reflect.TypeOf(time.Time{}) {
				t := rv.Elem().Interface().(time.Time)
				if !t.IsZero() {
					values = append(values, t.Format(timeFormat))
				} else {
					values = append(values, nil)
				}
				continue
			}

			// For pointer-to-basic-types (string, int, float, bool), preserve the pointer
			switch rv.Elem().Kind() {
			case reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
				reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
				reflect.Float32, reflect.Float64, reflect.Bool:
				// keep the pointer as-is (so callers can type-assert to *T)
				values = append(values, rv.Interface())
				continue
			case reflect.Slice, reflect.Array:
				// pointer to slice/array: treat like non-pointer slice/array below
				rv = rv.Elem()
			default:
				// fallthrough to generic handling with the dereferenced value
				rv = rv.Elem()
			}
		}

		switch rv.Kind() {
		case reflect.Struct:
			// time.Time handling (non-pointer)
			if t, ok := rv.Interface().(time.Time); ok {
				if !t.IsZero() {
					values = append(values, t.Format(timeFormat))
				} else {
					values = append(values, nil)
				}
				continue
			}
			// Other structs: try JSON marshal as a single column
			b, err := json.Marshal(rv.Interface())
			if err != nil {
				values = append(values, nil)
			} else {
				values = append(values, string(b))
			}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			// Handle named integer types (e.g., protobuf enums which are named int32 types)
			// Only convert named types (Type().Name() != "") to numeric int64 to avoid changing
			// behavior for built-in unnamed integer types.
			if rv.Type().Name() != "" && rv.Type().PkgPath() != "" {
				values = append(values, rv.Int())
				continue
			}
		case reflect.Slice, reflect.Array, reflect.Map:
			// treat []byte as raw bytes
			if rv.Kind() == reflect.Slice && rv.Type().Elem().Kind() == reflect.Uint8 {
				values = append(values, rv.Bytes())
				continue
			}

			values = append(values, rv.Interface())
			continue
		default:
			// basic kinds: bool, int, float, string, etc.
			values = append(values, rv.Interface())
		}
	}

	return values
}
