package update

import (
	"fmt"
	"strings"

	"entgo.io/ent/dialect/sql"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/tx7do/go-utils/fieldmaskutil"
	"github.com/tx7do/go-utils/stringcase"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func BuildSetNullUpdate(u *sql.UpdateBuilder, fields []string) {
	if len(fields) > 0 {
		for _, field := range fields {
			field = stringcase.ToSnakeCase(field)
			u.SetNull(field)
		}
	}
}

// BuildSetNullUpdater 构建一个UpdateBuilder，用于清空字段的值
func BuildSetNullUpdater(fields []string) func(u *sql.UpdateBuilder) {
	if len(fields) == 0 {
		return nil
	}

	return func(u *sql.UpdateBuilder) {
		BuildSetNullUpdate(u, fields)
	}
}

// escapeSQLIdentifier 转义 PostgreSQL 双引号标识符中的双引号（" → ""），
// 防止 JSON 列名（调用方传入）破坏标识符定界符。键/路径来自 proto 字段
// 名已安全，但 fieldName 作为表列名仍属调用方可控，转义作防御纵深。
func escapeSQLIdentifier(s string) string {
	return strings.ReplaceAll(s, "\"", "\"\"")
}

// escapeSQLLiteral 转义 PostgreSQL 单引号字面量中的单引号（' → ”）。
// 用于 jsonb_build_object 字符串值参数，防止值中的 ' 破坏字面量定界符
// 构成 SQL 注入（键来自 proto 描述符已安全；值来自消息字段可能含任意字符）。
func escapeSQLLiteral(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// ExtractJsonFieldKeyValues 提取json字段的键值对
func ExtractJsonFieldKeyValues(msg proto.Message, paths []string, needToSnakeCase bool) []string {
	var keyValues []string
	rft := msg.ProtoReflect()
	for _, path := range paths {
		fd := rft.Descriptor().Fields().ByName(protoreflect.Name(path))
		if fd == nil {
			continue
		}
		if !rft.Has(fd) {
			continue
		}

		var k string
		if needToSnakeCase {
			k = stringcase.ToSnakeCase(path)
		} else {
			k = path
		}

		keyValues = append(keyValues, fmt.Sprintf("'%s'", escapeSQLLiteral(k)))

		v := rft.Get(fd)
		switch v.Interface().(type) {
		case int32, int64, uint32, uint64, float32, float64, bool:
			keyValues = append(keyValues, fmt.Sprintf("%d", v.Interface()))
		case string:
			keyValues = append(keyValues, fmt.Sprintf("'%s'", escapeSQLLiteral(v.Interface().(string))))
		}
	}

	return keyValues
}

// SetJsonNullFieldUpdateBuilder 设置json字段的空值
func SetJsonNullFieldUpdateBuilder(fieldName string, msg proto.Message, paths []string) func(u *sql.UpdateBuilder) {
	nilPaths := fieldmaskutil.NilValuePaths(msg, paths)
	if len(nilPaths) == 0 {
		return nil
	}

	return func(u *sql.UpdateBuilder) {
		safeField := escapeSQLIdentifier(fieldName)
		u.Set(fieldName,
			sql.Expr(
				fmt.Sprintf("\"%s\" - '{%s}'::text[]", safeField, strings.Join(nilPaths, ",")),
			),
		)
	}
}

// SetJsonFieldValueUpdateBuilder 设置json字段的值
func SetJsonFieldValueUpdateBuilder(fieldName string, msg proto.Message, paths []string, needToSnakeCase bool) func(u *sql.UpdateBuilder) {
	keyValues := ExtractJsonFieldKeyValues(msg, paths, needToSnakeCase)
	if len(keyValues) == 0 {
		return nil
	}

	return func(u *sql.UpdateBuilder) {
		safeField := escapeSQLIdentifier(fieldName)
		u.Set(fieldName,
			sql.Expr(
				fmt.Sprintf("\"%s\" || jsonb_build_object(%s)", safeField, strings.Join(keyValues, ",")),
			),
		)
	}
}

// ApplyNilFieldMask 应用字段掩码以设置字段为NULL
func ApplyNilFieldMask[T interface {
	Modify(...func(*sql.UpdateBuilder)) T
}](
	msg proto.Message,
	updateMask *fieldmaskpb.FieldMask,
	builder T,
) {
	if updateMask == nil {
		return
	}

	nilPaths := fieldmaskutil.NilValuePaths(msg, updateMask.GetPaths())
	nilUpdater := BuildSetNullUpdater(nilPaths)
	if nilUpdater != nil {
		builder.Modify(nilUpdater)
	}
}
