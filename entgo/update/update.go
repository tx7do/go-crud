package update

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"entgo.io/ent/dialect/sql"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/tx7do/go-utils/fieldmaskutil"
	"github.com/tx7do/go-utils/stringcase"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// identifierPattern 是 PostgreSQL 标识符的白名单：字母/下划线开头，后随
// 字母数字下划线。用于校验 JSON 列名（fieldName）与字段名，确保即使
// ent 的 Builder.Ident 对含 " 的标识符会走 default 原样 WriteString（见
// builder.go:2982-2998 / isIdent:3343-3350），白名单也能在源头拒绝。
var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// validateIdentifier 拒绝非白名单标识符。返回 false 时调用方应跳过该字段
// 而非把它喂给 u.Set/u.SetNull（否则 ent 的 Ident 会原样写入构成注入）。
func validateIdentifier(s string) bool {
	return identifierPattern.MatchString(s)
}

func BuildSetNullUpdate(u *sql.UpdateBuilder, fields []string) {
	if len(fields) > 0 {
		for _, field := range fields {
			field = stringcase.ToSnakeCase(field)
			// 白名单校验：非标识符字段跳过，避免 ent Ident 原样写入。
			if !validateIdentifier(field) {
				continue
			}
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

// escapeSQLLiteral 转义 PostgreSQL 单引号字面量中的单引号（' → ”）与
// 反斜杠（\ → \\）。用于 jsonb_build_object 字符串值参数，防止值中的
// ' 破坏字面量定界符构成 SQL 注入（键来自 proto 描述符已安全；值来自
// 消息字段可能含任意字符）。
//
// 反斜杠转义与 elasticsearch/utils.go escapeQueryValue 对齐：PostgreSQL
// 在 standard_conforming_strings=off（旧配置）下会解释字面量中的反斜杠，
// 不转义则留理论残留；默认 on 配置下双引号/单引号转义已足够，此处补
// 反斜杠作防御纵深。
func escapeSQLLiteral(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `'`, `''`)
	return r.Replace(s)
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
		switch t := v.Interface().(type) {
		case int32:
			keyValues = append(keyValues, strconv.FormatInt(int64(t), 10))
		case int64:
			keyValues = append(keyValues, strconv.FormatInt(t, 10))
		case uint32:
			keyValues = append(keyValues, strconv.FormatUint(uint64(t), 10))
		case uint64:
			keyValues = append(keyValues, strconv.FormatUint(t, 10))
		case float32:
			keyValues = append(keyValues, strconv.FormatFloat(float64(t), 'f', -1, 32))
		case float64:
			keyValues = append(keyValues, strconv.FormatFloat(t, 'f', -1, 64))
		case bool:
			keyValues = append(keyValues, strconv.FormatBool(t))
		case string:
			keyValues = append(keyValues, fmt.Sprintf("'%s'", escapeSQLLiteral(t)))
		}
	}

	return keyValues
}

// SetJsonNullFieldUpdateBuilder 设置json字段的空值
func SetJsonNullFieldUpdateBuilder(fieldName string, msg proto.Message, paths []string) func(u *sql.UpdateBuilder) {
	// 白名单校验：非标识符列名直接放弃，避免 ent Ident 原样写入（D-1）。
	if !validateIdentifier(fieldName) {
		return nil
	}
	nilPaths := fieldmaskutil.NilValuePaths(msg, paths)
	if len(nilPaths) == 0 {
		return nil
	}

	safeField := escapeSQLIdentifier(fieldName)
	return func(u *sql.UpdateBuilder) {
		u.Set(safeField,
			sql.Expr(
				fmt.Sprintf("\"%s\" - '{%s}'::text[]", safeField, strings.Join(nilPaths, ",")),
			),
		)
	}
}

// SetJsonFieldValueUpdateBuilder 设置json字段的值
func SetJsonFieldValueUpdateBuilder(fieldName string, msg proto.Message, paths []string, needToSnakeCase bool) func(u *sql.UpdateBuilder) {
	// 白名单校验：非标识符列名直接放弃，避免 ent Ident 原样写入（D-1）。
	if !validateIdentifier(fieldName) {
		return nil
	}
	keyValues := ExtractJsonFieldKeyValues(msg, paths, needToSnakeCase)
	if len(keyValues) == 0 {
		return nil
	}

	safeField := escapeSQLIdentifier(fieldName)
	return func(u *sql.UpdateBuilder) {
		u.Set(safeField,
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
