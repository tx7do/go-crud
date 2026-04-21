package doris

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

// QuoteIdentifier safely quotes an identifier for SQL (table or column).
// It wraps each part separated by '.' with double quotes and escapes any existing '"'.
func QuoteIdentifier(id string) string {
	// If already contains a dot, quote each part
	parts := strings.Split(id, ".")
	for i, p := range parts {
		p = strings.ReplaceAll(p, `"`, `""`)
		parts[i] = `"` + p + `"`
	}
	return strings.Join(parts, ".")
}

// BuildSelectWithTable builds `table`.`column` style select expression.
// table and column are quoted safely.
func BuildSelectWithTable(table, column string) string {
	return QuoteIdentifier(table) + "." + QuoteIdentifier(column)
}

// BuildInsertSQL builds a parameterized bulk-insert SQL for given table, columns and rowsCount.
// Returns SQL like: INSERT INTO "table" ("c1","c2") VALUES (?,?),(?,?) ...
func BuildInsertSQL(table string, columns []string, rowsCount int) (string, error) {
	if table == "" {
		return "", fmt.Errorf("table empty")
	}
	if len(columns) == 0 {
		return "", fmt.Errorf("columns empty")
	}
	if rowsCount <= 0 {
		return "", fmt.Errorf("rowsCount must be > 0")
	}

	quoted := make([]string, len(columns))
	for i, c := range columns {
		quoted[i] = QuoteIdentifier(c)
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(QuoteIdentifier(table))
	sb.WriteString(" (")
	sb.WriteString(strings.Join(quoted, ","))
	sb.WriteString(") VALUES ")

	single := make([]string, len(columns))
	for i := range single {
		single[i] = "?"
	}
	tuple := "(" + strings.Join(single, ",") + ")"

	tuples := make([]string, rowsCount)
	for i := 0; i < rowsCount; i++ {
		tuples[i] = tuple
	}
	sb.WriteString(strings.Join(tuples, ","))

	return sb.String(), nil
}

// helper regex and formatting
var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
var numUnitRe = regexp.MustCompile(`^-?[0-9]+[KkMmGg]?$`)

func isSafeIdent(s string) bool {
	return identRe.MatchString(s)
}

// formatSessionValue returns a value representation suitable for SET: numeric+unit used as-is,
// otherwise single-quoted with inner quotes escaped.
func formatSessionValue(v string) string {
	v = strings.TrimSpace(v)
	// booleans (true/false) should be unquoted
	if strings.EqualFold(v, "true") || strings.EqualFold(v, "false") {
		return strings.ToLower(v)
	}
	if numUnitRe.MatchString(v) {
		return v
	}
	// already quoted? if starts and ends with single quote, return as is (assume user input is valid SQL string literal)
	if len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'' {
		return v
	}
	escaped := strings.ReplaceAll(v, "'", "''")
	return "'" + escaped + "'"
}

// ExtractColumnsAndRows extracts columns and rows from a slice of struct/proto.
// 只解析 db tag（如无则用字段名），并对 map 类型字段序列化为 json 字符串。
// 支持 db:"col,readonly"，readonly 字段只读（可 select，不可 insert/update）。
func ExtractColumnsAndRows(slice []any) ([]string, [][]any, error) {
	if len(slice) == 0 {
		return nil, nil, fmt.Errorf("no data to extract")
	}
	first := slice[0]
	val := reflect.ValueOf(first)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("element must be struct or pointer to struct")
	}
	var columns []string
	var fieldIndexes []int
	var fieldTypes []reflect.Type
	for i := 0; i < val.NumField(); i++ {
		field := val.Type().Field(i)
		if field.PkgPath != "" {
			continue // skip unexported
		}
		dbTag := field.Tag.Get("db")
		if dbTag == "-" || dbTag == "" {
			continue // only export fields with db tag and not ignored
		}
		parts := strings.Split(dbTag, ",")
		col := parts[0]
		// readonly is only used in ExtractColumnsAndValues, not needed here
		columns = append(columns, col)
		fieldIndexes = append(fieldIndexes, i)
		fieldTypes = append(fieldTypes, field.Type)
	}
	if len(columns) == 0 {
		return nil, nil, fmt.Errorf("no exported fields with tags found")
	}
	var rows [][]any
	for _, item := range slice {
		v := reflect.ValueOf(item)
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}
		if v.Kind() != reflect.Struct {
			return nil, nil, fmt.Errorf("element must be struct or pointer to struct")
		}
		row := make([]any, len(fieldIndexes))
		for idx, fi := range fieldIndexes {
			f := v.Field(fi)
			ft := fieldTypes[idx]
			if ft.Kind() == reflect.Map {
				if f.IsNil() {
					row[idx] = nil
				} else {
					b, err := json.Marshal(f.Interface())
					if err != nil {
						row[idx] = nil
					} else {
						row[idx] = string(b)
					}
				}
			} else {
				row[idx] = f.Interface()
			}
		}
		rows = append(rows, row)
	}
	return columns, rows, nil
}

// ExtractColumnsAndValues extracts columns and values from a struct entity.
// 支持 db:"col,readonly"，readonly 字段只读（可 select，不可 insert/update）。
// 只返回非 readonly 字段。
func ExtractColumnsAndValues(entity any) ([]string, []any, error) {
	val := reflect.ValueOf(entity)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil, nil, fmt.Errorf("entity must be a struct or pointer to struct")
	}

	var columns []string
	var values []any
	for i := 0; i < val.NumField(); i++ {
		field := val.Type().Field(i)
		if field.PkgPath != "" {
			continue // skip unexported
		}

		dbTag := field.Tag.Get("db")
		if dbTag == "-" || dbTag == "" {
			continue // only export fields with db tag and not ignored
		}
		parts := strings.Split(dbTag, ",")
		col := parts[0]
		readonly := false
		for _, p := range parts[1:] {
			if strings.TrimSpace(p) == "readonly" {
				readonly = true
				break
			}
		}
		if readonly {
			continue // skip readonly fields for insert/update
		}

		columns = append(columns, col)
		f := val.Field(i)
		ft := field.Type
		if ft.Kind() == reflect.Map {
			if f.IsNil() {
				values = append(values, nil)
			} else {
				b, err := json.Marshal(f.Interface())
				if err != nil {
					values = append(values, nil)
				} else {
					values = append(values, string(b))
				}
			}
		} else {
			values = append(values, f.Interface())
		}
	}

	if len(columns) == 0 {
		return nil, nil, fmt.Errorf("no exported fields with tags found")
	}

	return columns, values, nil
}

// structToColumnsAndValues 提取 struct 的列名和值，map 字段序列化为 JSON 字符串
func structToColumnsAndValues(v reflect.Value) ([]string, []any, error) {
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if !v.IsValid() || v.Kind() != reflect.Struct {
		return nil, nil, errors.New("input must be a struct or pointer to struct")
	}
	t := v.Type()
	cols := make([]string, 0, t.NumField())
	vals := make([]any, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if sf.PkgPath != "" {
			continue
		}
		col := sf.Tag.Get("db")
		if col == "" {
			col = sf.Tag.Get("json")
			if idx := strings.Index(col, ","); idx != -1 {
				col = col[:idx]
			}
		}
		if col == "" {
			col = strings.ToLower(sf.Name)
		}
		val := v.Field(i).Interface()
		// map、slice、array 类型序列化为 JSON
		switch sf.Type.Kind() {
		case reflect.Map, reflect.Slice, reflect.Array:
			b, err := json.Marshal(val)
			if err != nil {
				return nil, nil, err
			}
			val = string(b)
		}
		cols = append(cols, col)
		vals = append(vals, val)
	}
	return cols, vals, nil
}

// mapToColumnsAndValues 提取 map 的列名和值，map value 也支持嵌套 map 序列化
func mapToColumnsAndValues(m map[string]any) ([]string, []any, error) {
	cols := make([]string, 0, len(m))
	vals := make([]any, 0, len(m))
	for k, v := range m {
		// 嵌套 map 序列化
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Map {
			b, err := json.Marshal(v)
			if err != nil {
				return nil, nil, err
			}
			v = string(b)
		}
		cols = append(cols, k)
		vals = append(vals, v)
	}
	return cols, vals, nil
}
