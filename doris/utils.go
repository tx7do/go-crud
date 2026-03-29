package doris

import (
	"encoding/json"
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
	// already quoted? if starts and ends with single quote, keep but escape inner ones
	if len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'' {
		inner := v[1 : len(v)-1]
		inner = strings.ReplaceAll(inner, "'", "''")
		return "'" + inner + "'"
	}
	escaped := strings.ReplaceAll(v, "'", "''")
	return "'" + escaped + "'"
}

// ExtractColumnsAndRows extracts columns and rows from a slice of struct/proto.
// 只解析 db tag（如无则用字段名），并对 map 类型字段序列化为 json 字符串。
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
		if dbTag == "-" {
			continue
		}
		col := field.Name
		if dbTag != "" {
			col = strings.Split(dbTag, ",")[0]
		}
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
		if dbTag == "-" {
			continue
		}
		col := field.Name
		if dbTag != "" {
			col = strings.Split(dbTag, ",")[0]
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
