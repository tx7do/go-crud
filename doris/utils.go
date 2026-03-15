package doris

import (
	"fmt"
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
