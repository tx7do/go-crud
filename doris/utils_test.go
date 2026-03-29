package doris

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		in   string
		out  string
		name string
	}{
		{"id", `"id"`, "simple"},
		{"table.column", `"table"."column"`, "dot"},
		{"t.a.b", `"t"."a"."b"`, "multi dot"},
		{"a\"b", `"a""b"`, "quote in name"},
	}
	for _, tt := range tests {
		got := QuoteIdentifier(tt.in)
		if got != tt.out {
			t.Errorf("%s: input=%q, want=%q, got=%q", tt.name, tt.in, tt.out, got)
		}
	}
}

func TestBuildSelectWithTable(t *testing.T) {
	tests := []struct {
		table string
		col   string
		out   string
		name  string
	}{
		{"posts", "id", `"posts"."id"`, "simple"},
		{"t", "a.b", `"t"."a"."b"`, "column with dot"},
		{"a.b", "c", `"a"."b"."c"`, "table with dot"},
		{"a\"b", "c", `"a""b"."c"`, "quote in table"},
	}
	for _, tt := range tests {
		got := BuildSelectWithTable(tt.table, tt.col)
		if got != tt.out {
			t.Errorf("%s: table=%q, col=%q, want=%q, got=%q", tt.name, tt.table, tt.col, tt.out, got)
		}
	}
}

func TestExtractColumnsAndRows(t *testing.T) {
	type Demo struct {
		ID         int               `db:"id"`
		Name       string            `db:"name"`
		Meta       map[string]string `db:"meta"`
		Skip       string            `db:"-"`
		Unexported string            // should be ignored
	}
	input := []any{
		Demo{ID: 1, Name: "foo", Meta: map[string]string{"k": "v"}, Skip: "x", Unexported: "u"},
		&Demo{ID: 2, Name: "bar", Meta: nil, Skip: "y", Unexported: "v"},
	}
	cols, rows, err := ExtractColumnsAndRows(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantCols := []string{"id", "name", "meta"}
	if !reflect.DeepEqual(cols, wantCols) {
		t.Errorf("columns mismatch: got %v, want %v", cols, wantCols)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0][0] != 1 || rows[0][1] != "foo" {
		t.Errorf("row 0 basic fields mismatch: %v", rows[0])
	}
	if rows[1][0] != 2 || rows[1][1] != "bar" {
		t.Errorf("row 1 basic fields mismatch: %v", rows[1])
	}
	if rows[0][2] == nil {
		t.Errorf("row 0 meta should not be nil")
	} else {
		var m map[string]string
		if err := json.Unmarshal([]byte(rows[0][2].(string)), &m); err != nil || m["k"] != "v" {
			t.Errorf("row 0 meta json mismatch: %v", rows[0][2])
		}
	}
	if rows[1][2] != nil {
		t.Errorf("row 1 meta should be nil")
	}
}

func TestExtractColumnsAndValues(t *testing.T) {
	type Demo struct {
		ID         int               `db:"id"`
		Name       string            `db:"name"`
		Meta       map[string]string `db:"meta"`
		Skip       string            `db:"-"`
		Unexported string            // should be ignored
	}
	entity := Demo{ID: 3, Name: "baz", Meta: map[string]string{"x": "y"}, Skip: "z", Unexported: "w"}
	cols, vals, err := ExtractColumnsAndValues(entity)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantCols := []string{"id", "name", "meta"}
	if !reflect.DeepEqual(cols, wantCols) {
		t.Errorf("columns mismatch: got %v, want %v", cols, wantCols)
	}
	if len(vals) != 3 {
		t.Fatalf("expected 3 values, got %d", len(vals))
	}
	if vals[0] != 3 || vals[1] != "baz" {
		t.Errorf("basic fields mismatch: %v", vals)
	}
	if vals[2] == nil {
		t.Errorf("meta should not be nil")
	} else {
		var m map[string]string
		if err := json.Unmarshal([]byte(vals[2].(string)), &m); err != nil || m["x"] != "y" {
			t.Errorf("meta json mismatch: %v", vals[2])
		}
	}

	// test pointer
	cols2, vals2, err2 := ExtractColumnsAndValues(&entity)
	if err2 != nil {
		t.Fatalf("unexpected error for pointer: %v", err2)
	}
	if !reflect.DeepEqual(cols2, wantCols) {
		t.Errorf("pointer columns mismatch: got %v, want %v", cols2, wantCols)
	}
	if len(vals2) != 3 {
		t.Fatalf("pointer: expected 3 values, got %d", len(vals2))
	}
}

func TestExtractColumnsAndRows_Readonly(t *testing.T) {
	type Demo struct {
		ID      int    `db:"id"`
		Name    string `db:"name"`
		EventTS int64  `db:"event_ts,readonly"`
	}
	input := []any{
		Demo{ID: 1, Name: "foo", EventTS: 123456},
		&Demo{ID: 2, Name: "bar", EventTS: 654321},
	}
	cols, rows, err := ExtractColumnsAndRows(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantCols := []string{"id", "name", "event_ts"}
	if !reflect.DeepEqual(cols, wantCols) {
		t.Errorf("columns mismatch: got %v, want %v", cols, wantCols)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0][2] != int64(123456) || rows[1][2] != int64(654321) {
		t.Errorf("readonly field mismatch: %v %v", rows[0][2], rows[1][2])
	}
}

func TestExtractColumnsAndValues_Readonly(t *testing.T) {
	type Demo struct {
		ID      int    `db:"id"`
		Name    string `db:"name"`
		EventTS int64  `db:"event_ts,readonly"`
	}
	entity := Demo{ID: 3, Name: "baz", EventTS: 999}
	cols, vals, err := ExtractColumnsAndValues(entity)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantCols := []string{"id", "name"}
	if !reflect.DeepEqual(cols, wantCols) {
		t.Errorf("columns mismatch: got %v, want %v", cols, wantCols)
	}
	if len(vals) != 2 {
		t.Fatalf("expected 2 values, got %d", len(vals))
	}
	if vals[0] != 3 || vals[1] != "baz" {
		t.Errorf("basic fields mismatch: %v", vals)
	}
}

func TestFormatSessionValue(t *testing.T) {
	tests := []struct {
		in   string
		out  string
		name string
	}{
		{"true", "true", "bool true"},
		{"false", "false", "bool false"},
		{"123", "123", "int"},
		{"-456M", "-456M", "int with unit"},
		{"abc", "'abc'", "plain string"},
		{"a'b'c", "'a''b''c'", "string with quote"},
		{"'already quoted'", "'already quoted'", "already quoted"},
		{"'a''b'c'", "'a''b'c'", "already quoted with inner quote (as is)"},
		{"  xyz  ", "'xyz'", "trimmed string"},
	}
	for _, tt := range tests {
		got := formatSessionValue(tt.in)
		if got != tt.out {
			t.Errorf("%s: input=%q, want=%q, got=%q", tt.name, tt.in, tt.out, got)
		}
	}
}

func TestBuildInsertSQL(t *testing.T) {
	table := "my_table"
	cols := []string{"id", "name", "meta"}
	sql, err := BuildInsertSQL(table, cols, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := "INSERT INTO \"my_table\" (\"id\",\"name\",\"meta\") VALUES (?,?,?),(?,?,?)"
	if sql != want {
		t.Errorf("sql mismatch: got %q, want %q", sql, want)
	}

	// test error: empty table
	_, err = BuildInsertSQL("", cols, 2)
	if err == nil {
		t.Error("expected error for empty table")
	}
	// test error: empty columns
	_, err = BuildInsertSQL(table, []string{}, 2)
	if err == nil {
		t.Error("expected error for empty columns")
	}
	// test error: rowsCount <= 0
	_, err = BuildInsertSQL(table, cols, 0)
	if err == nil {
		t.Error("expected error for rowsCount <= 0")
	}
}
