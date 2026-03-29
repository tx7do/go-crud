package doris

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestBuildSelectWithTable(t *testing.T) {
	got := BuildSelectWithTable("posts", "id")
	want := `"posts"."id"`
	if got != want {
		t.Fatalf("expected %s, got %s", want, got)
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
