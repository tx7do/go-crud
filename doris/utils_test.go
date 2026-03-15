package doris

import "testing"

func TestBuildSelectWithTable(t *testing.T) {
	got := BuildSelectWithTable("posts", "id")
	want := `"posts"."id"`
	if got != want {
		t.Fatalf("expected %s, got %s", want, got)
	}
}
