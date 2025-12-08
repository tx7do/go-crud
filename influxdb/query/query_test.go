package query

import "testing"

func TestBuilder_BasicSelect(t *testing.T) {
	q := NewQueryBuilder("cpu").Select(nil).Build()
	want := "SELECT * FROM cpu"
	if q != want {
		t.Fatalf("got %q, want %q", q, want)
	}
}

func TestBuilder_WhereOperators(t *testing.T) {
	filters := map[string]interface{}{
		"host":  "server1",
		"usage": 0.5,
		"ids":   []int{1, 2},
	}
	ops := map[string]string{
		"host":  "=",
		"usage": ">",
		"ids":   "in",
	}
	q := NewQueryBuilder("cpu").Select(nil).WhereFromMaps(filters, ops).Build()
	// keys are sorted: host, ids, usage
	want := "SELECT * FROM cpu WHERE host = 'server1' AND ids IN (1,2) AND usage > 0.5"
	if q != want {
		t.Fatalf("got %q, want %q", q, want)
	}
}

func TestBuilder_RegexWhere(t *testing.T) {
	filters := map[string]interface{}{
		"message": "err.+",
	}
	ops := map[string]string{
		"message": "regex",
	}
	q := NewQueryBuilder("logs").Select([]string{"message"}).WhereFromMaps(filters, ops).Build()
	want := "SELECT message FROM logs WHERE message =~ /err.+/"
	if q != want {
		t.Fatalf("got %q, want %q", q, want)
	}
}

func TestBuilder_GroupOrderLimitOffset(t *testing.T) {
	q := NewQueryBuilder("metrics").
		Select([]string{"max(value)"}).
		GroupBy("host").
		OrderBy("time", true).
		Limit(10).
		Offset(5).
		Build()
	want := "SELECT max(value) FROM metrics GROUP BY host ORDER BY time DESC LIMIT 10 OFFSET 5"
	if q != want {
		t.Fatalf("got %q, want %q", q, want)
	}
}

func TestBuildQueryWithParams_Helper(t *testing.T) {
	filters := map[string]interface{}{
		"a": 1,
	}
	ops := map[string]string{
		"a": ">=",
	}
	q := BuildQueryWithParams("t", filters, ops, []string{"a", "b"})
	want := "SELECT a, b FROM t WHERE a >= 1"
	if q != want {
		t.Fatalf("got %q, want %q", q, want)
	}
}
