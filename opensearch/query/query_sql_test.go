package query

import (
	"strings"
	"testing"
)

func TestBuilder_BuildSQL_Basic(t *testing.T) {
	b := NewQueryBuilder()
	b.Where(map[string]any{"term": map[string]any{"name": "Tom"}})
	b.Filter(map[string]any{"range": map[string]any{"age": map[string]any{"gte": 18, "lte": 30}}})
	b.SetSort("age", false)
	b.SetFromSize(10, 20)
	b.SetSource("id", "name", "age")

	sql := b.BuildSQL("users")

	expected := "SELECT id, name, age FROM users WHERE name = 'Tom' AND age >= 18 AND age <= 30 ORDER BY age DESC LIMIT 20 OFFSET 10"
	if strings.TrimSpace(sql) != expected {
		t.Errorf("unexpected SQL:\nwant: %s\ngot:  %s", expected, sql)
	}
}

func TestBuilder_BuildSQL_Empty(t *testing.T) {
	b := NewQueryBuilder()
	sql := b.BuildSQL("users")
	expected := "SELECT * FROM users LIMIT 10"
	if strings.TrimSpace(sql) != expected {
		t.Errorf("unexpected SQL for empty builder:\nwant: %s\ngot:  %s", expected, sql)
	}
}

func TestBuilder_BuildSQL_IN(t *testing.T) {
	b := NewQueryBuilder()
	b.Where(map[string]any{"terms": map[string]any{"status": []any{"A", "B"}}})
	sql := b.BuildSQL("orders")
	expected := "SELECT * FROM orders WHERE status IN ('A', 'B') LIMIT 10"
	if strings.TrimSpace(sql) != expected {
		t.Errorf("unexpected SQL for IN:\nwant: %s\ngot:  %s", expected, sql)
	}
}

func TestBuilder_BuildSQL_Exists(t *testing.T) {
	b := NewQueryBuilder()
	b.Where(map[string]any{"exists": map[string]any{"field": "email"}})
	sql := b.BuildSQL("users")
	expected := "SELECT * FROM users WHERE email IS NOT NULL LIMIT 10"
	if strings.TrimSpace(sql) != expected {
		t.Errorf("unexpected SQL for exists:\nwant: %s\ngot:  %s", expected, sql)
	}
}
