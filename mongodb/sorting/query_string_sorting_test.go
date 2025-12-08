package sorting

import (
	"testing"

	"github.com/tx7do/go-crud/mongodb/query"
	bsonV2 "go.mongodb.org/mongo-driver/v2/bson"
)

func TestParseOrder_BasicCases(t *testing.T) {
	cases := []struct {
		expr     string
		wantCol  string
		wantDesc bool
		wantOk   bool
	}{
		{"-field", "field", true, true},
		{"field", "field", false, true},
		{"field:desc", "field", true, true},
		{"field.desc", "field", true, true},
		{"preferences.daily_email", "preferences.daily_email", false, true},
		{"name;drop", "", false, false},
		{"", "", false, false},
	}

	for _, c := range cases {
		// implementation uses parseOrder
		col, desc, ok := parseOrder(c.expr)
		if ok != c.wantOk {
			t.Fatalf("expr=%q: expected ok=%v, got %v", c.expr, c.wantOk, ok)
		}
		if !ok {
			continue
		}
		if col != c.wantCol || desc != c.wantDesc {
			t.Fatalf("expr=%q: expected (%q, %v), got (%q, %v)", c.expr, c.wantCol, c.wantDesc, col, desc)
		}
	}
}

func TestBuildOrderClause(t *testing.T) {
	qss := NewQueryStringSorting()

	t.Run("empty orders -> no sort applied", func(t *testing.T) {
		qb := query.NewQueryBuilder()
		got := qss.BuildOrderClause(qb, nil)
		if got == nil {
			t.Fatalf("expected builder returned, got nil")
		}

		// 使用 Build() 获取导出的 opts.Sort
		_, opts := got.Build()
		if opts.Sort != nil {
			t.Fatalf("expected no sort for nil orders, got: %#v", opts.Sort)
		}
	})

	t.Run("various orders -> sort contains expected entries", func(t *testing.T) {
		orders := []string{
			"-field",                  // DESC
			"field",                   // ASC
			"field:desc",              // DESC
			"preferences.daily_email", // keep dot path
			"name;drop",               // invalid -> ignored
		}
		qb := query.NewQueryBuilder()
		got := qss.BuildOrderClause(qb, orders)

		if got == nil {
			t.Fatalf("expected builder returned, got nil")
		}

		// 获取 opts.Sort 并转换为 bsonV2.D 以便判断内容
		_, opts := got.Build()
		if opts.Sort == nil {
			t.Fatalf("expected sort applied, got nil")
		}

		var sortD bsonV2.D
		switch v := opts.Sort.(type) {
		case bsonV2.D:
			sortD = v
		case []bsonV2.E:
			sortD = bsonV2.D(v)
		default:
			t.Fatalf("unexpected sort type: %#v", opts.Sort)
		}

		// expect 4 valid entries
		if len(sortD) != 4 {
			t.Fatalf("expected 4 sort entries, got %d: %#v", len(sortD), sortD)
		}

		// check first three corresponding to field orders
		if sortD[0].Key != "field" || sortD[0].Value != int32(-1) {
			t.Fatalf("expected first entry {field, -1}, got %#v", sortD[0])
		}
		if sortD[1].Key != "field" || sortD[1].Value != int32(1) {
			t.Fatalf("expected second entry {field, 1}, got %#v", sortD[1])
		}
		if sortD[2].Key != "field" || sortD[2].Value != int32(-1) {
			t.Fatalf("expected third entry {field, -1}, got %#v", sortD[2])
		}

		// check preferences.daily_email preserved as key
		if sortD[3].Key != "preferences.daily_email" || sortD[3].Value != int32(1) {
			t.Fatalf("expected fourth entry {preferences.daily_email, 1}, got %#v", sortD[3])
		}

		// ensure no dangerous token included in keys
		for _, e := range sortD {
			if e.Key == "" || e.Key == "DROP" {
				t.Fatalf("unexpected dangerous key in sort: %v", e.Key)
			}
		}

		// also verify BSON conversion
		_ = bsonV2.D(sortD)
	})
}
