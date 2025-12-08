package sorting

import (
	"strings"
	"testing"

	pagination "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/clickhouse/query"
)

func TestStructuredSorting_BuildOrderClause_NoOrders_NoOrderBy(t *testing.T) {
	ss := NewStructuredSorting()
	qb := query.NewQueryBuilder("test_table", nil)

	// nil orders -> 不应添加 ORDER BY
	gotBuilder := ss.BuildOrderClause(qb, nil)
	sql, _ := gotBuilder.Build()
	up := strings.ToUpper(sql)
	if strings.Contains(up, "ORDER BY") {
		t.Fatalf("did not expect ORDER BY for nil orders, got: %s", sql)
	}
}

func TestStructuredSorting_BuildOrderClause_Orderings(t *testing.T) {
	ss := NewStructuredSorting()
	qb := query.NewQueryBuilder("test_table", nil)

	orders := []*pagination.Sorting{
		{Field: "name", Order: pagination.Sorting_ASC},
		{Field: "age", Order: pagination.Sorting_DESC},
		nil,
		{Field: "", Order: pagination.Sorting_ASC},
		{Field: "created_at", Order: pagination.Sorting_ASC},
	}

	gotBuilder := ss.BuildOrderClause(qb, orders)
	sql, _ := gotBuilder.Build()
	up := strings.ToUpper(sql)

	if !strings.Contains(up, "ORDER BY") {
		t.Fatalf("expected ORDER BY in result, got: %s", sql)
	}
	if !strings.Contains(up, "NAME") {
		t.Fatalf("expected ordering by name, got: %s", sql)
	}
	if !strings.Contains(up, "AGE") || !strings.Contains(up, "DESC") {
		t.Fatalf("expected ordering by age DESC, got: %s", sql)
	}
	if !strings.Contains(up, "CREATED_AT") {
		t.Fatalf("expected ordering by created_at, got: %s", sql)
	}
}

func TestStructuredSorting_BuildOrderClauseWithDefaultField(t *testing.T) {
	ss := NewStructuredSorting()

	// orders 为空时应使用默认字段和方向
	qb1 := query.NewQueryBuilder("test_table", nil)
	gotBuilder := ss.BuildOrderClauseWithDefaultField(qb1, nil, "created_at", true)
	sql, _ := gotBuilder.Build()
	up := strings.ToUpper(sql)
	if !strings.Contains(up, "ORDER BY") || !strings.Contains(up, "CREATED_AT") || !strings.Contains(up, "DESC") {
		t.Fatalf("expected ORDER BY created_at DESC, got: %s", sql)
	}

	// 提供 orders 时应优先使用 orders 而非默认字段
	qb2 := query.NewQueryBuilder("test_table", nil)
	gotBuilder2 := ss.BuildOrderClauseWithDefaultField(qb2, []*pagination.Sorting{{Field: "score", Order: pagination.Sorting_DESC}}, "created_at", true)
	sql2, _ := gotBuilder2.Build()
	up2 := strings.ToUpper(sql2)
	if strings.Contains(up2, "CREATED_AT") {
		t.Fatalf("did not expect default field to be used when orders provided, got: %s", sql2)
	}
	if !strings.Contains(up2, "SCORE") || !strings.Contains(up2, "DESC") {
		t.Fatalf("expected ORDER BY score DESC, got: %s", sql2)
	}
}
