package sorting

import (
	"testing"

	"github.com/stretchr/testify/assert"
	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/opensearch/query"
)

func TestStructuredSorting_BuildOrderClause_NoOrders_NoSort(t *testing.T) {
	ss := NewStructuredSorting()
	qb := query.NewQueryBuilder()

	gotBuilder := ss.BuildOrderClause(qb, nil)
	dsl := gotBuilder.Build()
	_, ok := dsl["sort"]
	assert.False(t, ok, "did not expect sort for nil orders")
}

func TestStructuredSorting_BuildOrderClause_Orderings(t *testing.T) {
	ss := NewStructuredSorting()
	qb := query.NewQueryBuilder()

	orders := []*paginationV1.Sorting{
		{Field: "name", Direction: paginationV1.Sorting_ASC},
		{Field: "age", Direction: paginationV1.Sorting_DESC},
		nil,
		{Field: "", Direction: paginationV1.Sorting_ASC},
		{Field: "UserProfile.name", Direction: paginationV1.Sorting_ASC},
		{Field: "created_at", Direction: paginationV1.Sorting_ASC},
	}
	gotBuilder := ss.BuildOrderClause(qb, orders)
	dsl := gotBuilder.Build()
	sortVal, ok := dsl["sort"]
	assert.True(t, ok, "expected sort applied")
	sortArr, ok := sortVal.([]map[string]any)
	assert.True(t, ok, "sort should be []map[string]any")
	// valid entries: name, age, user_profile.name, created_at => 4 entries
	assert.Equal(t, 4, len(sortArr))
	assert.Equal(t, map[string]any{"order": "asc"}, sortArr[0]["name"])
	assert.Equal(t, map[string]any{"order": "desc"}, sortArr[1]["age"])
	assert.Equal(t, map[string]any{"order": "asc"}, sortArr[2]["user_profile.name"])
	assert.Equal(t, map[string]any{"order": "asc"}, sortArr[3]["created_at"])
}

func TestStructuredSorting_BuildOrderClauseWithDefaultField(t *testing.T) {
	ss := NewStructuredSorting()
	// 未提供 orders -> 应使用默认字段和方向
	qb1 := query.NewQueryBuilder()
	gotBuilder := ss.BuildOrderClauseWithDefaultField(qb1, nil, "created_at", true)
	dsl := gotBuilder.Build()
	sortVal, ok := dsl["sort"]
	assert.True(t, ok, "expected sort applied for default field")
	sortArr, ok := sortVal.([]map[string]any)
	assert.True(t, ok)
	assert.Equal(t, 1, len(sortArr))
	assert.Equal(t, map[string]any{"order": "desc"}, sortArr[0]["created_at"])
	// 提供 orders 时应优先使用 orders 而非默认字段
	qb2 := query.NewQueryBuilder()
	gotBuilder2 := ss.BuildOrderClauseWithDefaultField(qb2, []*paginationV1.Sorting{{Field: "score", Direction: paginationV1.Sorting_DESC}}, "created_at", true)
	dsl2 := gotBuilder2.Build()
	sortVal2, ok2 := dsl2["sort"]
	assert.True(t, ok2)
	sortArr2, ok2 := sortVal2.([]map[string]any)
	assert.True(t, ok2)
	assert.Equal(t, 1, len(sortArr2))
	assert.Equal(t, map[string]any{"order": "desc"}, sortArr2[0]["score"])
}
