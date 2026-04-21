package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuilderBasic(t *testing.T) {
	b := NewQueryBuilder()
	b.Where(map[string]any{"name": "test"})
	b.Should(map[string]any{"status": "active"})
	b.Filter(map[string]any{"age": map[string]any{"gte": 18}})
	b.MustNot(map[string]any{"deleted": true})
	b.SetSort("created_at", false)
	b.SetPage(2, 20)
	b.SetSource("name", "age")

	dsl := b.Build()
	assert.Equal(t, 20, dsl["size"])
	assert.Equal(t, 20, dsl["from"])
	assert.Equal(t, []string{"name", "age"}, dsl["_source"])
	assert.NotEmpty(t, dsl["sort"])

	boolQuery := dsl["query"].(map[string]any)["bool"].(map[string]any)
	assert.Len(t, boolQuery["must"], 1)
	assert.Len(t, boolQuery["should"], 1)
	assert.Len(t, boolQuery["filter"], 1)
	assert.Len(t, boolQuery["must_not"], 1)
}

func TestBuilderRangeInExists(t *testing.T) {
	b := NewQueryBuilder()
	b.SetRange("age", 18, 30)
	b.SetIn("tags", []any{"go", "es"})
	b.SetExists("email")
	dsl := b.Build()
	boolQuery := dsl["query"].(map[string]any)["bool"].(map[string]any)
	filters := boolQuery["filter"].([]map[string]any)
	assert.True(t, len(filters) >= 3)

	// 检查range
	foundRange := false
	for _, f := range filters {
		if r, ok := f["range"]; ok {
			if r.(map[string]any)["age"] != nil {
				foundRange = true
			}
		}
	}
	assert.True(t, foundRange)

	// 检查terms
	foundTerms := false
	for _, f := range filters {
		if terms, ok := f["terms"]; ok {
			if terms.(map[string]any)["tags"] != nil {
				foundTerms = true
			}
		}
	}
	assert.True(t, foundTerms)

	// 检查exists
	foundExists := false
	for _, f := range filters {
		if exists, ok := f["exists"]; ok {
			if exists.(map[string]any)["field"] == "email" {
				foundExists = true
			}
		}
	}
	assert.True(t, foundExists)
}

func TestBuilderSortAndSource(t *testing.T) {
	b := NewQueryBuilder()
	b.SetSort("score", true)
	b.SetSort("created_at", false)
	b.SetSource("field1", "field2")
	dsl := b.Build()
	assert.Equal(t, []string{"field1", "field2"}, dsl["_source"])
	assert.Len(t, dsl["sort"], 2)
	assert.Equal(t, map[string]any{"score": map[string]any{"order": "asc"}}, dsl["sort"].([]map[string]any)[0])
	assert.Equal(t, map[string]any{"created_at": map[string]any{"order": "desc"}}, dsl["sort"].([]map[string]any)[1])
}

func TestBuilderMustShouldFilterMustNot(t *testing.T) {
	b := NewQueryBuilder()
	b.Where(map[string]any{"field1": "v1"})
	b.Should(map[string]any{"field2": "v2"})
	b.Filter(map[string]any{"field3": "v3"})
	b.MustNot(map[string]any{"field4": "v4"})
	dsl := b.Build()
	boolQuery := dsl["query"].(map[string]any)["bool"].(map[string]any)
	assert.Len(t, boolQuery["must"], 1)
	assert.Len(t, boolQuery["should"], 1)
	assert.Len(t, boolQuery["filter"], 1)
	assert.Len(t, boolQuery["must_not"], 1)
}
