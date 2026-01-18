package sorting

import (
	"testing"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/mongodb/query"
	bsonV2 "go.mongodb.org/mongo-driver/v2/bson"
)

func TestStructuredSorting_BuildOrderClause_NoOrders_NoSort(t *testing.T) {
	ss := NewStructuredSorting()
	qb := query.NewQueryBuilder()

	gotBuilder := ss.BuildOrderClause(qb, nil)
	_, opts := gotBuilder.Build()
	if opts.Sort != nil {
		t.Fatalf("did not expect sort for nil orders, got: %#v", opts.Sort)
	}
}

func TestStructuredSorting_BuildOrderClause_Orderings(t *testing.T) {
	ss := NewStructuredSorting()
	qb := query.NewQueryBuilder()

	orders := []*paginationV1.Sorting{
		{Field: "name", Direction: paginationV1.Sorting_ASC},
		{Field: "age", Direction: paginationV1.Sorting_DESC},
		nil,
		{Field: "", Direction: paginationV1.Sorting_ASC},
		{Field: "UserProfile.name", Direction: paginationV1.Sorting_ASC}, // first segment -> snake_case
		{Field: "created_at", Direction: paginationV1.Sorting_ASC},
	}

	gotBuilder := ss.BuildOrderClause(qb, orders)
	_, opts := gotBuilder.Build()
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

	// valid entries: name, age, user_profile.name, created_at => 4 entries
	if len(sortD) != 4 {
		t.Fatalf("expected 4 sort entries, got %d: %#v", len(sortD), sortD)
	}

	if sortD[0].Key != "name" || sortD[0].Value != int32(1) {
		t.Fatalf("expected first entry {name, 1}, got %#v", sortD[0])
	}
	if sortD[1].Key != "age" || sortD[1].Value != int32(-1) {
		t.Fatalf("expected second entry {age, -1}, got %#v", sortD[1])
	}
	if sortD[2].Key != "user_profile.name" || sortD[2].Value != int32(1) {
		t.Fatalf("expected third entry {user_profile.name, 1}, got %#v", sortD[2])
	}
	if sortD[3].Key != "created_at" || sortD[3].Value != int32(1) {
		t.Fatalf("expected fourth entry {created_at, 1}, got %#v", sortD[3])
	}
}

func TestStructuredSorting_BuildOrderClauseWithDefaultField(t *testing.T) {
	ss := NewStructuredSorting()

	// 未提供 orders -> 应使用默认字段和方向
	qb1 := query.NewQueryBuilder()
	gotBuilder := ss.BuildOrderClauseWithDefaultField(qb1, nil, "created_at", true)
	_, opts := gotBuilder.Build()
	if opts.Sort == nil {
		t.Fatalf("expected sort applied for default field, got nil")
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
	if len(sortD) != 1 || sortD[0].Key != "created_at" || sortD[0].Value != int32(-1) {
		t.Fatalf("expected ORDER BY created_at DESC, got: %#v", sortD)
	}

	// 提供 orders 时应优先使用 orders 而非默认字段
	qb2 := query.NewQueryBuilder()
	gotBuilder2 := ss.BuildOrderClauseWithDefaultField(qb2, []*paginationV1.Sorting{{Field: "score", Direction: paginationV1.Sorting_DESC}}, "created_at", true)
	_, opts2 := gotBuilder2.Build()
	if opts2.Sort == nil {
		t.Fatalf("expected sort applied, got nil")
	}
	switch v := opts2.Sort.(type) {
	case bsonV2.D:
		sortD = v
	case []bsonV2.E:
		sortD = bsonV2.D(v)
	default:
		t.Fatalf("unexpected sort type: %#v", opts2.Sort)
	}
	if len(sortD) != 1 || sortD[0].Key != "score" || sortD[0].Value != int32(-1) {
		t.Fatalf("expected ORDER BY score DESC, got: %#v", sortD)
	}
}
