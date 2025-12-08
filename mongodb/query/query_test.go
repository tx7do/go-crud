package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson/primitive"
	bsonV2 "go.mongodb.org/mongo-driver/v2/bson"
)

func TestQueryBuilder(t *testing.T) {
	qb := NewQueryBuilder()

	// SetFilter
	filter := bsonV2.M{"name": "test"}
	qb.SetFilter(filter)
	assert.Equal(t, filter, qb.filter)

	// SetLimit
	limit := int64(10)
	qb.SetLimit(limit)
	assert.NotNil(t, qb.findOpts.Limit)
	assert.Equal(t, limit, *qb.findOpts.Limit)

	// SetSort
	sort := bsonV2.D{{Key: "name", Value: 1}}
	qb.SetSort(sort)
	assert.NotNil(t, qb.findOpts.Sort)
	assert.Equal(t, sort, qb.findOpts.Sort)

	// Build
	finalFilter, finalOpts := qb.Build()
	assert.Equal(t, filter, finalFilter)
	assert.Equal(t, qb.findOpts, finalOpts)
}

func TestQueryBuilderMethods(t *testing.T) {
	qb := NewQueryBuilder()

	// SetFilter
	filter := bsonV2.M{"name": "test"}
	qb.SetFilter(filter)
	assert.Equal(t, filter, qb.filter)

	// SetNotEqual
	qb.SetNotEqual("status", "inactive")
	assert.Equal(t, bsonV2.M{OperatorNe: "inactive"}, qb.filter["status"])

	// SetGreaterThan
	qb.SetGreaterThan("age", 18)
	assert.Equal(t, bsonV2.M{OperatorGt: 18}, qb.filter["age"])

	// SetLessThan
	qb.SetLessThan("age", 30)
	assert.Equal(t, bsonV2.M{OperatorLt: 30}, qb.filter["age"])

	// SetExists
	qb.SetExists("email", true)
	assert.Equal(t, bsonV2.M{OperatorExists: true}, qb.filter["email"])

	// SetType
	qb.SetType("age", "int")
	assert.Equal(t, bsonV2.M{OperatorType: "int"}, qb.filter["age"])

	// SetBetween
	qb.SetBetween("price", 10, 100)
	assert.Equal(t, bsonV2.M{OperatorGte: 10, OperatorLte: 100}, qb.filter["price"])

	// SetOr
	orConditions := []bsonV2.M{
		{"status": "active"},
		{"status": "pending"},
	}
	qb.SetOr(orConditions)
	assert.Equal(t, orConditions, qb.filter[OperatorOr])

	// SetAnd
	andConditions := []bsonV2.M{
		{"age": bsonV2.M{OperatorGt: 18}},
		{"status": "active"},
	}
	qb.SetAnd(andConditions)
	assert.Equal(t, andConditions, qb.filter[OperatorAnd])

	// SetLimit
	limit := int64(10)
	qb.SetLimit(limit)
	assert.NotNil(t, qb.findOpts.Limit)
	assert.Equal(t, limit, *qb.findOpts.Limit)

	// SetSort
	sort := bsonV2.D{{Key: "name", Value: 1}}
	qb.SetSort(sort)
	assert.NotNil(t, qb.findOpts.Sort)
	assert.Equal(t, sort, qb.findOpts.Sort)

	// SetSortWithPriority
	sortWithPriority := []bsonV2.E{{Key: "priority", Value: -1}, {Key: "name", Value: 1}}
	qb.SetSortWithPriority(sortWithPriority)
	assert.Equal(t, bsonV2.D(sortWithPriority), qb.findOpts.Sort)

	// SetProjection
	projection := bsonV2.M{"name": 1, "age": 1}
	qb.SetProjection(projection)
	assert.Equal(t, projection, qb.findOpts.Projection)

	// SetSkip
	skip := int64(5)
	qb.SetSkip(skip)
	assert.NotNil(t, qb.findOpts.Skip)
	assert.Equal(t, skip, *qb.findOpts.Skip)

	// SetPage
	page, size := int64(2), int64(10)
	qb.SetPage(page, size)
	assert.NotNil(t, qb.findOpts.Limit)
	assert.NotNil(t, qb.findOpts.Skip)
	assert.Equal(t, size, *qb.findOpts.Limit)
	expectedSkip := (page - 1) * size
	assert.Equal(t, expectedSkip, *qb.findOpts.Skip)

	// SetRegex
	qb.SetRegex("name", "^test", "")
	expectedRegexDoc := bsonV2.M{OperatorRegex: primitive.Regex{Pattern: "^test", Options: ""}}
	assert.Equal(t, expectedRegexDoc, qb.filter["name"])

	// SetIn
	qb.SetIn("tags", []interface{}{"tag1", "tag2"})
	assert.Equal(t, bsonV2.M{OperatorIn: []interface{}{"tag1", "tag2"}}, qb.filter["tags"])

	// Build
	finalFilter, finalOpts := qb.Build()
	assert.Equal(t, qb.filter, finalFilter)
	assert.Equal(t, qb.findOpts, finalOpts)
}

func TestSetGeoWithin(t *testing.T) {
	qb := NewQueryBuilder()

	field := "location"
	geometry := bsonV2.M{"type": "Polygon", "coordinates": []interface{}{
		[]interface{}{
			[]float64{40.0, -70.0},
			[]float64{41.0, -70.0},
			[]float64{41.0, -71.0},
			[]float64{40.0, -71.0},
			[]float64{40.0, -70.0},
		},
	}}

	qb.SetGeoWithin(field, geometry)

	expected := bsonV2.M{
		OperatorGeoWithin: bsonV2.M{
			OperatorGeometry: geometry,
		},
	}

	assert.Equal(t, expected, qb.filter[field])
}

func TestSetGeoIntersects(t *testing.T) {
	qb := NewQueryBuilder()

	field := "location"
	geometry := bsonV2.M{"type": "LineString", "coordinates": [][]float64{
		{40.0, -70.0},
		{41.0, -71.0},
	}}

	qb.SetGeoIntersects(field, geometry)

	expected := bsonV2.M{
		OperatorGeoIntersects: bsonV2.M{
			OperatorGeometry: geometry,
		},
	}

	assert.Equal(t, expected, qb.filter[field])
}

func TestSetNear(t *testing.T) {
	qb := NewQueryBuilder()

	field := "location"
	point := bsonV2.M{"type": "Point", "coordinates": []float64{40.7128, -74.0060}}
	maxDistance := 500.0
	minDistance := 50.0

	qb.SetNear(field, point, maxDistance, minDistance)

	expected := bsonV2.M{
		OperatorNear: bsonV2.M{
			OperatorGeometry:    point,
			OperatorMaxDistance: maxDistance,
			OperatorMinDistance: minDistance,
		},
	}

	assert.Equal(t, expected, qb.filter[field])
}

func TestSetNearSphere(t *testing.T) {
	qb := NewQueryBuilder()

	field := "location"
	point := bsonV2.M{"type": "Point", "coordinates": []float64{40.7128, -74.0060}}
	maxDistance := 1000.0
	minDistance := 100.0

	qb.SetNearSphere(field, point, maxDistance, minDistance)

	expected := bsonV2.M{
		OperatorNearSphere: bsonV2.M{
			OperatorGeometry:    point,
			OperatorMaxDistance: maxDistance,
			OperatorMinDistance: minDistance,
		},
	}

	assert.Equal(t, expected, qb.filter[field])
}

func TestQueryBuilderPipeline(t *testing.T) {
	qb := NewQueryBuilder()

	matchStage := bsonV2.D{{Key: OperatorMatch, Value: bsonV2.M{"status": "active"}}}
	groupStage := bsonV2.D{{Key: OperatorGroup, Value: bsonV2.M{"_id": "$category", "count": bsonV2.M{OperatorSum: 1}}}}
	sortStage := bsonV2.D{{Key: OperatorSortAgg, Value: bsonV2.M{"count": -1}}}

	qb.AddStage(matchStage).AddStage(groupStage).AddStage(sortStage)

	pipeline := qb.BuildPipeline()

	expectedPipeline := []bsonV2.D{matchStage, groupStage, sortStage}
	assert.Equal(t, expectedPipeline, pipeline)
}

func TestOtherOperators(t *testing.T) {
	qb := NewQueryBuilder()

	// SetAll
	qb.SetAll("arr", []interface{}{1, 2, 3})
	assert.Equal(t, bsonV2.M{OperatorAll: []interface{}{1, 2, 3}}, qb.filter["arr"])

	// SetElemMatch
	qb.SetElemMatch("arr2", bsonV2.M{"score": bsonV2.M{OperatorGt: 10}})
	assert.Equal(t, bsonV2.M{OperatorElemMatch: bsonV2.M{"score": bsonV2.M{OperatorGt: 10}}}, qb.filter["arr2"])

	// SetSize
	qb.SetSize("arr3", 5)
	assert.Equal(t, bsonV2.M{OperatorSize: 5}, qb.filter["arr3"])

	// SetCurrentDate
	qb.SetCurrentDate("updatedAt")
	assert.Equal(t, bsonV2.M{OperatorCurrentDate: true}, qb.filter["updatedAt"])

	// SetTextSearch
	qb.SetTextSearch("hello world")
	assert.Equal(t, bsonV2.M{OperatorSearch: "hello world"}, qb.filter[OperatorText])

	// SetMod
	qb.SetMod("count", 3, 1)
	assert.Equal(t, bsonV2.M{OperatorMod: bsonV2.A{3, 1}}, qb.filter["count"])

	// SetNotIn
	qb.SetNotIn("exclude", []interface{}{"a", "b"})
	assert.Equal(t, bsonV2.M{OperatorNin: []interface{}{"a", "b"}}, qb.filter["exclude"])
}

func TestBuildImmutability(t *testing.T) {
	qb := NewQueryBuilder()

	// prepare initial state
	qb.SetFilter(bsonV2.M{"k": "v"})
	qb.SetLimit(100)
	qb.SetSkip(10)

	// build copies
	beforeFilter, beforeOpts := qb.Build()

	// mutate original builder after build
	qb.SetFilter(bsonV2.M{"k": "changed"})
	qb.SetLimit(200)
	qb.SetSkip(20)

	// ensure built copies unchanged
	assert.Equal(t, bsonV2.M{"k": "v"}, beforeFilter)
	if beforeOpts.Limit != nil {
		assert.Equal(t, int64(100), *beforeOpts.Limit)
	}
	if beforeOpts.Skip != nil {
		assert.Equal(t, int64(10), *beforeOpts.Skip)
	}
}
