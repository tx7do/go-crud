package filter

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

func dpPtr(dp paginationV1.DatePart) *paginationV1.DatePart {
	return &dp
}

func TestQueryStringToMap_ObjectAndArray(t *testing.T) {
	qsc := NewQueryStringConverter()

	// object
	obj := `{"amount":"500"}`
	arr, err := qsc.QueryStringToMap(obj)
	if err != nil {
		t.Fatalf("QueryStringToMap(object) error: %v", err)
	}
	if len(arr) != 1 {
		t.Fatalf("object -> want len 1, got %d", len(arr))
	}
	if arr[0]["amount"] != "500" {
		t.Fatalf("object -> amount want %q got %v", "500", arr[0]["amount"])
	}

	// array
	js := `[{"a":"1"},{"b":2}]`
	arr2, err := qsc.QueryStringToMap(js)
	if err != nil {
		t.Fatalf("QueryStringToMap(array) error: %v", err)
	}
	if len(arr2) != 2 {
		t.Fatalf("array -> want len 2, got %d", len(arr2))
	}
	if arr2[0]["a"] != "1" {
		t.Fatalf("array[0].a want %q got %v", "1", arr2[0]["a"])
	}
	if _, ok := arr2[1]["b"].(float64); !ok {
		t.Fatalf("array[1].b want float64 got %T", arr2[1]["b"])
	}
}

func TestConvert_SimpleFieldAndOperator(t *testing.T) {
	qsc := NewQueryStringConverter()

	andJS := `{"amount":"500","active":true}`
	got, err := qsc.Convert(andJS, "")
	if err != nil {
		t.Fatalf("Convert error: %v", err)
	}

	want := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
		Conditions: []*paginationV1.FilterCondition{
			{
				Field: "amount",
				Op:    paginationV1.Operator_EQ,
				ValueOneof: &paginationV1.FilterCondition_Value{
					Value: "500",
				},
			},
			{
				Field: "active",
				Op:    paginationV1.Operator_EQ,
				ValueOneof: &paginationV1.FilterCondition_Value{
					Value: "true",
				},
			},
		},
	}

	if !proto.Equal(want, got) {
		t.Fatalf("Convert simple -> mismatch:\n%s", cmp.Diff(want, got))
	}
}

func TestConvert_OperatorSuffixAndNumber(t *testing.T) {
	qsc := NewQueryStringConverter()

	andJS := `{"amount__lt":500}`
	got, err := qsc.Convert(andJS, "")
	if err != nil {
		t.Fatalf("Convert error: %v", err)
	}

	want := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
		Conditions: []*paginationV1.FilterCondition{
			{
				Field: "amount",
				Op:    paginationV1.Operator_LT,
				ValueOneof: &paginationV1.FilterCondition_Value{
					Value: "500",
				},
			},
		},
	}

	if !proto.Equal(want, got) {
		t.Fatalf("Convert operator suffix -> mismatch:\n%s", cmp.Diff(want, got))
	}
}

func TestConvert_DatePart(t *testing.T) {
	qsc := NewQueryStringConverter()

	andJS := `{"created_at__year__eq":"2023"}`
	got, err := qsc.Convert(andJS, "")
	if err != nil {
		t.Fatalf("Convert error: %v", err)
	}

	want := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
		Conditions: []*paginationV1.FilterCondition{
			{
				Field:      "created_at",
				Op:         paginationV1.Operator_EQ,
				DatePart:   dpPtr(paginationV1.DatePart_YEAR),
				ValueOneof: &paginationV1.FilterCondition_Value{Value: "2023"},
			},
		},
	}

	if !proto.Equal(want, got) {
		t.Fatalf("Convert date part -> mismatch:\n%s", cmp.Diff(want, got))
	}
}

func TestConvert_JsonFieldPath(t *testing.T) {
	qsc := NewQueryStringConverter()

	andJS := `{"meta.name__contains":"alice"}`
	got, err := qsc.Convert(andJS, "")
	if err != nil {
		t.Fatalf("Convert error: %v", err)
	}

	// 生成器在二级键时把 JsonPath 设为第二段并使用 JsonValue
	jsonPath := "name"
	want := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
		Conditions: []*paginationV1.FilterCondition{
			{
				// Note: Field stays as the full key (snake-cased) in implementation for json two-part
				Field:    "meta",
				Op:       paginationV1.Operator_CONTAINS,
				JsonPath: &jsonPath,
				ValueOneof: &paginationV1.FilterCondition_JsonValue{
					JsonValue: structpb.NewStringValue("alice"),
				},
			},
		},
	}

	if !proto.Equal(want, got) {
		t.Fatalf("Convert json path -> mismatch:\n%s", cmp.Diff(want, got))
	}
}

func TestConvert_ORGroupAndArrayInput(t *testing.T) {
	qsc := NewQueryStringConverter()

	orJS := `{"status":"active"}`
	andJS := `[{"x":"1"},{"y":"2"}]`

	got, err := qsc.Convert(andJS, orJS)
	if err != nil {
		t.Fatalf("Convert error: %v", err)
	}

	// expect top-level AND with two conditions from array and one OR group
	want := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
		Conditions: []*paginationV1.FilterCondition{
			{
				Field:      "x",
				Op:         paginationV1.Operator_EQ,
				ValueOneof: &paginationV1.FilterCondition_Value{Value: "1"},
			},
			{
				Field:      "y",
				Op:         paginationV1.Operator_EQ,
				ValueOneof: &paginationV1.FilterCondition_Value{Value: "2"},
			},
		},
		Groups: []*paginationV1.FilterExpr{
			{
				Type: paginationV1.ExprType_OR,
				Conditions: []*paginationV1.FilterCondition{
					{
						Field:      "status",
						Op:         paginationV1.Operator_EQ,
						ValueOneof: &paginationV1.FilterCondition_Value{Value: "active"},
					},
				},
			},
		},
	}

	if !proto.Equal(want, got) {
		t.Fatalf("Convert OR group / array -> mismatch:\n%s", cmp.Diff(want, got))
	}
}
