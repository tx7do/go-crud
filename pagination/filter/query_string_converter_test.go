package filter

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/tx7do/go-utils/stringcase"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
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
	got, err := qsc.Convert(andJS)
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
	got, err := qsc.Convert(andJS)
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
	got, err := qsc.Convert(andJS)
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
	got, err := qsc.Convert(andJS)
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

	// 合并为一个顶层数组：第一个元素是数组（表示一组 AND 条件），第二个元素是包含 $or 的对象
	combinedJS := `[[{"x":"1"},{"y":"2"}], {"$or":[{"status":"active"}]}]`

	got, err := qsc.Convert(combinedJS)
	if err != nil {
		t.Fatalf("Convert error: %v", err)
	}

	// 期望：root 为 AND，包含一个 AND 子组；该子组含有两个条件，并且包含一个 OR 子组
	want := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
		Groups: []*paginationV1.FilterExpr{
			{
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
			},
		},
	}

	if !proto.Equal(want, got) {
		t.Fatalf("Convert OR group / array -> mismatch:\n%s", cmp.Diff(want, got, protocmp.Transform()))
	}
}

func TestParseQuery_ArrayTopLevel(t *testing.T) {
	qsc := NewQueryStringConverter()

	js := `[{"a":"1"},{"b":2}]`
	got, err := qsc.ParseQuery(js)
	if err != nil {
		t.Fatalf("ParseQuery(array) error: %v", err)
	}

	want := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
		Groups: []*paginationV1.FilterExpr{
			{
				Type: paginationV1.ExprType_AND,
				Conditions: []*paginationV1.FilterCondition{
					{
						Field:      "a",
						Op:         paginationV1.Operator_EQ,
						ValueOneof: &paginationV1.FilterCondition_Value{Value: "1"},
					},
					{
						Field:      "b",
						Op:         paginationV1.Operator_EQ,
						ValueOneof: &paginationV1.FilterCondition_Value{Value: "2"},
					},
				},
			},
		},
	}

	if !proto.Equal(want, got) {
		t.Fatalf("ParseQuery array -> mismatch:\n%s", cmp.Diff(want, got))
	}
}

func TestParseQuery_ObjectWithAndOr(t *testing.T) {
	qsc := NewQueryStringConverter()

	andJS := `{"$and":[{"x":"1"},{"y":"2"}]}`
	gotAnd, err := qsc.ParseQuery(andJS)
	if err != nil {
		t.Fatalf("ParseQuery($and) error: %v", err)
	}
	wantAnd := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
		Groups: []*paginationV1.FilterExpr{
			{
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
			},
		},
	}
	if !proto.Equal(wantAnd, gotAnd) {
		t.Fatalf("ParseQuery $and -> mismatch:\n%s", cmp.Diff(wantAnd, gotAnd))
	}

	orJS := `{"$or":[{"status":"active"}]}`
	gotOr, err := qsc.ParseQuery(orJS)
	if err != nil {
		t.Fatalf("ParseQuery($or) error: %v", err)
	}
	wantOr := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
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
	if !proto.Equal(wantOr, gotOr) {
		t.Fatalf("ParseQuery $or -> mismatch:\n%s", cmp.Diff(wantOr, gotOr))
	}
}

func TestParseQuery_InvalidType(t *testing.T) {
	qsc := NewQueryStringConverter()

	// 顶层为字符串（非对象/数组）应返回错误
	js := `"abc"`
	_, err := qsc.ParseQuery(js)
	if err == nil {
		t.Fatalf("ParseQuery(invalid) expected error, got nil")
	}
}

func TestConvert_EmptyArray(t *testing.T) {
	qsc := NewQueryStringConverter()

	js := `[]`
	got, err := qsc.Convert(js)
	if err != nil {
		t.Fatalf("Convert(empty array) error: %v", err)
	}

	want := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
		Groups: []*paginationV1.FilterExpr{
			{
				Type: paginationV1.ExprType_AND,
			},
		},
	}

	if !proto.Equal(want, got) {
		t.Fatalf("Convert empty array -> mismatch:\n%s", cmp.Diff(want, got, protocmp.Transform()))
	}
}

func TestConvert_ArrayWithNonObjectElement(t *testing.T) {
	qsc := NewQueryStringConverter()

	js := `[{"a":"1"}, "not_an_object"]`
	_, err := qsc.Convert(js)
	if err == nil {
		t.Fatalf("Convert(array with non-object) expected error, got nil")
	}
}

func TestParseQuery_AndOrWithNonArrayValue(t *testing.T) {
	qsc := NewQueryStringConverter()

	// $and 非数组
	js1 := `{"$and": "nope"}`
	_, err := qsc.ParseQuery(js1)
	if err == nil {
		t.Fatalf("ParseQuery($and non-array) expected error, got nil")
	}

	// $or 非数组
	js2 := `{"$or": {"a":1}}`
	_, err = qsc.ParseQuery(js2)
	if err == nil {
		t.Fatalf("ParseQuery($or non-array) expected error, got nil")
	}
}

func TestConvert_NestedArrayFlattening_SingleInnerArray(t *testing.T) {
	qsc := NewQueryStringConverter()

	// 双重数组，内层数组包含对象，期望被扁平化为单个 AND 子组并包含条件
	js := `[[{"a":"1"}]]`
	got, err := qsc.Convert(js)
	if err != nil {
		t.Fatalf("Convert(nested array) error: %v", err)
	}

	// root -> Groups[0] should be AND with one condition a == "1"
	if len(got.Groups) != 1 {
		t.Fatalf("nested flattening -> want 1 group, got %d", len(got.Groups))
	}
	sub := got.Groups[0]
	if sub.Type != paginationV1.ExprType_AND {
		t.Fatalf("nested flattening -> want sub type AND, got %v", sub.Type)
	}
	if len(sub.Conditions) != 1 {
		t.Fatalf("nested flattening -> want 1 condition, got %d", len(sub.Conditions))
	}
	cond := sub.Conditions[0]
	if cond.Field != "a" || cond.Op != paginationV1.Operator_EQ || cond.GetValue() != "1" {
		t.Fatalf("nested flattening -> unexpected condition: %+v", cond)
	}
}

func TestConvert_JsonFieldWithMoreThanTwoParts(t *testing.T) {
	qsc := NewQueryStringConverter()

	// key 中有多个点段（非标准二段 json path），预期不会被解析为 JsonValue/JsonPath，
	// 而是作为普通字段名处理（并做 snake_case 转换），值作为普通字符串值。
	js := `{"meta.name.extra__contains":"alice"}`
	got, err := qsc.Convert(js)
	if err != nil {
		t.Fatalf("Convert(json multi-part) error: %v", err)
	}

	if len(got.Conditions) != 1 {
		t.Fatalf("json multi-part -> want 1 condition, got %d", len(got.Conditions))
	}
	cond := got.Conditions[0]

	// Field 应为 snake_case 后的原始 key（包含点），不应设置 JsonPath
	expectedField := stringcase.ToSnakeCase("meta.name.extra")
	if cond.Field != expectedField {
		t.Fatalf("json multi-part -> field mismatch, want %q got %q", expectedField, cond.Field)
	}
	if cond.JsonPath != nil {
		t.Fatalf("json multi-part -> expected JsonPath to be nil, got %v", *cond.JsonPath)
	}
	if cond.Op != paginationV1.Operator_CONTAINS {
		t.Fatalf("json multi-part -> expected op CONTAINS, got %v", cond.Op)
	}
	if cond.GetValue() != "alice" {
		t.Fatalf("json multi-part -> expected value %q, got %q", "alice", cond.GetValue())
	}
}

func TestConvert_TopLevelNumberInvalid(t *testing.T) {
	qsc := NewQueryStringConverter()

	js := `123`
	_, err := qsc.ParseQuery(js)
	if err == nil {
		t.Fatalf("ParseQuery(top-level number) expected error, got nil")
	}
}
