package sorting

import (
	"reflect"
	"testing"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

func TestOrderByStringConverter_Convert_Empty(t *testing.T) {
	obc := NewOrderByStringConverter()
	got, err := obc.Convert("")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil result for empty input, got: %#v", got)
	}
}

func TestOrderByStringConverter_ParseJsonString_AllEmptyItems(t *testing.T) {
	obc := NewOrderByStringConverter()
	// JSON array with two empty strings
	got, err := obc.ParseJsonString(`["", ""]`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// current implementation 会跳过空项，返回 nil slice
	if got != nil {
		t.Fatalf("expected nil result for [\"\", \"\"], got: %#v", got)
	}
}

func TestOrderByStringConverter_Convert_JSON_SnakeCaseAndDirection(t *testing.T) {
	obc := NewOrderByStringConverter()
	input := `["-createTime", "Name"]`
	got, err := obc.Convert(input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 sortings, got %d: %#v", len(got), got)
	}

	if got[0].Field != "create_time" || got[0].Direction != paginationV1.Sorting_DESC {
		t.Fatalf("first sorting mismatch, want field=create_time dir=DESC, got: %#v", got[0])
	}
	if got[1].Field != "name" || got[1].Direction != paginationV1.Sorting_ASC {
		t.Fatalf("second sorting mismatch, want field=name dir=ASC, got: %#v", got[1])
	}
}

func TestOrderByStringConverter_Convert_AIPFormat(t *testing.T) {
	obc := NewOrderByStringConverter()

	tests := []struct {
		name  string
		input string
		want  []*paginationV1.Sorting
	}{
		{
			name:  "dash prefix",
			input: "foo, bar desc",
			want: []*paginationV1.Sorting{
				{Field: "foo", Direction: paginationV1.Sorting_ASC},
				{Field: "bar", Direction: paginationV1.Sorting_DESC},
			},
		},
		{
			name:  "desc keyword then comma",
			input: "foo desc, bar",
			want: []*paginationV1.Sorting{
				{Field: "foo", Direction: paginationV1.Sorting_DESC},
				{Field: "bar", Direction: paginationV1.Sorting_ASC},
			},
		},
		{
			name:  "spaces around comma with desc on second",
			input: "foo , bar desc",
			want: []*paginationV1.Sorting{
				{Field: "foo", Direction: paginationV1.Sorting_ASC},
				{Field: "bar", Direction: paginationV1.Sorting_DESC},
			},
		},
		{
			name:  "no space after comma",
			input: "foo desc,bar",
			want: []*paginationV1.Sorting{
				{Field: "foo", Direction: paginationV1.Sorting_DESC},
				{Field: "bar", Direction: paginationV1.Sorting_ASC},
			},
		},
		{
			name:  "space before comma",
			input: "foo ,bar desc",
			want: []*paginationV1.Sorting{
				{Field: "foo", Direction: paginationV1.Sorting_ASC},
				{Field: "bar", Direction: paginationV1.Sorting_DESC},
			},
		},
		{
			name:  "comma then desc",
			input: "foo, bar desc",
			want: []*paginationV1.Sorting{
				{Field: "foo", Direction: paginationV1.Sorting_ASC},
				{Field: "bar", Direction: paginationV1.Sorting_DESC},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := obc.Convert(tt.input)
			if err != nil {
				t.Fatalf("unexpected error for input %q: %v", tt.input, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("input %q: expected %#v, got %#v", tt.input, tt.want, got)
			}
		})
	}
}
