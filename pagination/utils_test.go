package pagination

import (
	"encoding/json"
	"fmt"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"
)

type myStringer struct{ s string }

func (m myStringer) String() string { return m.s }

func TestAnyToString(t *testing.T) {
	str := "hello"
	tests := []struct {
		name string
		in   any
		want string
	}{
		{"nil", nil, ""},
		{"string", "abc", "abc"},
		{"*string", &str, "hello"},
		{"Stringer", myStringer{"sval"}, "sval"},
		{"[]byte", []byte("bytes"), "bytes"},
		{"int", 123, "123"},
		{"float", 3.14, "3.14"},
		{"bool", true, "true"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := AnyToString(tt.in)
			if got != tt.want {
				t.Fatalf("AnyToString(%v) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestAnyToStructValue(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want any // expected AsInterface() or nil
	}{
		{"nil", nil, nil},
		{"string", "x", "x"},
		{"number", 42, float64(42)}, // structpb converts numbers to float64
		{"bool", false, false},
		{"map", map[string]any{"a": 1}, map[string]any{"a": float64(1)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sv := AnyToStructValue(tt.in)
			if tt.name == "nil" {
				if sv == nil {
					t.Fatalf("AnyToStructValue(nil) returned nil, want NullValue")
				}
				if sv.GetNullValue() != structpb.NullValue_NULL_VALUE {
					t.Fatalf("AnyToStructValue(nil) not NullValue: %v", sv)
				}
				return
			}
			got := sv.AsInterface()
			// For maps compare via marshaling to avoid deep type issues; simple equality for others
			switch want := tt.want.(type) {
			case map[string]any:
				// compare by converting both to structpb and back to interfaces for normalization

				var wantSV *structpb.Value
				var err error
				if want == nil {
					structpb.NewNullValue()
				} else {
					wantSV, err = structpb.NewValue(want)
				}

				if err != nil {
					t.Fatalf("failed to create value for want: %v", err)
				}
				if gotStr := wantSV.AsInterface(); !equalInterface(gotStr, got) {
					t.Fatalf("AnyToStructValue(%v).AsInterface() = %v, want %v", tt.in, got, gotStr)
				}
			default:
				if !equalInterface(tt.want, got) {
					t.Fatalf("AnyToStructValue(%v).AsInterface() = %v, want %v", tt.in, got, tt.want)
				}
			}
		})
	}
}

func TestStructValueToString(t *testing.T) {
	type makerFunc func() (*structpb.Value, error)

	cases := []struct {
		name string
		make makerFunc
	}{
		{
			name: "nil pointer",
			make: func() (*structpb.Value, error) { return nil, nil },
		},
		{
			name: "NullValue",
			make: func() (*structpb.Value, error) { return structpb.NewValue(nil) },
		},
		{
			name: "string",
			make: func() (*structpb.Value, error) { return structpb.NewValue("p") },
		},
		{
			name: "number",
			make: func() (*structpb.Value, error) { return structpb.NewValue(3.14) },
		},
		{
			name: "bool",
			make: func() (*structpb.Value, error) { return structpb.NewValue(true) },
		},
		{
			name: "array",
			make: func() (*structpb.Value, error) { return structpb.NewValue([]any{"a", 2}) },
		},
		{
			name: "map",
			make: func() (*structpb.Value, error) { return structpb.NewValue(map[string]any{"x": "y", "n": 1}) },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sv, err := tc.make()
			if err != nil {
				t.Fatalf("failed to create value for case %q: %v", tc.name, err)
			}

			// 计算期望输出，按照 StructValueToString 的行为：
			want := ""
			if sv != nil {
				switch k := sv.Kind.(type) {
				case *structpb.Value_StringValue:
					want = k.StringValue
				case *structpb.Value_NumberValue:
					want = fmt.Sprintf("%v", k.NumberValue)
				case *structpb.Value_BoolValue:
					want = fmt.Sprintf("%v", k.BoolValue)
				case *structpb.Value_NullValue:
					want = ""
				case *structpb.Value_ListValue, *structpb.Value_StructValue:
					if v := sv.AsInterface(); v != nil {
						b, err := json.Marshal(v)
						if err != nil {
							t.Fatalf("failed to marshal interface for case %q: %v", tc.name, err)
						}
						want = string(b)
					} else {
						want = ""
					}
				default:
					want = sv.String()
				}
			}

			got := StructValueToString(sv)
			if got != want {
				t.Fatalf("case %q: StructValueToString = %q, want %q", tc.name, got, want)
			}
		})
	}
}

// equalInterface does a simple deep comparison for basic types and maps produced by structpb
func equalInterface(a, b any) bool {
	switch av := a.(type) {
	case string:
		if bv, ok := b.(string); ok {
			return av == bv
		}
	case bool:
		if bv, ok := b.(bool); ok {
			return av == bv
		}
	case float64:
		if bv, ok := b.(float64); ok {
			return av == bv
		}
	case map[string]any:
		bm, ok := b.(map[string]any)
		if !ok {
			return false
		}
		if len(av) != len(bm) {
			return false
		}
		for k, v := range av {
			if !equalInterface(v, bm[k]) {
				return false
			}
		}
		return true
	}
	return false
}
