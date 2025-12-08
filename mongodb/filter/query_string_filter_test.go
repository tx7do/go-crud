package filter

import (
	"testing"

	"github.com/tx7do/go-crud/mongodb/query"
)

func TestBuildSelectors_ReturnsBuilder_NoPanic(t *testing.T) {
	sf := NewQueryStringFilter()

	// cases: nil builder (should be created) and non-nil builder (should be returned)
	cases := []struct {
		name    string
		builder *query.Builder
		andJson string
		orJson  string
	}{
		{"NilBuilder_Empty", nil, "", ""},
		{"ProvidedBuilder_Empty", &query.Builder{}, "", ""},
		{"ProvidedBuilder_ANDOR", &query.Builder{}, `{"name":"tom","title__contains":"Go"}`, `{"status__in":"[\"active\",\"pending\"]","title__contains":"Go"}`},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := sf.BuildSelectors(c.builder, c.andJson, c.orJson)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got == nil {
				t.Fatalf("expected non-nil builder")
			}
			// if original builder was provided, expect same pointer returned
			if c.builder != nil && got != c.builder {
				t.Fatalf("expected same builder pointer returned")
			}
		})
	}
}

func TestBuildSelectors_SpecificCases_ReturnsBuilder(t *testing.T) {
	sf := NewQueryStringFilter()

	t.Run("EQ_AND_IN_BETWEEN_DotPath", func(t *testing.T) {
		// EQ
		b1 := &query.Builder{}
		got1, err := sf.BuildSelectors(b1, `{"name":"alice"}`, "")
		if err != nil || got1 == nil || got1 != b1 {
			t.Fatalf("EQ case failed: err=%v, got=nil?%v, same?%v", err, got1 == nil, got1 == b1)
		}

		// IN (comma separated)
		b2 := &query.Builder{}
		got2, err := sf.BuildSelectors(b2, `{"tags__in":"a,b,c"}`, "")
		if err != nil || got2 == nil || got2 != b2 {
			t.Fatalf("IN case failed: err=%v", err)
		}

		// BETWEEN (comma separated)
		b3 := &query.Builder{}
		got3, err := sf.BuildSelectors(b3, `{"created_at__between":"2020-01-01,2021-01-01"}`, "")
		if err != nil || got3 == nil || got3 != b3 {
			t.Fatalf("BETWEEN case failed: err=%v", err)
		}

		// JSON dot path
		b4 := &query.Builder{}
		got4, err := sf.BuildSelectors(b4, `{"preferences.daily_email":"true"}`, "")
		if err != nil || got4 == nil || got4 != b4 {
			t.Fatalf("dot path case failed: err=%v", err)
		}
	})
}

func TestBuildSelectors_EmptyAndInvalid(t *testing.T) {
	sf := NewQueryStringFilter()

	// empty inputs produce no error and a builder
	b := &query.Builder{}
	got, err := sf.BuildSelectors(b, "", "")
	if err != nil {
		t.Fatalf("expected no error for empty inputs, got: %v", err)
	}
	if got == nil || got != b {
		t.Fatalf("expected same non-nil builder returned for empty inputs")
	}

	// invalid JSON returns error
	_, err = sf.BuildSelectors(&query.Builder{}, "not a json", "")
	if err == nil {
		t.Fatalf("expected error for invalid json")
	}
}
