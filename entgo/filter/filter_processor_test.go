package filter

import (
	"testing"

	"entgo.io/ent/dialect/sql"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

func newSelector() *sql.Selector {
	// 创建一个简单的 selector，用于构造列引用。
	// 只需要一个可用的 *sql.Selector 实例，具体表名无关紧要。
	return sql.Select().From(sql.Table("users"))
}

func TestProcessor_BasicOperators(t *testing.T) {
	s := newSelector()
	p := sql.P()
	proc := Processor{}

	t.Run("Equal_NotNil", func(t *testing.T) {
		if got := proc.Equal(s, p, "name", "tom"); got == nil {
			t.Fatalf("Equal returned nil, want non-nil")
		}
	})

	t.Run("NotEqual_NotNil", func(t *testing.T) {
		if got := proc.NotEqual(s, p, "name", "tom"); got == nil {
			t.Fatalf("NotEqual returned nil, want non-nil")
		}
	})

	t.Run("In_ValidJSON", func(t *testing.T) {
		if got := proc.In(s, p, "name", `["a","b"]`, nil); got == nil {
			t.Fatalf("In returned nil for valid JSON array")
		}
	})

	t.Run("In_InvalidJSON", func(t *testing.T) {
		if got := proc.In(s, p, "name", `notjson`, nil); got != nil {
			t.Fatalf("In returned non-nil for invalid JSON, want nil")
		}
	})

	t.Run("NotIn_ValidJSON", func(t *testing.T) {
		if got := proc.NotIn(s, p, "name", `["a","b"]`, nil); got == nil {
			t.Fatalf("NotIn returned nil for valid JSON array")
		}
	})

	t.Run("Range_Valid", func(t *testing.T) {
		if got := proc.Range(s, p, "created_at", `["2020-01-01","2021-01-01"]`, nil); got == nil {
			t.Fatalf("Range returned nil for valid range")
		}
	})

	t.Run("Range_InvalidLength", func(t *testing.T) {
		if got := proc.Range(s, p, "created_at", `["only"]`, nil); got != nil {
			t.Fatalf("Range returned non-nil for invalid range length")
		}
	})

	t.Run("IsNull_NotNil", func(t *testing.T) {
		if got := proc.IsNull(s, p, "deleted_at", ""); got == nil {
			t.Fatalf("IsNull returned nil, want non-nil")
		}
	})

	t.Run("IsNotNull_NotNil", func(t *testing.T) {
		if got := proc.IsNotNull(s, p, "deleted_at", ""); got == nil {
			t.Fatalf("IsNotNull returned nil, want non-nil")
		}
	})
}

func TestProcessor_StringOperatorsAndRegex(t *testing.T) {
	s := newSelector()
	p := sql.P()
	proc := Processor{}

	t.Run("Contains_NotNil", func(t *testing.T) {
		if got := proc.Contains(s, p, "title", "go"); got == nil {
			t.Fatalf("Contains returned nil")
		}
	})

	t.Run("StartsWith_NotNil", func(t *testing.T) {
		if got := proc.StartsWith(s, p, "title", "Go"); got == nil {
			t.Fatalf("StartsWith returned nil")
		}
	})

	t.Run("EndsWith_NotNil", func(t *testing.T) {
		if got := proc.EndsWith(s, p, "title", "Lang"); got == nil {
			t.Fatalf("EndsWith returned nil")
		}
	})

	t.Run("Exact_NotNil", func(t *testing.T) {
		if got := proc.Exact(s, p, "status", "active"); got == nil {
			t.Fatalf("Exact returned nil")
		}
	})

	t.Run("Regex_NotNil", func(t *testing.T) {
		if got := proc.Regex(s, p, "title", `^An?`); got == nil {
			t.Fatalf("Regex returned nil")
		}
	})

	t.Run("InsensitiveRegex_NotNil", func(t *testing.T) {
		if got := proc.InsensitiveRegex(s, p, "title", `^An?`); got == nil {
			t.Fatalf("InsensitiveRegex returned nil")
		}
	})
}

func TestProcessor_ProcessDispatcher(t *testing.T) {
	s := newSelector()
	p := sql.P()
	proc := Processor{}

	cases := []struct {
		op    paginationV1.Operator
		field string
		value string
		want  bool // want non-nil
	}{
		{paginationV1.Operator_EQ, "name", "tom", true},
		{paginationV1.Operator_IN, "name", `["a","b"]`, true},
		{paginationV1.Operator_BETWEEN, "created_at", `["2020-01-01","2021-01-01"]`, true},
		{paginationV1.Operator_IS_NULL, "deleted_at", "", true},
		{paginationV1.Operator_SEARCH, "any", "", true},
	}

	for _, c := range cases {
		got := proc.Process(s, p, c.op, c.field, c.value, nil)
		if (got != nil) != c.want {
			t.Fatalf("Process(op=%v) returned nil=%v, want non-nil=%v", c.op, got == nil, c.want)
		}
	}
}

func TestProcessor_DatePartAndJsonbHelpers(t *testing.T) {
	s := newSelector()
	p := sql.P()
	proc := Processor{}

	t.Run("DatePartField_NotEmpty", func(t *testing.T) {
		if got := proc.DatePartField(s, "year", "created_at"); got == "" {
			t.Fatalf("DatePartField returned empty string")
		}
	})

	t.Run("DatePart_NotNil", func(t *testing.T) {
		if got := proc.DatePart(s, p, "month", "created_at"); got == nil {
			t.Fatalf("DatePart returned nil")
		}
	})

	t.Run("Jsonb_NotNil", func(t *testing.T) {
		if got := proc.Jsonb(s, p, "daily_email", "preferences"); got == nil {
			t.Fatalf("Jsonb returned nil")
		}
	})

	t.Run("JsonbField_NotEmpty", func(t *testing.T) {
		if got := proc.JsonbField(s, "daily_email", "preferences"); got == "" {
			t.Fatalf("JsonbField returned empty string")
		}
	})
}
