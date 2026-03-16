package clickhouse

import (
	"database/sql"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Ptr returns a pointer to the provided value.
func Ptr[T any](v T) *T {
	return &v
}

func TestStructToValueArrayWithCandle(t *testing.T) {
	now := time.Now()

	candle := Candle{
		Timestamp: Ptr(now),
		Symbol:    Ptr("AAPL"),
		Open:      Ptr(100.5),
		High:      Ptr(105.0),
		Low:       Ptr(99.5),
		Close:     Ptr(102.0),
		Volume:    Ptr(1500.0),
	}

	values := structToValueArray(candle)
	assert.NotNil(t, values, "Values should not be nil")
	assert.Len(t, values, 7, "Expected 7 fields in the Candle struct")
	assert.Equal(t, now.Format(timeFormat), values[0].(string), "Timestamp should match")
	assert.Equal(t, *candle.Symbol, *(values[1].(*string)), "Symbol should match")
	assert.Equal(t, *candle.Open, *values[2].(*float64), "Open price should match")
	assert.Equal(t, *candle.High, *values[3].(*float64), "High price should match")
	assert.Equal(t, *candle.Low, *values[4].(*float64), "Low price should match")
	assert.Equal(t, *candle.Close, *values[5].(*float64), "Close price should match")
	assert.Equal(t, *candle.Volume, *values[6].(*float64), "Volume should match")

	t.Logf("QueryRow Result: [%v] Candle: %s, Open: %f, High: %f, Low: %f, Close: %f, Volume: %f\n",
		values[0],
		*(values[1].(*string)),
		*values[2].(*float64),
		*values[3].(*float64),
		*values[4].(*float64),
		*values[5].(*float64),
		*values[6].(*float64),
	)
}

func TestStructToValueArrayWithCandlePtr(t *testing.T) {
	now := time.Now()

	candle := &Candle{
		Timestamp: Ptr(now),
		Symbol:    Ptr("AAPL"),
		Open:      Ptr(100.5),
		High:      Ptr(105.0),
		Low:       Ptr(99.5),
		Close:     Ptr(102.0),
		Volume:    Ptr(1500.0),
	}

	values := structToValueArray(candle)
	assert.NotNil(t, values, "Values should not be nil")
	assert.Len(t, values, 7, "Expected 7 fields in the Candle struct")
	assert.Equal(t, now.Format(timeFormat), values[0].(string), "Timestamp should match")
	assert.Equal(t, *candle.Symbol, *(values[1].(*string)), "Symbol should match")
	assert.Equal(t, *candle.Open, *values[2].(*float64), "Open price should match")
	assert.Equal(t, *candle.High, *values[3].(*float64), "High price should match")
	assert.Equal(t, *candle.Low, *values[4].(*float64), "Low price should match")
	assert.Equal(t, *candle.Close, *values[5].(*float64), "Close price should match")
	assert.Equal(t, *candle.Volume, *values[6].(*float64), "Volume should match")

	t.Logf("QueryRow Result: [%v] Candle: %s, Open: %f, High: %f, Low: %f, Close: %f, Volume: %f\n",
		values[0],
		*(values[1].(*string)),
		*values[2].(*float64),
		*values[3].(*float64),
		*values[4].(*float64),
		*values[5].(*float64),
		*values[6].(*float64),
	)
}

func TestStructToValueArrayVarious(t *testing.T) {
	// prepare values
	nsVal := sql.NullString{String: "hello", Valid: true}
	nsNil := &sql.NullString{Valid: false}

	iVal := sql.NullInt64{Int64: 123, Valid: true}
	iNil := &sql.NullInt64{Valid: false}

	fVal := sql.NullFloat64{Float64: 1.23, Valid: true}
	bVal := sql.NullBool{Bool: true, Valid: true}

	tt := time.Date(2021, 1, 2, 3, 4, 5, 0, time.UTC)
	ptt := tt

	pb := []byte{0x1, 0x2, 0x3}

	sl := []int{1, 2, 3}

	ns := struct{ Name string }{Name: "nested"}

	tsPtr := timestamppb.New(tt)
	tsVal := *timestamppb.New(tt)

	durPtr := durationpb.New(5 * time.Second)
	durVal := *durationpb.New(5 * time.Second)

	pintVal := 42

	// build test struct (note: includes an unexported field)
	s := struct {
		SS             sql.NullString
		SP             *sql.NullString
		SI             sql.NullInt64
		SIP            *sql.NullInt64
		SF             sql.NullFloat64
		SB             sql.NullBool
		ST             sql.NullTime
		TT             time.Time
		PTT            *time.Time
		PB             []byte
		SL             []int
		NS             struct{ Name string }
		Tsv            timestamppb.Timestamp
		Tsp            *timestamppb.Timestamp
		Dv             durationpb.Duration
		Dp             *durationpb.Duration
		unexportedSome string
		PInt           *int
	}{
		SS:  nsVal,
		SP:  nsNil,
		SI:  iVal,
		SIP: iNil,
		SF:  fVal,
		SB:  bVal,
		ST:  sql.NullTime{Time: tt, Valid: true},
		TT:  tt,
		PTT: &ptt,
		PB:  pb,
		SL:  sl,
		NS:  ns,
		Tsv: tsVal,
		Tsp: tsPtr,
		Dv:  durVal,
		Dp:  durPtr,
		// unexportedSome left as zero value (should produce nil in output)
		PInt: &pintVal,
	}

	out := structToValueArray(s)

	// expected length equals number of fields
	if len(out) != 18 {
		t.Fatalf("expected 18 values, got %d: %#v", len(out), out)
	}

	// helper for comparing JSONized fields
	mustJSON := func(v any) string {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("json marshal failed: %v", err)
		}
		return string(b)
	}

	checks := []struct {
		idx int
		exp any
	}{
		{0, "hello"},                            // SS
		{1, nil},                                // SP (invalid)
		{2, int64(123)},                         // SI
		{3, nil},                                // SIP (invalid)
		{4, 1.23},                               // SF
		{5, true},                               // SB
		{6, tt.Format(timeFormat)},              // ST
		{7, tt.Format(timeFormat)},              // TT
		{8, tt.Format(timeFormat)},              // PTT
		{9, pb},                                 // PB []byte
		{10, mustJSON(sl)},                      // SL -> json string
		{11, mustJSON(ns)},                      // NS -> json string
		{12, tsVal.AsTime().Format(timeFormat)}, // Tsv (value)
		{13, tsPtr.AsTime().Format(timeFormat)}, // Tsp (ptr)
		{14, durVal.AsDuration().String()},      // Dv
		{15, durPtr.AsDuration().String()},      // Dp
		{16, nil},                               // unexportedSome -> nil
		{17, 42},                                // PInt (pointer to int)
	}

	for _, c := range checks {
		if c.exp == nil {
			if out[c.idx] != nil {
				t.Errorf("index %d expected nil, got %#v", c.idx, out[c.idx])
			}
			continue
		}

		switch want := c.exp.(type) {
		case string:
			// could be JSON string or time string
			gotStr, ok := out[c.idx].(string)
			if !ok {
				t.Errorf("index %d expected string %q, got %#v", c.idx, want, out[c.idx])
				continue
			}
			// If expected is JSON string (starts with '[' or '{'), compare by unmarshalling
			if len(want) > 0 && (want[0] == '[' || want[0] == '{') {
				var gm any
				if err := json.Unmarshal([]byte(gotStr), &gm); err != nil {
					t.Errorf("index %d expected json, got invalid json: %v", c.idx, err)
					continue
				}
				var wm any
				if err := json.Unmarshal([]byte(want), &wm); err != nil {
					t.Errorf("internal test error unmarshal want json: %v", err)
					continue
				}
				if !reflect.DeepEqual(gm, wm) {
					t.Errorf("index %d json mismatch: want %s got %s", c.idx, want, gotStr)
				}
				continue
			}
			if gotStr != want {
				t.Errorf("index %d mismatch: want %q got %q", c.idx, want, gotStr)
			}
		case []byte:
			gotBytes, ok := out[c.idx].([]byte)
			if !ok {
				t.Errorf("index %d expected []byte, got %#v", c.idx, out[c.idx])
				continue
			}
			if !reflect.DeepEqual(gotBytes, want) {
				t.Errorf("index %d bytes mismatch: want %v got %v", c.idx, want, gotBytes)
			}
		case int:
			// numeric types might come back as int64 (from reflect); coerce
			if reflect.DeepEqual(out[c.idx], want) {
				continue
			}
			if iv, ok := out[c.idx].(int64); ok {
				if int(iv) != want {
					t.Errorf("index %d mismatch: want %v got %v", c.idx, want, out[c.idx])
				}
				continue
			}
			t.Errorf("index %d mismatch: want %v got %v", c.idx, want, out[c.idx])
		case int64, float64, bool:
			if !reflect.DeepEqual(out[c.idx], want) {
				t.Errorf("index %d mismatch: want %v got %v", c.idx, want, out[c.idx])
			}
		default:
			if !reflect.DeepEqual(out[c.idx], want) {
				t.Errorf("index %d mismatch: want %v got %v", c.idx, want, out[c.idx])
			}
		}
	}
}
