package influxdb

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
)

// normalizeQuery 消除多过滤条件因 map 遍历顺序带来的 AND 子句顺序抖动：
// 固定 SELECT...WHERE 前缀，仅对 WHERE 之后的条件做确定性重排。
func normalizeQuery(q string) string {
	idx := strings.Index(q, " WHERE ")
	if idx < 0 {
		return q
	}
	head, tail := q[:idx+len(" WHERE ")], q[idx+len(" WHERE "):]
	conds := strings.Split(tail, " AND ")
	sort.Strings(conds)
	return head + strings.Join(conds, " AND ")
}

// normalizeArgs 同理消除参数顺序抖动。
func normalizeArgs(args []any) []string {
	s := make([]string, len(args))
	for i, a := range args {
		s[i] = fmt.Sprint(a)
	}
	sort.Strings(s)
	return s
}

func TestBuildQuery(t *testing.T) {
	tests := []struct {
		name          string
		table         string
		filters       map[string]any
		operators     map[string]string
		fields        []string
		expectedQuery string
		expectedArgs  []any
	}{
		{
			name:          "Basic query with filters and fields",
			table:         "candles",
			filters:       map[string]any{"s": "AAPL", "o": 150.0},
			fields:        []string{"s", "o", "h", "l", "c", "v"},
			expectedQuery: "SELECT s, o, h, l, c, v FROM candles WHERE s = ? AND o = ?",
			expectedArgs:  []any{"AAPL", 150.0},
		},
		{
			name:          "Query with no filters",
			table:         "candles",
			filters:       map[string]any{},
			fields:        []string{"s", "o", "h"},
			expectedQuery: "SELECT s, o, h FROM candles",
			expectedArgs:  []any{},
		},
		{
			name:          "Query with no fields",
			table:         "candles",
			filters:       map[string]any{"s": "AAPL"},
			fields:        []string{},
			expectedQuery: "SELECT * FROM candles WHERE s = ?",
			expectedArgs:  []any{"AAPL"},
		},
		{
			name:          "Empty table name",
			table:         "",
			filters:       map[string]any{"s": "AAPL"},
			fields:        []string{"s", "o"},
			expectedQuery: "SELECT s, o FROM  WHERE s = ?",
			expectedArgs:  []any{"AAPL"},
		},
		{
			name:          "Special characters in filters",
			table:         "candles",
			filters:       map[string]any{"name": "O'Reilly"},
			fields:        []string{"name"},
			expectedQuery: "SELECT name FROM candles WHERE name = ?",
			expectedArgs:  []any{"O'Reilly"},
		},
		{
			name:          "Query with interval filters",
			table:         "candles",
			filters:       map[string]any{"time": "now() - interval '15 minutes'"},
			fields:        []string{"*"},
			operators:     map[string]string{"time": ">="},
			expectedQuery: "SELECT * FROM candles WHERE time >= ?",
			expectedArgs:  []any{"now() - interval '15 minutes'"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args := BuildQuery(tt.table, tt.filters, tt.operators, tt.fields)

			if normalizeQuery(query) != normalizeQuery(tt.expectedQuery) {
				t.Errorf("expected query %s, got %s", tt.expectedQuery, query)
			}

			if !reflect.DeepEqual(normalizeArgs(args), normalizeArgs(tt.expectedArgs)) {
				t.Errorf("expected args %v, got %v", tt.expectedArgs, args)
			}
		})
	}
}

func TestBuildQueryWithParams(t *testing.T) {
	tests := []struct {
		name          string
		table         string
		filters       map[string]any
		operators     map[string]string
		fields        []string
		expectedQuery string
	}{
		{
			name:          "Basic query with filters and fields",
			table:         "candles",
			filters:       map[string]any{"s": "'AAPL'", "o": 150.0},
			operators:     map[string]string{"o": ">"},
			fields:        []string{"s", "o", "h", "l", "c", "v"},
			expectedQuery: "SELECT s, o, h, l, c, v FROM candles WHERE s = 'AAPL' AND o > 150",
		},
		{
			name:          "Query with no filters",
			table:         "candles",
			filters:       map[string]any{},
			operators:     map[string]string{},
			fields:        []string{"s", "o", "h"},
			expectedQuery: "SELECT s, o, h FROM candles",
		},
		{
			name:          "Query with no fields",
			table:         "candles",
			filters:       map[string]any{"s": "'AAPL'"},
			operators:     map[string]string{},
			fields:        []string{},
			expectedQuery: "SELECT * FROM candles WHERE s = 'AAPL'",
		},
		{
			name:          "Empty table name",
			table:         "",
			filters:       map[string]any{"s": "'AAPL'"},
			operators:     map[string]string{},
			fields:        []string{"s", "o"},
			expectedQuery: "SELECT s, o FROM  WHERE s = 'AAPL'",
		},
		{
			name:          "Special characters in filters",
			table:         "candles",
			filters:       map[string]any{"name": "'O'Reilly'"},
			operators:     map[string]string{},
			fields:        []string{"name"},
			expectedQuery: "SELECT name FROM candles WHERE name = 'O'Reilly'",
		},
		{
			name:          "Query with interval filters",
			table:         "candles",
			filters:       map[string]any{"time": "now() - interval '15 minutes'"},
			operators:     map[string]string{"time": ">="},
			fields:        []string{"*"},
			expectedQuery: "SELECT * FROM candles WHERE time >= now() - interval '15 minutes'",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query := BuildQueryWithParams(tt.table, tt.filters, tt.operators, tt.fields)

			if normalizeQuery(query) != normalizeQuery(tt.expectedQuery) {
				t.Errorf("expected query %s, got %s", tt.expectedQuery, query)
			}
			//t.Log(query)
		})
	}
}

// 用 struct tag 标记字段角色：tag/field/time/measurement
type SensorData struct {
	// influx:"tag"：标记为 tag（必须是 string 类型）
	DeviceID string `influx:"tag" json:"device_id"`
	Location string `influx:"tag" json:"location"`
	// influx:"field"：标记为 field（支持多种类型）
	Temperature float64 `influx:"field" json:"temperature"`
	Humidity    float64 `influx:"field" json:"humidity"`
	Battery     int64   `influx:"field" json:"battery"`
	IsOnline    bool    `influx:"field" json:"is_online"`
	// influx:"time"：标记为时间戳（必须是 time.Time 类型）
	Timestamp time.Time `influx:"time" json:"timestamp"`
	// influx:"measurement"：可选，标记 measurement 名称（也可硬编码）
	// 若多个 struct 对应不同 measurement，可通过 tag 指定
	Measurement string `influx:"measurement" json:"-"` // json:"-" 忽略序列化
}

func TestStructToPoint_SensorData_Success(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Nanosecond)
	sd := &SensorData{
		Measurement: "sensors",
		DeviceID:    "dev-1",
		Location:    "room1",
		Temperature: 23.5,
		Humidity:    55.2,
		Battery:     95,
		IsOnline:    true,
		Timestamp:   now,
	}

	pt, err := StructToPoint(sd)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pt == nil {
		t.Fatalf("expected non-nil point")
	}

	// tag 检查（字段名会被转换为 snake_case）
	if v, ok := pt.GetTag("device_id"); !ok || v != "dev-1" {
		t.Fatalf("expected tag device_id=dev-1, got %v (ok=%v)", v, ok)
	}
	if v, ok := pt.GetTag("location"); !ok || v != "room1" {
		t.Fatalf("expected tag location=room1, got %v (ok=%v)", v, ok)
	}

	// field 检查
	if v := pt.GetField("temperature"); v == nil {
		t.Fatalf("expected field temperature present")
	} else if fv, ok := v.(float64); !ok || fv != 23.5 {
		t.Fatalf("expected temperature=23.5, got %v", v)
	}

	if v := pt.GetField("humidity"); v == nil {
		t.Fatalf("expected field humidity present")
	} else if fv, ok := v.(float64); !ok || fv != 55.2 {
		t.Fatalf("expected humidity=55.2, got %v", v)
	}

	if v := pt.GetField("battery"); v == nil {
		t.Fatalf("expected field battery present")
	} else if fv, ok := v.(int64); !ok || fv != int64(95) {
		t.Fatalf("expected battery=95 (int64), got %v (ok=%v)", v, ok)
	}

	if v := pt.GetField("is_online"); v == nil {
		t.Fatalf("expected field is_online present")
	} else if fv, ok := v.(bool); !ok || fv != true {
		t.Fatalf("expected is_online=true, got %v", v)
	}
}

func TestStructToPoint_SensorData_MissingMeasurement(t *testing.T) {
	sd := &SensorData{
		Measurement: "",
		DeviceID:    "dev-1",
		Location:    "room1",
		Temperature: 23.5,
		Humidity:    55.2,
		Battery:     95,
		IsOnline:    true,
		Timestamp:   time.Now(),
	}

	_, err := StructToPoint(sd)
	if err == nil {
		t.Fatalf("expected error when measurement is empty")
	}
}
