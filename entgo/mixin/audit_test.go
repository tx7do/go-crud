package mixin

import (
	"testing"
)

// TestRedactNested 验证嵌套结构中的敏感字段被递归脱敏
// （此前仅顶层键脱敏，{"profile":{"phone":"138..."}} 会明文入库）。
func TestRedactNested(t *testing.T) {
	data := map[string]any{
		"name": "bob",
		"profile": map[string]any{
			"phone": "13800138000",
			"email": "bob@example.com",
			"info":  map[string]any{"ssn": "123-45-6789", "note": "x st"},
			"tags":  []any{"a", "b"},
		},
		"history": []any{
			map[string]any{"password": "hunter2", "note": "ok"},
		},
	}

	out := redactNested(data).(map[string]any)
	profile := out["profile"].(map[string]any)
	if profile["phone"] != "********" {
		t.Errorf("nested phone must be redacted, got %v", profile["phone"])
	}
	if profile["email"] != "********" {
		t.Errorf("nested email must be redacted, got %v", profile["email"])
	}
	info := profile["info"].(map[string]any)
	if info["ssn"] != "********" {
		t.Errorf("nested info.ssn must be redacted, got %v", info["ssn"])
	}
	if info["note"] != "x st" {
		t.Errorf("non-sensitive nested value must be preserved, got %v", info["note"])
	}
	history := out["history"].([]any)
	first := history[0].(map[string]any)
	if first["password"] != "********" {
		t.Errorf("slice-nested password must be redacted, got %v", first["password"])
	}
	if first["note"] != "ok" {
		t.Errorf("non-sensitive value must be preserved, got %v", first["note"])
	}
	if profile["tags"].([]any)[0] != "a" {
		t.Errorf("non-sensitive slice values must be preserved")
	}
}

// TestIsSensitiveField 验证敏感词表覆盖常见 PII 字段。
func TestIsSensitiveField(t *testing.T) {
	for _, f := range []string{"password", "user_email", "ssn", "birthday", "home_address", "passwd"} {
		if !isSensitiveField(f) {
			t.Errorf("field %q must be sensitive", f)
		}
	}
	for _, f := range []string{"name", "status", "created_at"} {
		if isSensitiveField(f) {
			t.Errorf("field %q must not be sensitive", f)
		}
	}
}
