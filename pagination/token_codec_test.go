package pagination

import (
	"testing"
)

// TestTokenCodec_RoundTrip 验证签名 token 的编码-校验往返。
func TestTokenCodec_RoundTrip(t *testing.T) {
	secret := []byte("test-secret-32-bytes-long-xxxxxxx")
	lastID := int64(42)

	tok := EncodeAndSign(lastID, secret)
	if tok == "" {
		t.Fatal("EncodeAndSign returned empty")
	}
	if !IsSignedToken(tok) {
		t.Fatalf("expected signed token, got %q", tok)
	}

	got, ok := VerifyAndDecode(tok, secret)
	if !ok {
		t.Fatalf("VerifyAndDecode rejected valid token")
	}
	if got != lastID {
		t.Fatalf("expected %d, got %d", lastID, got)
	}
}

// TestTokenCodec_TamperedRejected 验证签名 token 被篡改后校验失败。
func TestTokenCodec_TamperedRejected(t *testing.T) {
	secret := []byte("test-secret-32-bytes-long-xxxxxxx")
	tok := EncodeAndSign(100, secret)

	// 篡改 last_id 段：把 base64 载荷里的第一个字符替换。
	body := tok[3:] // 去掉 "v2."
	dot := indexByte(body, '.')
	if dot < 0 {
		t.Fatal("malformed token")
	}
	tampered := "v2." + flip(body[:dot]) + body[dot:]
	if _, ok := VerifyAndDecode(tampered, secret); ok {
		t.Fatal("expected tampered token to be rejected")
	}
}

// TestTokenCodec_WrongSecretRejected 验证用错误 secret 校验签名 token 失败。
func TestTokenCodec_WrongSecretRejected(t *testing.T) {
	secret := []byte("test-secret-32-bytes-long-xxxxxxx")
	tok := EncodeAndSign(100, secret)
	if _, ok := VerifyAndDecode(tok, []byte("wrong-secret")); ok {
		t.Fatal("expected wrong secret to be rejected")
	}
}

// TestTokenCodec_SignedRejectedWithoutSecret 验证无 secret 时拒绝签名 token。
func TestTokenCodec_SignedRejectedWithoutSecret(t *testing.T) {
	secret := []byte("test-secret-32-bytes-long-xxxxxxx")
	tok := EncodeAndSign(100, secret)
	if _, ok := VerifyAndDecode(tok, nil); ok {
		t.Fatal("expected signed token to be rejected when secret is nil")
	}
}

// TestTokenCodec_LegacyCompat 验证旧式未签名 token 在无 secret 时的兼容解码。
func TestTokenCodec_LegacyCompat(t *testing.T) {
	// 旧式未签名 token：无 v2. 前缀，纯 base64(json)。
	legacy := EncodeAndSign(7, nil)
	if legacy == "" {
		t.Fatal("legacy encode returned empty")
	}
	if IsSignedToken(legacy) {
		t.Fatalf("expected legacy token to not be signed, got %q", legacy)
	}
	got, ok := VerifyAndDecode(legacy, nil)
	if !ok {
		t.Fatal("expected legacy token to decode in compat mode")
	}
	if got != 7 {
		t.Fatalf("expected 7, got %d", got)
	}
}

// TestTokenCodec_LegacyRejectedWhenSecretSet 验证迁移期（secret 非空）拒绝旧 token。
func TestTokenCodec_LegacyRejectedWhenSecretSet(t *testing.T) {
	legacy := EncodeAndSign(7, nil)
	if _, ok := VerifyAndDecode(legacy, []byte("now-enforcing")); ok {
		t.Fatal("expected legacy token to be rejected when secret is set")
	}
}

// TestTokenCodec_EmptyRejected 验证空 token 始终拒绝。
func TestTokenCodec_EmptyRejected(t *testing.T) {
	if _, ok := VerifyAndDecode("", nil); ok {
		t.Fatal("expected empty token to be rejected")
	}
}

// flip 把 base64 载荷的首字符改成另一个字符以制造篡改。
func flip(s string) string {
	b := []byte(s)
	if len(b) == 0 {
		return s
	}
	b[0] = b[0] ^ 1
	return string(b)
}

// indexByte 返回字节 c 在 s 中首次出现的下标，不存在则返回 -1。
func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}
