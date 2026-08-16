package pagination

import "testing"

func TestTokenSecret_RoundTripWithSecret(t *testing.T) {
	secret := []byte("test-secret-0123456789abcdef")
	SetTokenSecret(secret)
	defer SetTokenSecret(nil)

	tok := EncodeAndSign(42, TokenSecret())
	if !IsSignedToken(tok) {
		t.Fatalf("expected signed token, got %q", tok)
	}
	lastID, ok := VerifyAndDecode(tok, TokenSecret())
	if !ok || lastID != 42 {
		t.Fatalf("verify failed: ok=%v lastID=%d", ok, lastID)
	}
}

func TestTokenSecret_TamperRejected(t *testing.T) {
	SetTokenSecret([]byte("test-secret-0123456789abcdef"))
	defer SetTokenSecret(nil)

	tok := EncodeAndSign(7, TokenSecret())
	tampered := tok[:len(tok)-1] + "0" // 篡改签名末位
	if _, ok := VerifyAndDecode(tampered, TokenSecret()); ok {
		t.Fatal("tampered token must be rejected")
	}

	// 未签名旧式 token 在设置 secret 后必须被拒绝（迁移期策略）
	unsigned := EncodeAndSign(7, nil)
	if _, ok := VerifyAndDecode(unsigned, TokenSecret()); ok {
		t.Fatal("unsigned token must be rejected when secret is set")
	}

	// 伪造签名 token 在无 secret 时必须被拒绝
	if _, ok := VerifyAndDecode(tok, nil); ok {
		t.Fatal("signed token must be rejected when secret is empty")
	}
}

func TestTokenSecret_SecretIsCopied(t *testing.T) {
	orig := []byte("secret-material-123456")
	SetTokenSecret(orig)
	defer SetTokenSecret(nil)

	orig[0] = 'X' // 外部修改不得影响内部密钥
	got := TokenSecret()
	if got == nil || got[0] == 'X' {
		t.Fatal("SetTokenSecret must copy the secret bytes")
	}
}
