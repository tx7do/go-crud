package pagination

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
)

// signedTokenPrefix 是签名 token 的版本前缀，用于与旧的明文 token 区分。
const signedTokenPrefix = "v2."

// tokenCursor 是分页游标在序列化时的载荷结构。
type tokenCursor struct {
	LastID int64 `json:"last_id"`
}

// EncodeAndSign 将 lastID 序列化为带 HMAC 签名的分页游标 token。
// 格式: v2.<base64(json 载荷)>.<hex(hmac-sha256)>
// 当 secret 为空时，退化为不带签名的 base64(json) 形式（兼容旧 token），
// 不产生带 v2. 前缀的签名 token。
func EncodeAndSign(lastID int64, secret []byte) string {
	payload, err := json.Marshal(tokenCursor{LastID: lastID})
	if err != nil {
		return ""
	}
	encoded := base64.StdEncoding.EncodeToString(payload)
	if len(secret) == 0 {
		// 无 secret：返回旧式未签名 token，供测试/兼容使用。
		return encoded
	}
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(encoded))
	sig := hex.EncodeToString(mac.Sum(nil))
	return signedTokenPrefix + encoded + "." + sig
}

// VerifyAndDecode 校验并解码分页游标 token。
//   - 对 v2. 前缀的签名 token：校验 HMAC，通过则返回 (lastID, true)，
//     不通过则返回 (0, false)。当 secret 为空时一律拒绝签名 token。
//   - 对旧式未签名 token（无 v2. 前缀）：仅在 secret 为空时解码并返回
//     (lastID, true)，保持向后兼容；当 secret 非空时视为迁移期，拒绝旧 token。
func VerifyAndDecode(token string, secret []byte) (int64, bool) {
	if token == "" {
		return 0, false
	}

	// 签名 token 路径。
	if IsSignedToken(token) {
		if len(secret) == 0 {
			return 0, false
		}
		body := strings.TrimPrefix(token, signedTokenPrefix)
		dot := strings.LastIndex(body, ".")
		if dot < 0 {
			return 0, false
		}
		encoded := body[:dot]
		sigHex := body[dot+1:]

		mac := hmac.New(sha256.New, secret)
		mac.Write([]byte(encoded))
		expectedSig := hex.EncodeToString(mac.Sum(nil))
		if !hmac.Equal([]byte(sigHex), []byte(expectedSig)) {
			return 0, false
		}
		return decodeCursor(encoded)
	}

	// 旧式未签名 token：迁移期（secret 非空）拒绝，否则兼容解码。
	if len(secret) != 0 {
		return 0, false
	}
	return decodeCursor(token)
}

// IsSignedToken 判断 token 是否为 v2. 前缀的签名 token。
func IsSignedToken(token string) bool {
	return strings.HasPrefix(token, signedTokenPrefix)
}

// decodeCursor 对 base64 编码的载荷做 JSON 解码，返回 lastID。
func decodeCursor(encoded string) (int64, bool) {
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return 0, false
	}
	var c tokenCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return 0, false
	}
	return c.LastID, true
}

// ErrInvalidToken 是 VerifyAndDecode 失败时可供上层判断的哨兵错误（当前未直接返回，
// 保留以便后续接入校验失败时的错误传播）。
var ErrInvalidToken = errors.New("invalid or tampered pagination token")
