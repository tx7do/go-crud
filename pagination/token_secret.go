package pagination

import "sync"

// tokenSecret 是分页游标 token 的 HMAC-SHA256 签名密钥（进程级全局配置）。
// 为空（默认）时保持旧的无签名 token 行为，兼容存量系统。
var (
	tokenSecretMu sync.RWMutex
	tokenSecret   []byte
)

// SetTokenSecret 设置分页游标 token 的签名密钥。
//
// 设置后：
//   - EncodeAndSign 产出带 v2. 前缀的签名 token；
//   - VerifyAndDecode 拒绝一切未签名、伪造或被篡改的 token（旧式 token 视为迁移期输入，同样拒绝）。
//
// 建议：在进程启动时调用一次；密钥应来自配置中心/环境变量等受控来源，
// 至少 32 字节高熵随机值；轮换密钥会使存量 token 全部失效（客户端重新从第一页开始）。
func SetTokenSecret(secret []byte) {
	tokenSecretMu.Lock()
	defer tokenSecretMu.Unlock()
	if secret == nil {
		tokenSecret = nil
		return
	}
	tokenSecret = make([]byte, len(secret))
	copy(tokenSecret, secret)
}

// TokenSecret 返回当前生效的 token 签名密钥（未设置时为 nil）。
func TokenSecret() []byte {
	tokenSecretMu.RLock()
	defer tokenSecretMu.RUnlock()
	return tokenSecret
}
