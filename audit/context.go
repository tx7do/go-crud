package audit

import "context"

type contextKey struct{}

// WithAuditor 将 Auditor 实例注入 context
func WithAuditor(ctx context.Context, a Auditor) context.Context {
	return context.WithValue(ctx, contextKey{}, a)
}

// FromContext 从 context 中提取 Auditor 实例
// 如果不存在，返回一个 NopAuditor（空实现），防止空指针恐慌
func FromContext(ctx context.Context) Auditor {
	if a, ok := ctx.Value(contextKey{}).(Auditor); ok {
		return a
	}
	return &NopAuditor{} // 提供一个默认的空实现
}
