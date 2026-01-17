package audit

import "context"

type contextKey struct{}

// WithAuditor 将 Auditor 实例注入 context
func WithAuditor(ctx context.Context, a Auditor) context.Context {
	return context.WithValue(ctx, contextKey{}, a)
}

// FromContext 从 context 中提取 Context
func FromContext(ctx context.Context) (Auditor, bool) {
	v := ctx.Value(contextKey{})
	vc, ok := v.(Auditor)
	return vc, ok
}

// MustFromContext 从 context 中提取 Auditor，若不存在则返回一个默认的空实现
func MustFromContext(ctx context.Context) Auditor {
	if ctx == nil {
		return NewNoopAuditor()
	}

	if a, ok := ctx.Value(contextKey{}).(Auditor); ok {
		return a
	}
	return NewNoopAuditor() // 提供一个默认的空实现
}
