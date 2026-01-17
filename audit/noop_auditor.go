package audit

import "context"

// noopAuditor 是一个不执行任何操作的 Auditor，用于默认情况或测试
type noopAuditor struct{}

func (*noopAuditor) Record(_ context.Context, _ *Entry) error { return nil }
func (*noopAuditor) Flush(_ context.Context) error            { return nil }

func NewNoopAuditor() Auditor {
	return &noopAuditor{}
}
