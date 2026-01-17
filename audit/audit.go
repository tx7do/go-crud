package audit

import (
	"context"
)

// Auditor 负责记录和管理审计日志的生命周期
type Auditor interface {
	// Record 方法是同步调用（由调用者负责传入 context 和 entry）
	// Auditor 内部决定是异步缓冲还是同步写入
	Record(ctx context.Context, entry *Entry) error

	// Flush 确保所有待处理的日志都被提交到最终存储
	// 应该在应用程序关闭、测试结束或需要强制持久化时调用
	Flush(ctx context.Context) error
}

// NopAuditor 是一个不执行任何操作的 Auditor，用于默认情况或测试
type NopAuditor struct{}

func (*NopAuditor) Record(_ context.Context, _ *Entry) error { return nil }
func (*NopAuditor) Flush(_ context.Context) error            { return nil }
