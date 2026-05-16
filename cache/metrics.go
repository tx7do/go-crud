package cache

// MetricsCollector 缓存组件的度量接口
type MetricsCollector interface {
	// IncRequestsTotal 记录请求总数，包括成功和失败的请求
	IncRequestsTotal(entity string, status string)

	// ObserveLoaderDuration 观察加载器执行时长，单位为秒
	ObserveLoaderDuration(entity string, duration float64)
}

type nopMetrics struct{}

func (n *nopMetrics) IncRequestsTotal(_ string, _ string)       {}
func (n *nopMetrics) ObserveLoaderDuration(_ string, _ float64) {}
