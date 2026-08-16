package paginator

// DefaultMaxLimit 是每页条数的默认上限（对应 pagination.proto 中 page_size 的建议上限）。
const DefaultMaxLimit = 100

// MaxLimit 是构造函数与 WithLimit/WithSize 允许的每页条数上限，超出会被截断到该值。
// 这是防止客户端通过超大 page_size/limit 造成 DB/内存压力（DoS）的兜底防线。
// 个别服务确需更大页长时，可在启动时调大，例如：
//
//	paginator.MaxLimit = 500
//
// 设为 0 或负数表示不限制（不推荐）。
var MaxLimit = DefaultMaxLimit

// DefaultNoPagingMaxLimit 是 no_paging=true（不分页）查询的默认行数兜底上限。
const DefaultNoPagingMaxLimit = 10000

// NoPagingMaxLimit 是 no_paging=true 查询的行数兜底上限。
// no_paging 是客户端可设置的 proto 字段，若完全跳过 LIMIT 会构成 DoS 向量，
// 因此默认仍然加一个宽松的上限；需要真正全量导出的服务可将其调大或设为 0 关闭。
var NoPagingMaxLimit = DefaultNoPagingMaxLimit

// clampLimit 将 limit 规整到 [1, MaxLimit] 区间（MaxLimit <= 0 时仅保留下限）。
func clampLimit(limit int) int {
	if limit < 1 {
		return 1
	}
	if MaxLimit > 0 && limit > MaxLimit {
		return MaxLimit
	}
	return limit
}
