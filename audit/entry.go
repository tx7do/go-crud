package audit

import (
	"encoding/json"
	"time"
)

type Operation string

const (
	OpInsert Operation = "INSERT"
	OpUpdate Operation = "UPDATE"
	OpUpsert Operation = "UPSERT"
	OpDelete Operation = "DELETE"
)

type Status int

const (
	StatusOK   Status = 0
	StatusFail Status = 1
)

// Entry 审计日志条目
type Entry struct {
	// --- 基础上下文 (Base Context) ---
	TraceID   string    `json:"trace_id"`  // 全链路追踪 ID
	Timestamp time.Time `json:"timestamp"` // 发生时间（建议 UTC）

	// --- 操作者信息 (Viewer Data) ---
	UserID    uint64 `json:"user_id,omitempty"`    // 操作人 ID
	TenantID  uint64 `json:"tenant_id,omitempty"`  // 租户 ID
	Username  string `json:"username,omitempty"`   // 操作人账号名（冗余存储，防止用户删除后无法溯源）
	UserIP    string `json:"user_ip,omitempty"`    // 客户端 IP
	UserAgent string `json:"user_agent,omitempty"` // 客户端环境信息

	// --- 操作行为 (Action) ---
	Service  string `json:"service,omitempty"`  // 所属微服务名
	Module   string `json:"module,omitempty"`   // 业务模块（如：订单、用户、权限）
	Action   string `json:"action,omitempty"`   // 具体动作（如：Create, Update, Login, Export）
	Resource string `json:"resource,omitempty"` // 操作的资源对象（如：user_table, order_123）

	// --- 数据变更 (Data Changes) ---
	// 使用 JSON 字符串或 map 存储，记录“改了什么”
	Operation Operation       `json:"operation,omitempty"`  // 对应数据库操作：INSERT, UPDATE, DELETE
	TargetID  string          `json:"target_id,omitempty"`  // 被操作对象的 ID
	PreValue  json.RawMessage `json:"pre_value,omitempty"`  // 变更前的值（可选，敏感表建议开启）
	PostValue json.RawMessage `json:"post_value,omitempty"` // 变更后的值（New Value）

	// --- 结果状态 (Result) ---
	Status       Status `json:"status"`                  // 状态码：0-成功，1-失败
	ErrorMessage string `json:"error_message,omitempty"` // 失败原因（如果 Status != 0）
	CostMS       int64  `json:"cost_ms,omitempty"`       // 操作耗时（毫秒）

	// --- 扩展字段 (Metadata) ---
	// 存储 IsPlatformContext 或特定的 DataScope 信息
	Extra map[string]any `json:"extra,omitempty"`
}

func (e *Entry) SetPreValue(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	e.PreValue = b
	return nil
}

func (e *Entry) SetPostValue(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	e.PostValue = b
	return nil
}
