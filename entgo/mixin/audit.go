package mixin

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"entgo.io/ent"

	"github.com/tx7do/go-crud/audit"
	"github.com/tx7do/go-crud/viewer"
)

type Audit struct {
	ent.Schema
}

// Hooks 审计日志核心逻辑
func (Audit) Hooks() []ent.Hook {
	return []ent.Hook{
		func(next ent.Mutator) ent.Mutator {
			return ent.MutateFunc(func(ctx context.Context, m ent.Mutation) (ent.Value, error) {
				op := m.Op()

				// 只审计 写操作
				if !op.Is(ent.OpCreate | ent.OpUpdate | ent.OpUpdateOne | ent.OpDelete | ent.OpDeleteOne) {
					return next.Mutate(ctx, m)
				}

				// 获取操作者信息（Viewer）
				vc, exist := viewer.FromContext(ctx)
				if !exist || !vc.ShouldAudit() {
					return next.Mutate(ctx, m)
				}

				// TODO: 若需记录 PreValue（旧值），在这里使用 ent client 从 DB 查询并保存

				// 执行数据库变更
				value, err := next.Mutate(ctx, m)
				if err != nil {
					return nil, err
				}

				entry := &audit.Entry{
					TraceID:  vc.TraceID(),
					TenantID: vc.TenantID(),
					UserID:   vc.UserID(),

					Timestamp: time.Now(),

					Service:   "",
					Module:    "",
					Action:    "",
					Resource:  m.Type(),
					Operation: audit.Operation(m.Op().String()),
					Status:    audit.StatusOK,
					CostMS:    0,
				}

				entry.TargetID = extractTargetID(value)

				postData := buildPostDataFromValue(value)
				if len(postData) == 0 {
					postData = getPostValue(m)
				}
				if len(postData) > 0 {
					if b, err := json.Marshal(postData); err == nil {
						entry.PostValue = append([]byte(nil), b...)
					}
				}

				ac, ok := audit.FromContext(ctx)
				if !ok || ac == nil {
					log.Printf("[Audit][WARN] missing AuditContext, Trace=%s, User=%d, Resource=%s", vc.TraceID(), vc.UserID(), m.Type())
					return value, nil
				}

				eCopy := *entry
				if eCopy.PostValue != nil {
					eCopy.PostValue = append([]byte(nil), eCopy.PostValue...)
				}

				// 异步写入日志
				go func(a audit.Auditor, e audit.Entry) {
					if recErr := a.Record(context.Background(), &e); recErr != nil {
						log.Printf("[Audit][ERROR] record failed: %v", recErr)
					}
				}(ac, eCopy)

				return value, nil
			})
		},
	}
}

// getPostValue 提取变更后的字段值
func getPostValue(m ent.Mutation) map[string]any {
	changes := make(map[string]any)
	fields := m.Fields()
	for _, f := range fields {
		if val, ok := m.Field(f); ok {
			// 脱敏逻辑：敏感字段不记录明文
			if isSensitiveField(f) {
				changes[f] = "********"
			} else {
				changes[f] = val
			}
		}
	}
	return changes
}

// buildPostDataFromValue 尝试将返回实体序列化为 map[string]any，供 PostValue 使用
func buildPostDataFromValue(value any) map[string]any {
	if value == nil {
		return nil
	}

	typ := reflect.TypeOf(value)
	kind := typ.Kind()
	// 若是指针则取 Elem 的 Kind 做基本类型判断
	if kind == reflect.Ptr {
		if typ.Elem() != nil {
			kind = typ.Elem().Kind()
		}
	}
	// 基本类型直接跳过
	if (kind >= reflect.Int && kind <= reflect.Float64) || kind == reflect.String || kind == reflect.Bool {
		return nil
	}

	b, err := json.Marshal(value)
	if err != nil {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil
	}

	// 脱敏敏感字段
	for k := range m {
		if isSensitiveField(k) {
			m[k] = "********"
		}
	}
	return m
}

// extractTargetID 尝试从返回值中提取 ID（支持方法 ID()、字段 ID/Id），否则回退到 fmt.Sprintf
func extractTargetID(value any) string {
	if value == nil {
		return ""
	}
	val := reflect.ValueOf(value)
	if !val.IsValid() {
		return ""
	}

	// 尝试以方法优先（支持指针与非指针接收者）
	tryMethod := func(rv reflect.Value) (string, bool) {
		if !rv.IsValid() {
			return "", false
		}
		m := rv.MethodByName("ID")
		if m.IsValid() && m.Type().NumIn() == 0 && m.Type().NumOut() == 1 {
			out := m.Call(nil)
			if len(out) == 1 {
				return fmt.Sprintf("%v", out[0].Interface()), true
			}
		}
		return "", false
	}

	// 尝试 pointer 方法
	if res, ok := tryMethod(val); ok {
		return res
	}
	// 若是指针则用 Elem 尝试
	if val.Kind() == reflect.Ptr && !val.IsNil() {
		elem := val.Elem()
		if res, ok := tryMethod(elem); ok {
			return res
		}
		// 再尝试字段读取
		if elem.IsValid() && elem.Kind() == reflect.Struct {
			if f := elem.FieldByName("ID"); f.IsValid() && f.CanInterface() {
				return fmt.Sprintf("%v", f.Interface())
			}
			if f := elem.FieldByName("Id"); f.IsValid() && f.CanInterface() {
				return fmt.Sprintf("%v", f.Interface())
			}
		}
	} else {
		// 非指针情况：尝试字段读取
		if val.IsValid() && val.Kind() == reflect.Struct {
			if f := val.FieldByName("ID"); f.IsValid() && f.CanInterface() {
				return fmt.Sprintf("%v", f.Interface())
			}
			if f := val.FieldByName("Id"); f.IsValid() && f.CanInterface() {
				return fmt.Sprintf("%v", f.Interface())
			}
		}
	}

	// 最后回退为字符串表示
	return fmt.Sprintf("%v", value)
}

var sensitiveFieldNames = map[string]struct{}{
	// 凭据类
	"password":    {},
	"secret":      {},
	"token":       {},
	"api_key":     {},
	"access_key":  {},
	"secret_key":  {},
	"private_key": {},
	"salt":        {},
	"session_id":  {},
	"auth_code":   {},

	// 个人隐私类 (PII)
	"id_card":        {},
	"id_number":      {},
	"phone":          {},
	"mobile":         {},
	"bank_card":      {},
	"card_number":    {},
	"cvv":            {},
	"address_detail": {},
}

// AddSensitiveField 添加敏感字段名
func AddSensitiveField(f string) {
	sensitiveFieldNames[f] = struct{}{}
}

// isSensitiveField 检查字段名是否为敏感字段
func isSensitiveField(f string) bool {
	f = strings.ToLower(f)
	for name := range sensitiveFieldNames {
		if strings.Contains(f, name) { // 匹配如 "user_password", "app_secret"
			return true
		}
	}
	return false
}
