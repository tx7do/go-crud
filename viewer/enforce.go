package viewer

import (
	"context"
	"errors"
	"reflect"
)

// ErrMissingViewer 在租户隔离实体操作时上下文缺少 ViewerContext 返回。
// 与 entgo TenantPrivacy.EvalQuery/EvalMutation 的 fail-closed 行为一致：
// 缺身份一律拒绝，而非静默放行（否则租户过滤悄悄消失）。
var ErrMissingViewer = errors.New("security: missing ViewerContext in context")

// EnforcementDecision 是 EnforceTenant 的返回值，封装三态决策，避免调用方
// 重复从 Context 读取并自行分支（5 个模块共享同一语义）。
type EnforcementDecision struct {
	// Enforce 为 true 表示当前为租户业务视图，调用方必须注入 tenant_id 谓词
	// 或在 Create 上强制 SetTenantID。
	Enforce bool
	// TenantID 仅在 Enforce==true 时有意义，为当前 Viewer 的租户 ID。
	TenantID uint64
}

// EnforceTenant 是所有租户隔离实体共享的强制决策入口，语义与 entgo
// TenantPrivacy 一致：
//   - 缺 ViewerContext → (Enforce=false, err=ErrMissingViewer) fail-closed
//   - 平台/系统视图（IsPlatformContext || IsSystemContext）→ (Enforce=false, err=nil) pass-through
//   - 租户业务视图（tenant_id > 0）→ (Enforce=true, TenantID=vc.TenantID()) 注入谓词/强制 set
//
// 此函数不触碰查询/变更对象本身，仅给出决策；具体注入方式由各模块按其
// ORM/查询构造器实现（gorm clause、ent predicate、mongo bson、SQL where）。
func EnforceTenant(ctx context.Context) (EnforcementDecision, error) {
	vc, exist := FromContext(ctx)
	if !exist {
		return EnforcementDecision{}, ErrMissingViewer
	}
	if vc.IsPlatformContext() || vc.IsSystemContext() {
		return EnforcementDecision{}, nil
	}
	return EnforcementDecision{Enforce: true, TenantID: vc.TenantID()}, nil
}

// ScopedModel 是租户隔离实体的 opt-in 标记接口。实体嵌入对应模块的
// TenantID mixin 时即实现该接口，repository 通过类型断言识别（无需反射）。
// gorm/entgo 因有其各自的 schema/mixin 检测机制，不依赖此接口。
type ScopedModel interface {
	GetTenantID() *uint32
	SetTenantID(uint32)
}

var scopedModelType = reflect.TypeOf((*ScopedModel)(nil)).Elem()

// IsTenantScopedType 在类型层检测 ENTITY 是否实现了 ScopedModel 接口
// （即嵌入了对应模块的 TenantID mixin）。用于泛型 repository 的读路径，
// 这些路径只有 query builder 而无实体实例，无法做接口断言。
// 通过 reflect.TypeOf((*T)(nil)) 取类型（不实例化），检查 Implements。
// 非 tenant-scoped 实体返回 false，repository 跳过强制。
func IsTenantScopedType[T any]() bool {
	return reflect.TypeOf((*T)(nil)).Implements(scopedModelType)
}

// EnforceOnScopedInstance 在实体实例上做租户强制：若实例实现 ScopedModel
// （即 tenant-scoped），按 EnforceTenant 决策 SetTenantID（Create 路径）。
// 非 scoped 实体直接返回 nil。用于 Create/BatchCreate 的实例级强制。
func EnforceOnScopedInstance[T any](ctx context.Context, instance *T) error {
	if instance == nil {
		return nil
	}
	sm, ok := any(instance).(ScopedModel)
	if !ok {
		return nil
	}
	dec, err := EnforceTenant(ctx)
	if err != nil {
		return err
	}
	if !dec.Enforce {
		return nil
	}
	sm.SetTenantID(uint32(dec.TenantID))
	return nil
}

// EnforceOnScopedInstanceAny 是 EnforceOnScopedInstance 的非泛型版本，接收
// any 实例。用于通过反射获取的指针（如批量插入中 slice 元素的地址），这些
// 场景无法在编译期确定 *T 类型，故无法使用泛型版。
// 语义完全一致：ScopedModel 断言 → EnforceTenant 决策 → SetTenantID。
// 非 scoped 实例（断言失败）直接返回 nil。
func EnforceOnScopedInstanceAny(ctx context.Context, instance any) error {
	if instance == nil {
		return nil
	}
	sm, ok := instance.(ScopedModel)
	if !ok {
		return nil
	}
	dec, err := EnforceTenant(ctx)
	if err != nil {
		return err
	}
	if !dec.Enforce {
		return nil
	}
	sm.SetTenantID(uint32(dec.TenantID))
	return nil
}
