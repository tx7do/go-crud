package gorm

import (
	"context"
	"fmt"
	"reflect"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/schema"

	"github.com/tx7do/go-crud/viewer"
)

// RegisterTenantCallbacks 注册租户隔离强制回调，覆盖 Query/Update/Delete
// 三个路径。语义与 entgo TenantPrivacy.EvalQuery 一致：
//   - 缺 ViewerContext → AddError 中止（fail-closed）
//   - 平台/系统视图 → 放行（不注入谓词）
//   - 租户业务视图 → 注入 tenant_id = ? 谓词
//
// 仅对 schema 含 tenant_id 列的实体生效（FieldsByDBName 检测，等同 entgo
// 的 mixin opt-in）。非租户实体不受影响。
//
// 注意：gorm Query 回调统一覆盖 SELECT/UPDATE/DELETE（Get/Exists/Count
// 均经此路径），故三回调逻辑相同，仅注册位置不同。
func RegisterTenantCallbacks(db *gorm.DB) error {
	q := db.Callback().Query().Before("gorm:query")
	if err := q.Register("tenant:query", tenantScopeCallback); err != nil {
		return fmt.Errorf("register tenant query callback: %w", err)
	}
	u := db.Callback().Update().Before("gorm:before_update")
	if err := u.Register("tenant:update", tenantScopeCallback); err != nil {
		return fmt.Errorf("register tenant update callback: %w", err)
	}
	d := db.Callback().Delete().Before("gorm:before_delete")
	if err := d.Register("tenant:delete", tenantScopeCallback); err != nil {
		return fmt.Errorf("register tenant delete callback: %w", err)
	}
	return nil
}

// tenantScopeCallback 是 Query/Update/Delete 共用的租户谓词注入回调。
// 在 gorm:query / gorm:before_update / gorm:before_delete 之前执行，
// 通过 AddClause 注入 WHERE tenant_id = ?（参考 gorm 自身主键谓词注入：
// callbacks/query.go:51-56 的 clause.Eq + AddClause 模式）。
func tenantScopeCallback(db *gorm.DB) {
	if db == nil || db.Statement == nil || db.Statement.Schema == nil {
		return
	}
	// 仅对声明 tenant_id 列的实体生效（mixin opt-in 检测）
	tf, ok := db.Statement.Schema.FieldsByDBName["tenant_id"]
	if !ok {
		return
	}

	dec, err := viewer.EnforceTenant(db.Statement.Context)
	if err != nil {
		// fail-closed：缺身份直接中止，而非静默放行
		_ = db.AddError(err)
		return
	}
	if !dec.Enforce {
		// 平台/系统视图放行
		return
	}

	// 注入 tenant_id = ? 谓词（参数化，与 gorm 主键注入同构）
	db.Statement.AddClause(clause.Where{
		Exprs: []clause.Expression{
			clause.Eq{
				Column: clause.Column{Table: db.Statement.Table, Name: tf.DBName},
				Value:  dec.TenantID,
			},
		},
	})
}

// SetTenantIDOnCreateBeforeHook 是 Create 路径的强制 set 入口，供
// TenantID mixin 的 BeforeCreate 调用。语义同 entgo EvalMutation Create：
// 租户业务视图强制覆盖 tenant_id，平台/系统视图尊重显式设置，缺身份
// fail-closed 报错。
func SetTenantIDOnCreateBeforeHook(tx *gorm.DB) error {
	if tx == nil || tx.Statement == nil || tx.Statement.Schema == nil {
		return nil
	}
	return setTenantIDOnCreate(tx.Statement.Context, tx.Statement.Schema, tx.Statement.ReflectValue)
}

func setTenantIDOnCreate(ctx context.Context, sch *schema.Schema, reflectValue reflect.Value) error {
	if sch == nil {
		return nil
	}
	tf, ok := sch.FieldsByDBName["tenant_id"]
	if !ok {
		return nil
	}
	dec, err := viewer.EnforceTenant(ctx)
	if err != nil {
		return err
	}
	if !dec.Enforce {
		return nil
	}
	// 强制覆盖：普通用户无法为自己指定他租户
	tid := uint32(dec.TenantID)
	return tf.Set(ctx, reflectValue, tid)
}
