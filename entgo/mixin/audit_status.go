package mixin

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/mixin"
)

// 确保 AuditStatus 实现了 ent.Mixin 接口
var _ ent.Mixin = (*AuditStatus)(nil)

type AuditStatus struct{ mixin.Schema }

func (AuditStatus) Fields() []ent.Field {
	return []ent.Field{
		field.Enum("audit_status").
			Comment("审核状态").
			Nillable().
			Default("PENDING").
			NamedValues(
				"Pending", "PENDING",
				"Approved", "APPROVED",
				"Rejected", "REJECTED",
			),
	}
}
