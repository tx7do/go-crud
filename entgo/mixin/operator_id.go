package mixin

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/mixin"
)

// 确保 CreatorId 实现了 ent.Mixin 接口
var _ ent.Mixin = (*CreatorId)(nil)

type CreatorId struct {
	mixin.Schema
}

func (CreatorId) Fields() []ent.Field {
	return []ent.Field{
		field.Uint32("creator_id").
			Comment("创建者用户ID").
			Immutable().
			Optional().
			Nillable(),
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var _ ent.Mixin = (*CreateBy)(nil)

type CreateBy struct{ mixin.Schema }

func (CreateBy) Fields() []ent.Field {
	return []ent.Field{
		field.Uint32("create_by").
			Comment("创建者ID").
			Optional().
			Nillable(),
	}
}

var _ ent.Mixin = (*CreateBy64)(nil)

type CreateBy64 struct{ mixin.Schema }

func (CreateBy64) Fields() []ent.Field {
	return []ent.Field{
		field.Uint64("create_by").
			Comment("创建者ID").
			Optional().
			Nillable(),
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var _ ent.Mixin = (*UpdateBy)(nil)

type UpdateBy struct{ mixin.Schema }

func (UpdateBy) Fields() []ent.Field {
	return []ent.Field{
		field.Uint32("update_by").
			Comment("更新者ID").
			Optional().
			Nillable(),
	}
}

var _ ent.Mixin = (*UpdateBy64)(nil)

type UpdateBy64 struct{ mixin.Schema }

func (UpdateBy64) Fields() []ent.Field {
	return []ent.Field{
		field.Uint64("update_by").
			Comment("更新者ID").
			Optional().
			Nillable(),
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var _ ent.Mixin = (*DeleteBy)(nil)

type DeleteBy struct{ mixin.Schema }

func (DeleteBy) Fields() []ent.Field {
	return []ent.Field{
		field.Uint32("delete_by").
			Comment("删除者ID").
			Optional().
			Nillable(),
	}
}

var _ ent.Mixin = (*DeleteBy64)(nil)

type DeleteBy64 struct{ mixin.Schema }

func (DeleteBy64) Fields() []ent.Field {
	return []ent.Field{
		field.Uint64("delete_by").
			Comment("删除者ID").
			Optional().
			Nillable(),
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var _ ent.Mixin = (*CreatedBy)(nil)

type CreatedBy struct{ mixin.Schema }

func (CreatedBy) Fields() []ent.Field {
	return []ent.Field{
		field.Uint32("created_by").
			Comment("创建者ID").
			Optional().
			Nillable(),
	}
}

var _ ent.Mixin = (*CreatedBy64)(nil)

type CreatedBy64 struct{ mixin.Schema }

func (CreatedBy64) Fields() []ent.Field {
	return []ent.Field{
		field.Uint64("created_by").
			Comment("创建者ID").
			Optional().
			Nillable(),
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var _ ent.Mixin = (*UpdatedBy)(nil)

type UpdatedBy struct{ mixin.Schema }

func (UpdatedBy) Fields() []ent.Field {
	return []ent.Field{
		field.Uint32("updated_by").
			Comment("更新者ID").
			Optional().
			Nillable(),
	}
}

var _ ent.Mixin = (*UpdatedBy64)(nil)

type UpdatedBy64 struct{ mixin.Schema }

func (UpdatedBy64) Fields() []ent.Field {
	return []ent.Field{
		field.Uint64("updated_by").
			Comment("更新者ID").
			Optional().
			Nillable(),
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var _ ent.Mixin = (*DeletedBy)(nil)

type DeletedBy struct{ mixin.Schema }

func (DeletedBy) Fields() []ent.Field {
	return []ent.Field{
		field.Uint32("deleted_by").
			Comment("删除者ID").
			Optional().
			Nillable(),
	}
}

var _ ent.Mixin = (*DeletedBy64)(nil)

type DeletedBy64 struct{ mixin.Schema }

func (DeletedBy64) Fields() []ent.Field {
	return []ent.Field{
		field.Uint64("deleted_by").
			Comment("删除者ID").
			Optional().
			Nillable(),
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var _ ent.Mixin = (*OperatorID)(nil)

type OperatorID struct{ mixin.Schema }

func (OperatorID) Fields() []ent.Field {
	var fields []ent.Field
	fields = append(fields, CreatedBy{}.Fields()...)
	fields = append(fields, UpdatedBy{}.Fields()...)
	fields = append(fields, DeletedBy{}.Fields()...)
	return fields
}

var _ ent.Mixin = (*OperatorID64)(nil)

type OperatorID64 struct{ mixin.Schema }

func (OperatorID64) Fields() []ent.Field {
	var fields []ent.Field
	fields = append(fields, CreatedBy64{}.Fields()...)
	fields = append(fields, UpdatedBy64{}.Fields()...)
	fields = append(fields, DeletedBy64{}.Fields()...)
	return fields
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var _ ent.Mixin = (*AuditorID)(nil)

// AuditorID 包含创建者、更新者和删除者字段的 Mixin
type AuditorID struct{ mixin.Schema }

func (AuditorID) Fields() []ent.Field {
	var fields []ent.Field
	fields = append(fields, CreatedBy{}.Fields()...)
	fields = append(fields, UpdatedBy{}.Fields()...)
	fields = append(fields, DeletedBy{}.Fields()...)
	return fields
}

var _ ent.Mixin = (*AuditorID64)(nil)

// AuditorID 包含创建者、更新者和删除者字段的 Mixin
type AuditorID64 struct{ mixin.Schema }

func (AuditorID64) Fields() []ent.Field {
	var fields []ent.Field
	fields = append(fields, CreatedBy64{}.Fields()...)
	fields = append(fields, UpdatedBy64{}.Fields()...)
	fields = append(fields, DeletedBy64{}.Fields()...)
	return fields
}
