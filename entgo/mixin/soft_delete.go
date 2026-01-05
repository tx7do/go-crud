package mixin

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/mixin"
)

var _ ent.Mixin = (*SoftDelete)(nil)

type SoftDelete struct {
	mixin.Schema
}

func (SoftDelete) Fields() []ent.Field {
	var fields []ent.Field
	fields = append(fields, DeletedAt{}.Fields()...)
	fields = append(fields, DeletedBy{}.Fields()...)
	return fields
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var _ ent.Mixin = (*SoftDelete64)(nil)

type SoftDelete64 struct {
	mixin.Schema
}

func (SoftDelete64) Fields() []ent.Field {
	var fields []ent.Field
	fields = append(fields, DeletedAt{}.Fields()...)
	fields = append(fields, DeletedBy64{}.Fields()...)
	return fields
}
