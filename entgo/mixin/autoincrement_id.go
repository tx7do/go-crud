package mixin

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/mixin"
)

var _ ent.Mixin = (*AutoIncrementId)(nil)

type AutoIncrementId struct{ mixin.Schema }

func (AutoIncrementId) Fields() []ent.Field {
	return []ent.Field{
		field.Uint32("id").
			Comment("id").
			Nillable().
			Immutable().
			Positive(),
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var _ ent.Mixin = (*AutoIncrementId64)(nil)

type AutoIncrementId64 struct{ mixin.Schema }

func (AutoIncrementId64) Fields() []ent.Field {
	return []ent.Field{
		field.Uint64("id").
			Comment("id").
			Nillable().
			Immutable().
			Positive(),
	}
}
