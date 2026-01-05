package mixin

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/mixin"

	"github.com/tx7do/go-utils/id"
)

// 确保 SnowflakeId 实现了 ent.Mixin 接口
var _ ent.Mixin = (*SnowflakeId)(nil)

type SnowflakeId struct {
	mixin.Schema
}

func (SnowflakeId) Fields() []ent.Field {
	return []ent.Field{
		field.Uint64("id").
			Comment("id").
			DefaultFunc(id.GenerateSonyflakeID).
			Positive().
			Immutable().
			SchemaType(map[string]string{
				dialect.MySQL:    "bigint unsigned",
				dialect.Postgres: "bigint",
			}),
	}
}
