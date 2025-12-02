package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
)

// User holds the schema definition for the User entity.
type User struct {
	ent.Schema
}

// Fields of the User.
func (User) Fields() []ent.Field {
	return []ent.Field{
		// ent 默认会生成主键 id（自增 int），可在应用层将其映射到 proto 的 uint32 id
		field.String("name").
			NotEmpty().
			Comment("user name"),
		field.Uint32("age").
			Default(0).
			Comment("user age"),
	}
}

// Edges of the User.
func (User) Edges() []ent.Edge {
	return nil
}
