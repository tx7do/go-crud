package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/schema/field"
	"github.com/tx7do/go-crud/entgo/mixin"
)

// Menu holds the schema definition for the Menu entity.
type Menu struct {
	ent.Schema
}

// Fields of the Menu.
func (Menu) Fields() []ent.Field {
	return []ent.Field{
		field.String("name").
			NotEmpty().
			Comment("menu name"),
	}
}

func (Menu) Mixin() []ent.Mixin {
	return []ent.Mixin{
		mixin.AutoIncrementId{},
		mixin.Tree[Menu]{},
		mixin.TreePath{},
	}
}
