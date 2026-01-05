package mixin

import (
	"fmt"
	"net"

	"entgo.io/ent"
	"entgo.io/ent/dialect"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/mixin"
)

// 确保 IpAddress 实现了 ent.Mixin 接口
var _ ent.Mixin = (*IpAddress)(nil)

type IpAddress struct{ mixin.Schema }

func (IpAddress) Fields() []ent.Field {
	return []ent.Field{
		field.String("ip_address").
			Comment("IP 地址").
			Nillable().
			Optional().
			MaxLen(45).
			SchemaType(map[string]string{
				dialect.Postgres: "inet",
			}).
			Validate(func(s string) error {
				if s == "" {
					return nil
				}
				if net.ParseIP(s) == nil {
					return fmt.Errorf("invalid ip address: %s", s)
				}
				return nil
			}),
	}
}
