package entgo

import (
	"context"

	"entgo.io/ent/dialect/sql"
)

type QueryBuilder[ENT_QUERY any, ENT_SELECT any, ENTITY any] interface {
	Modify(modifiers ...func(s *sql.Selector)) *ENT_SELECT

	Clone() *ENT_QUERY

	All(ctx context.Context) ([]*ENTITY, error)

	Only(ctx context.Context) (*ENTITY, error)

	Count(ctx context.Context) (int, error)

	Select(fields ...string) *ENT_SELECT

	Exist(ctx context.Context) (bool, error)
}

type ListBuilder[ENT_QUERY any, ENT_SELECT any, ENTITY any] interface {
	Modify(modifiers ...func(s *sql.Selector)) *ENT_SELECT

	Clone() *ENT_QUERY

	All(ctx context.Context) ([]*ENTITY, error)

	Count(ctx context.Context) (int, error)

	Offset(offset int) *ENT_QUERY
	Limit(limit int) *ENT_QUERY
}

type SelectBuilder[ENT_SELECT any, ENTITY any] interface {
	Modify(modifiers ...func(s *sql.Selector)) *ENT_SELECT

	Clone() SelectBuilder[ENT_SELECT, ENTITY]

	All(ctx context.Context) ([]*ENTITY, error)

	Only(ctx context.Context) (*ENTITY, error)

	Count(ctx context.Context) (int, error)

	Select(fields ...string) *ENT_SELECT

	Exist(ctx context.Context) (bool, error)
}

type CreateBuilder[ENTITY any] interface {
	Exec(ctx context.Context) error

	ExecX(ctx context.Context)

	Save(ctx context.Context) (*ENTITY, error)

	SaveX(ctx context.Context) *ENTITY
}

type CreateBulkBuilder[ENT_CREATE_BULK any, ENTITY any] interface {
	Exec(ctx context.Context) error

	ExecX(ctx context.Context)

	Save(ctx context.Context) ([]*ENTITY, error)

	SaveX(ctx context.Context) []*ENTITY
}

type UpdateBuilder[ENT_UPDATE any, PREDICATE any] interface {
	Exec(ctx context.Context) error

	ExecX(ctx context.Context)

	Save(ctx context.Context) (int, error)

	SaveX(ctx context.Context) int

	Where(ps ...PREDICATE) *ENT_UPDATE

	Modify(modifiers ...func(u *sql.UpdateBuilder)) *ENT_UPDATE
}

type UpdateOneBuilder[ENT_UPDATE_ONE any, PREDICATE any, ENTITY any] interface {
	Modify(modifiers ...func(u *sql.UpdateBuilder)) *ENT_UPDATE_ONE

	Save(ctx context.Context) (*ENTITY, error)
	SaveX(ctx context.Context) *ENTITY

	Exec(ctx context.Context) error
	ExecX(ctx context.Context)

	Where(ps ...PREDICATE) *ENT_UPDATE_ONE
}

type DeleteBuilder[ENT_DELETE any, PREDICATE any] interface {
	Exec(ctx context.Context) (int, error)

	ExecX(ctx context.Context) int

	Where(ps ...PREDICATE) *ENT_DELETE
}
