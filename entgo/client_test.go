package entgo

import (
	"context"
	"fmt"
	"testing"

	"github.com/tx7do/go-crud/entgo/ent/menu"
	"github.com/tx7do/go-crud/entgo/ent/migrate"
	"github.com/tx7do/go-crud/viewer"
	_ "github.com/xiaoqidun/entps"

	"github.com/tx7do/go-crud/entgo/ent"
	_ "github.com/tx7do/go-crud/entgo/ent/runtime"
)

func createTestEntClient(t *testing.T) *EntClient[*ent.Client] {
	drv, err := CreateDriver(
		"sqlite3",
		"file:ent?mode=memory&cache=shared&_fk=1",
		false, false,
	)
	if err != nil {
		t.Fatalf("failed opening connection to db: %v", err)
	}

	_ = drv

	db := ent.NewClient(
		ent.Driver(drv),
		ent.Log(func(a ...any) {
			t.Log(a...)
		}),
	)

	if err = db.Schema.Create(t.Context(), migrate.WithForeignKeys(true)); err != nil {
		t.Fatalf("failed creating schema resources: %v", err)
	}

	wrapperClient := NewEntClient(db, drv)

	return wrapperClient
}

func TestEntClient_Close(t *testing.T) {
	cli := createTestEntClient(t)
	defer cli.Close()
}

func setMenuPath(ctx context.Context, cli *EntClient[*ent.Client], entity *ent.Menu) (err error) {
	var parentPath string
	if entity.ParentID != nil {
		fmt.Println("setMenuPath entity:", entity.ParentID)

		var parentEntity *ent.Menu
		parentEntity, err = cli.Client().Debug().Menu.Query().
			Where(
				menu.IDEQ(*entity.ParentID),
			).
			Select(menu.FieldPath).
			Only(ctx)
		if err != nil {
			return err
		} else {
			if parentEntity.Path != nil {
				parentPath = *parentEntity.Path
			}
		}
	}
	err = cli.Client().Debug().Menu.UpdateOneID(entity.ID).
		SetPath(ComputeTreePath(parentPath, entity.ID)).
		Exec(ctx)

	return err
}

func TestEntClient_Menu(t *testing.T) {
	cli := createTestEntClient(t)
	defer cli.Close()

	entity, err := cli.Client().Debug().Menu.Create().
		SetName("test").
		Save(t.Context())
	if err != nil {
		t.Fatalf("failed creating menu: %v", err)
	}
	t.Logf("created menu: %v", entity)
	setMenuPath(t.Context(), cli, entity)

	entity, err = cli.Client().Debug().Menu.Create().
		SetName("test1").
		SetParentID(entity.ID).
		Save(t.Context())
	t.Logf("created menu: %v", entity)
	setMenuPath(t.Context(), cli, entity)

	builder := cli.Client().Debug().Menu.Create()
	entity, err = builder.
		SetName("test2").
		SetParentID(entity.ID).
		Save(t.Context())
	t.Logf("created menu: %v", entity)
	setMenuPath(t.Context(), cli, entity)

	entities, err := cli.Client().Debug().Menu.Query().All(t.Context())
	if err != nil {
		t.Fatalf("failed querying menus: %v", err)
	}
	t.Logf("queried menus: %v", entities)

	//builder = cli.Client().Debug().Menu.Create()
	ids := builder.Mutation().ParentIDs()
	t.Logf("parent ids: %v", ids)

	var parentEntity *ent.Menu
	parentEntity, err = cli.Client().Debug().Menu.QueryParent(entity).Only(t.Context())
	if err != nil {
		t.Fatalf("failed querying parent menu: %v", err)
	}
	t.Logf("parent menu: %v", parentEntity)

	//cli.Client().Debug().Menu.Query().Que
}

type testContext struct{}

func (testContext) UserID() uint64                 { return 0 }
func (testContext) TenantID() uint64               { return 1 }
func (testContext) OrgUnitID() uint64              { return 0 }
func (testContext) Permissions() []string          { return nil }
func (testContext) Roles() []string                { return nil }
func (testContext) DataScope() []viewer.DataScope  { return nil }
func (testContext) TraceID() string                { return "" }
func (testContext) HasPermission(_, _ string) bool { return false }
func (testContext) IsPlatformContext() bool        { return false }
func (testContext) IsTenantContext() bool          { return false }
func (testContext) IsSystemContext() bool          { return false }
func (testContext) ShouldAudit() bool              { return false }

func TestEntClient_Tenant(t *testing.T) {
	cli := createTestEntClient(t)
	defer cli.Close()

	//cli.Client().Intercept(interceptor.TenantInterceptor())

	ctx := viewer.WithContext(t.Context(), testContext{})

	entity, err := cli.Client().User.Create().SetName("test").Save(ctx)
	if err != nil {
		t.Fatalf("failed creating user: %v", err)
	}
	t.Logf("created user: %v", entity)

	builder := cli.Client().Debug().User.Query()
	var entities []*ent.User
	entities, err = builder.Where().All(ctx)
	if err != nil {
		t.Logf("query user error: %v", err)
	} else {
		t.Logf("queried users: %v", entities)
	}
}
