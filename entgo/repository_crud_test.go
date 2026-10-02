package entgo

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"entgo.io/ent/dialect/sql"

	"github.com/redis/go-redis/v9"
	"github.com/tx7do/go-utils/mapper"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/entgo/ent"
	"github.com/tx7do/go-crud/entgo/ent/menu"
	"github.com/tx7do/go-crud/entgo/ent/migrate"
	"github.com/tx7do/go-crud/entgo/ent/predicate"
	"github.com/tx7do/go-crud/pagination"
	"github.com/tx7do/go-crud/viewer"
	_ "github.com/xiaoqidun/entps"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// ---------------------------------------------------------------------------
// Isolated in-memory sqlite client (unique DSN per test so tests never share
// state through the "ent" shared-cache DB used by createTestEntClient).
// ---------------------------------------------------------------------------

func newIsolatedTestEntClient(t *testing.T) *EntClient[*ent.Client] {
	t.Helper()
	dsn := fmt.Sprintf("file:repo_crud_%s?mode=memory&cache=shared&_fk=1",
		strings.ReplaceAll(t.Name(), "/", "_"))

	drv, err := CreateDriver("sqlite3", dsn, false, false)
	if err != nil {
		t.Fatalf("failed opening connection to db: %v", err)
	}

	db := ent.NewClient(ent.Driver(drv))
	if err := db.Schema.Create(context.Background(), migrate.WithForeignKeys(true)); err != nil {
		t.Fatalf("failed creating schema resources: %v", err)
	}

	wrapper := NewEntClient(db, drv)
	t.Cleanup(func() { _ = wrapper.Close() })
	return wrapper
}

func seedMenus(t *testing.T, cli *EntClient[*ent.Client], names ...string) []*ent.Menu {
	t.Helper()
	menus := make([]*ent.Menu, 0, len(names))
	for _, n := range names {
		m, err := cli.Client().Menu.Create().SetName(n).Save(context.Background())
		if err != nil {
			t.Fatalf("seed menu %q: %v", n, err)
		}
		menus = append(menus, m)
	}
	return menus
}

// ---------------------------------------------------------------------------
// A proto.Message DTO for the write paths. The entgo repository asserts that
// DTOs are proto.Message before applying field masks; the module has no
// generated user proto (user.proto is not compiled), so the test builds an
// equivalent message descriptor at runtime and wraps it in a value struct
// whose getter methods double as copier accessors for ToEntity.
// ---------------------------------------------------------------------------

var testUserMessageType = func() protoreflect.MessageDescriptor {
	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("repo_crud_test_user.proto"),
		Package: proto.String("wind.plugins.repo_crud_test"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: proto.String("TestUser"),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("id"), Number: proto.Int32(1), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_UINT32.Enum()},
				{Name: proto.String("name"), Number: proto.Int32(2), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
				{Name: proto.String("age"), Number: proto.Int32(3), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_UINT32.Enum()},
				{Name: proto.String("path"), Number: proto.Int32(4), Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
			},
		}},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		panic(err)
	}
	return fd.Messages().Get(0)
}()

type testUserDTO struct {
	msg *dynamicpb.Message
}

func newTestUserDTO(name string) *testUserDTO {
	m := dynamicpb.NewMessage(testUserMessageType)
	if name != "" {
		m.Set(m.Descriptor().Fields().ByName("name"), protoreflect.ValueOfString(name))
	}
	return &testUserDTO{msg: m}
}

func (d testUserDTO) ProtoReflect() protoreflect.Message { return d.msg }

func (d testUserDTO) field(name string) protoreflect.Value {
	if d.msg == nil {
		return protoreflect.Value{}
	}
	fd := d.msg.Descriptor().Fields().ByName(protoreflect.Name(name))
	if fd == nil || !d.msg.Has(fd) {
		return protoreflect.Value{}
	}
	return d.msg.Get(fd)
}

func (d testUserDTO) Id() uint32 {
	if v := d.field("id"); v.IsValid() {
		return uint32(v.Uint())
	}
	return 0
}

func (d testUserDTO) Name() string {
	if v := d.field("name"); v.IsValid() {
		return v.String()
	}
	return ""
}

func (d testUserDTO) Age() uint32 {
	if v := d.field("age"); v.IsValid() {
		return uint32(v.Uint())
	}
	return 0
}

func (d testUserDTO) Path() string {
	if v := d.field("path"); v.IsValid() {
		return v.String()
	}
	return ""
}

// GetId matches the IDGetter shape used by extractIDFromDTO.
func (d testUserDTO) GetId() int64 {
	return int64(d.Id())
}

func newProtoMenuRepository(t *testing.T) *Repository[
	ent.MenuQuery, ent.MenuSelect,
	ent.MenuCreate, ent.MenuCreateBulk,
	ent.MenuUpdate, ent.MenuUpdateOne,
	ent.MenuDelete,
	predicate.Menu, testUserDTO, ent.Menu,
] {
	t.Helper()
	return NewRepository[
		ent.MenuQuery, ent.MenuSelect,
		ent.MenuCreate, ent.MenuCreateBulk,
		ent.MenuUpdate, ent.MenuUpdateOne,
		ent.MenuDelete,
		predicate.Menu,
	](mapper.NewCopierMapper[testUserDTO, ent.Menu]())
}

func mustMask(paths ...string) *fieldmaskpb.FieldMask {
	return &fieldmaskpb.FieldMask{Paths: paths}
}

// ---------------------------------------------------------------------------
// Count / Exists / Get / Only against the in-memory database
// ---------------------------------------------------------------------------

func TestRepositoryCountExists_WithDB(t *testing.T) {
	cli := newIsolatedTestEntClient(t)
	repo := newMenuRepository(t)
	ctx := context.Background()

	seedMenus(t, cli, "n1", "n2", "n1-dup")

	count, err := repo.Count(ctx, cli.Client().Menu.Query())
	if err != nil {
		t.Fatalf("count failed: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected count 3, got %d", count)
	}

	// predicates flow through Modify
	namePredicate := func(s *sql.Selector) {
		s.Where(sql.EQ(s.C(menu.FieldName), "n1"))
	}
	count, err = repo.Count(ctx, cli.Client().Menu.Query(), namePredicate)
	if err != nil {
		t.Fatalf("count with predicate failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected count 1 with predicate, got %d", count)
	}

	exists, err := repo.Exists(ctx, cli.Client().Menu.Query())
	if err != nil || !exists {
		t.Fatalf("exists = %v, %v; want true, nil", exists, err)
	}

	exists, err = repo.Exists(ctx, cli.Client().Menu.Query().Where(menu.NameEQ("missing")))
	if err != nil || exists {
		t.Fatalf("exists = %v, %v; want false, nil", exists, err)
	}

	if _, err := repo.Exists(ctx, nil); err == nil {
		t.Fatal("Exists with nil builder must fail")
	}
}

func TestRepositoryGetOnly_WithDB(t *testing.T) {
	cli := newIsolatedTestEntClient(t)
	repo := newMenuRepository(t)
	ctx := context.Background()

	seeded := seedMenus(t, cli, "target", "other")

	dto, err := repo.Get(ctx, cli.Client().Menu.Query().Where(menu.IDEQ(seeded[0].ID)), nil)
	if err != nil {
		t.Fatalf("get failed: %v", err)
	}
	if dto == nil || dto.Name != "target" {
		t.Fatalf("unexpected dto: %+v", dto)
	}

	// Only is an alias of Get
	dto, err = repo.Only(ctx, cli.Client().Menu.Query().Where(menu.IDEQ(seeded[0].ID)), nil)
	if err != nil || dto == nil || dto.Name != "target" {
		t.Fatalf("Only = %+v, %v", dto, err)
	}

	// viewMask restricts the selected columns
	mask := &fieldmaskpb.FieldMask{Paths: []string{"name"}}
	dto, err = repo.Get(ctx, cli.Client().Menu.Query().Where(menu.IDEQ(seeded[0].ID)), mask)
	if err != nil {
		t.Fatalf("get with viewMask failed: %v", err)
	}
	if dto == nil || dto.Name != "target" {
		t.Fatalf("unexpected dto with viewMask: %+v", dto)
	}

	// missing row surfaces the builder error
	_, err = repo.Get(ctx, cli.Client().Menu.Query().Where(menu.NameEQ("missing")), nil)
	if err == nil || !ent.IsNotFound(err) {
		t.Fatalf("expected NotFound error, got %v", err)
	}

	if _, err := repo.Get(ctx, nil, nil); err == nil {
		t.Fatal("Get with nil builder must fail")
	}
}

// ---------------------------------------------------------------------------
// ListWithPaging / BuildListSelectorWithPaging
// ---------------------------------------------------------------------------

func TestRepositoryListWithPaging_Errors(t *testing.T) {
	repo := newMenuRepository(t)
	ctx := context.Background()

	if _, err := repo.ListWithPaging(ctx, nil, nil, nil); err == nil {
		t.Fatal("nil request must fail")
	}
	if _, err := repo.ListWithPaging(ctx, nil, nil, &paginationV1.PagingRequest{}); err == nil {
		t.Fatal("nil builder must fail")
	}
	if _, err := repo.ListTreeWithPaging(ctx, nil, nil, nil); err == nil {
		t.Fatal("nil request must fail (tree)")
	}
	if _, err := repo.ListTreeWithPaging(ctx, nil, nil, &paginationV1.PagingRequest{}); err == nil {
		t.Fatal("nil builder must fail (tree)")
	}
}

func TestRepositoryListWithPaging_PageBased(t *testing.T) {
	cli := newIsolatedTestEntClient(t)
	repo := newMenuRepository(t)
	ctx := context.Background()

	seedMenus(t, cli, "n1", "n2", "n3", "n4", "n5")

	req := &paginationV1.PagingRequest{
		Page:     proto.Uint32(2),
		PageSize: proto.Uint32(2),
		Sorting: []*paginationV1.Sorting{
			{Field: "name", Direction: paginationV1.Sorting_DESC},
		},
	}
	listQuery := cli.Client().Menu.Query()
	res, err := repo.ListWithPaging(ctx, listQuery, listQuery.Clone(), req)
	if err != nil {
		t.Fatalf("ListWithPaging failed: %v", err)
	}
	if res.Total != 5 {
		t.Fatalf("expected total 5, got %d", res.Total)
	}
	if len(res.Items) != 2 {
		t.Fatalf("expected 2 items on page 2, got %d", len(res.Items))
	}
	if res.Items[0].Name != "n3" || res.Items[1].Name != "n2" {
		t.Fatalf("unexpected page window: %q, %q", res.Items[0].Name, res.Items[1].Name)
	}

	// countBuilder == nil keeps Total at zero but still returns items
	res, err = repo.ListWithPaging(ctx, cli.Client().Menu.Query(), nil, req)
	if err != nil {
		t.Fatalf("ListWithPaging without count builder failed: %v", err)
	}
	if res.Total != 0 || len(res.Items) != 2 {
		t.Fatalf("unexpected result: total=%d items=%d", res.Total, len(res.Items))
	}
}

func TestRepositoryListWithPaging_OffsetTokenNoPaging(t *testing.T) {
	cli := newIsolatedTestEntClient(t)
	repo := newMenuRepository(t)
	ctx := context.Background()

	seedMenus(t, cli, "o1", "o2", "o3", "o4")

	// offset/limit window
	req := &paginationV1.PagingRequest{
		Offset: proto.Uint64(1),
		Limit:  proto.Uint32(2),
		Sorting: []*paginationV1.Sorting{
			{Field: "name", Direction: paginationV1.Sorting_ASC},
		},
	}
	res, err := repo.ListWithPaging(ctx, cli.Client().Menu.Query(), cli.Client().Menu.Query(), req)
	if err != nil {
		t.Fatalf("offset paging failed: %v", err)
	}
	if res.Total != 4 || len(res.Items) != 2 || res.Items[0].Name != "o2" {
		t.Fatalf("unexpected offset page: total=%d n=%d first=%v", res.Total, len(res.Items), res.Items[0].Name)
	}

	// token based cursor
	token := pagination.EncodeAndSign(2, pagination.TokenSecret())
	req = &paginationV1.PagingRequest{
		Token:  proto.String(token),
		Offset: proto.Uint64(2),
		Sorting: []*paginationV1.Sorting{
			{Field: "name", Direction: paginationV1.Sorting_ASC},
		},
	}
	res, err = repo.ListWithPaging(ctx, cli.Client().Menu.Query(), cli.Client().Menu.Query(), req)
	if err != nil {
		t.Fatalf("token paging failed: %v", err)
	}
	if len(res.Items) != 2 || res.Items[0].Name != "o3" {
		t.Fatalf("unexpected token page: n=%d first=%v", len(res.Items), res.Items[0].Name)
	}

	// no_paging applies the server-side row cap instead of unbounded reads
	req = &paginationV1.PagingRequest{NoPaging: proto.Bool(true)}
	res, err = repo.ListWithPaging(ctx, cli.Client().Menu.Query(), cli.Client().Menu.Query(), req)
	if err != nil {
		t.Fatalf("no paging failed: %v", err)
	}
	if res.Total != 4 || len(res.Items) != 4 {
		t.Fatalf("unexpected no-paging result: total=%d n=%d", res.Total, len(res.Items))
	}
}

func TestRepositoryListWithPaging_FilterAndFieldMask(t *testing.T) {
	cli := newIsolatedTestEntClient(t)
	repo := newMenuRepository(t)
	ctx := context.Background()

	seedMenus(t, cli, "f1", "f2", "keep")

	req := &paginationV1.PagingRequest{
		Page:     proto.Uint32(1),
		PageSize: proto.Uint32(10),
		FilteringType: &paginationV1.PagingRequest_FilterExpr{FilterExpr: &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_OR,
			Conditions: []*paginationV1.FilterCondition{
				{Field: "name", ValueOneof: &paginationV1.FilterCondition_Value{Value: "f1"}, Op: paginationV1.Operator_EQ},
				{Field: "name", ValueOneof: &paginationV1.FilterCondition_Value{Value: "f2"}, Op: paginationV1.Operator_EQ},
			},
		}},
		FieldMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
	}
	res, err := repo.ListWithPaging(ctx, cli.Client().Menu.Query(), cli.Client().Menu.Query(), req)
	if err != nil {
		t.Fatalf("filtered list failed: %v", err)
	}
	if res.Total != 2 || len(res.Items) != 2 {
		t.Fatalf("unexpected filtered result: total=%d n=%d", res.Total, len(res.Items))
	}
	for _, item := range res.Items {
		if item.Name != "f1" && item.Name != "f2" {
			t.Fatalf("unexpected filtered row: %q", item.Name)
		}
	}

	// order by string is converted into sorting selectors
	req = &paginationV1.PagingRequest{
		Page:     proto.Uint32(1),
		PageSize: proto.Uint32(2),
		OrderBy:  proto.String("name desc"),
	}
	res, err = repo.ListWithPaging(ctx, cli.Client().Menu.Query(), cli.Client().Menu.Query(), req)
	if err != nil {
		t.Fatalf("order-by list failed: %v", err)
	}
	if len(res.Items) != 2 || res.Items[0].Name != "keep" {
		t.Fatalf("unexpected order-by window: %v", res.Items)
	}
}

func TestRepositoryBuildListSelectorWithPaging_Branches(t *testing.T) {
	repo := newMenuRepository(t)

	if _, _, err := repo.BuildListSelectorWithPaging(nil, nil); err == nil {
		t.Fatal("nil req must fail")
	}
	if _, _, err := repo.BuildListSelectorWithPaging(nil, &paginationV1.PagingRequest{}); err == nil {
		t.Fatal("nil builder must fail")
	}

	cases := []struct {
		name string
		req  *paginationV1.PagingRequest
	}{
		{"page", &paginationV1.PagingRequest{Page: proto.Uint32(1), PageSize: proto.Uint32(5)}},
		{"offset", &paginationV1.PagingRequest{Offset: proto.Uint64(3), Limit: proto.Uint32(7)}},
		{"token", &paginationV1.PagingRequest{Token: proto.String(pagination.EncodeAndSign(4, pagination.TokenSecret())), Offset: proto.Uint64(4)}},
		{"no-paging", &paginationV1.PagingRequest{NoPaging: proto.Bool(true)}},
		{"field-mask", &paginationV1.PagingRequest{FieldMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}}}},
		{"sorting", &paginationV1.PagingRequest{Sorting: []*paginationV1.Sorting{{Field: "name", Direction: paginationV1.Sorting_ASC}}}},
	}
	for _, tc := range cases {
		where, query, err := repo.BuildListSelectorWithPaging(nilBuilderQuery{}, tc.req)
		if err != nil {
			t.Fatalf("%s: unexpected error: %v", tc.name, err)
		}
		if query == nil {
			t.Fatalf("%s: query selectors must not be nil", tc.name)
		}
		_ = where
	}

	// an unparsable order-by string aborts selector building
	badReq := &paginationV1.PagingRequest{OrderBy: proto.String("[1,2]")}
	if _, _, err := repo.BuildListSelectorWithPaging(nilBuilderQuery{}, badReq); err == nil {
		t.Fatal("invalid order-by must fail")
	}
}

// nilBuilderQuery satisfies the ListBuilder interface for pure selector
// building (no statement is executed, only Modify closures are registered).
type nilBuilderQuery struct{}

func (nilBuilderQuery) Modify(modifiers ...func(s *sql.Selector)) *ent.MenuSelect {
	return nil
}
func (nilBuilderQuery) Clone() *ent.MenuQuery { return nil }
func (nilBuilderQuery) All(ctx context.Context) ([]*ent.Menu, error) {
	return nil, errors.New("not executed")
}
func (nilBuilderQuery) Count(ctx context.Context) (int, error) { return 0, nil }
func (nilBuilderQuery) Offset(int) *ent.MenuQuery              { return nil }
func (nilBuilderQuery) Limit(int) *ent.MenuQuery               { return nil }

// ---------------------------------------------------------------------------
// ListWithPagination / ListTreeWithPagination
// ---------------------------------------------------------------------------

func TestRepositoryListWithPagination_Variants(t *testing.T) {
	cli := newIsolatedTestEntClient(t)
	repo := newMenuRepository(t)
	ctx := context.Background()

	seedMenus(t, cli, "p1", "p2", "p3", "p4", "p5")

	offsetReq := &paginationV1.PaginationRequest{
		PaginationType: &paginationV1.PaginationRequest_OffsetBased{
			OffsetBased: &paginationV1.OffsetBasedPagination{Offset: 2, Limit: 2},
		},
		Sorting: []*paginationV1.Sorting{{Field: "name", Direction: paginationV1.Sorting_ASC}},
	}
	res, err := repo.ListWithPagination(ctx, cli.Client().Menu.Query(), cli.Client().Menu.Query(), offsetReq)
	if err != nil {
		t.Fatalf("offset pagination failed: %v", err)
	}
	if res.Total != 5 || len(res.Items) != 2 || res.Items[0].Name != "p3" {
		t.Fatalf("unexpected offset page: total=%d n=%d first=%v", res.Total, len(res.Items), res.Items[0].Name)
	}

	pageReq := &paginationV1.PaginationRequest{
		PaginationType: &paginationV1.PaginationRequest_PageBased{
			PageBased: &paginationV1.PageBasedPagination{Page: 2, PageSize: 2},
		},
		Sorting: []*paginationV1.Sorting{{Field: "name", Direction: paginationV1.Sorting_ASC}},
	}
	res, err = repo.ListWithPagination(ctx, cli.Client().Menu.Query(), cli.Client().Menu.Query(), pageReq)
	if err != nil {
		t.Fatalf("page pagination failed: %v", err)
	}
	if res.Total != 5 || len(res.Items) != 2 || res.Items[0].Name != "p3" {
		t.Fatalf("unexpected page window: total=%d n=%d first=%v", res.Total, len(res.Items), res.Items[0].Name)
	}

	tokenReq := &paginationV1.PaginationRequest{
		PaginationType: &paginationV1.PaginationRequest_TokenBased{
			TokenBased: &paginationV1.TokenBasedPagination{
				Token:    pagination.EncodeAndSign(3, pagination.TokenSecret()),
				PageSize: 2,
			},
		},
		Sorting: []*paginationV1.Sorting{{Field: "name", Direction: paginationV1.Sorting_ASC}},
	}
	res, err = repo.ListWithPagination(ctx, cli.Client().Menu.Query(), cli.Client().Menu.Query(), tokenReq)
	if err != nil {
		t.Fatalf("token pagination failed: %v", err)
	}
	if res.Total != 5 || len(res.Items) != 2 || res.Items[0].Name != "p4" {
		t.Fatalf("unexpected token window: total=%d n=%d first=%v", res.Total, len(res.Items), res.Items[0].Name)
	}

	// unspecified pagination type falls back to the no-paging row cap
	noneReq := &paginationV1.PaginationRequest{}
	res, err = repo.ListWithPagination(ctx, cli.Client().Menu.Query(), cli.Client().Menu.Query(), noneReq)
	if err != nil {
		t.Fatalf("default pagination failed: %v", err)
	}
	if res.Total != 5 || len(res.Items) != 5 {
		t.Fatalf("unexpected default result: total=%d n=%d", res.Total, len(res.Items))
	}

	// errors
	if _, err := repo.ListWithPagination(ctx, nil, nil, nil); err == nil {
		t.Fatal("nil request must fail")
	}
	if _, err := repo.ListWithPagination(ctx, nil, nil, &paginationV1.PaginationRequest{}); err == nil {
		t.Fatal("nil builder must fail")
	}
	badOrder := &paginationV1.PaginationRequest{OrderBy: proto.String("[1,2]")}
	if _, _, err := repo.BuildListSelectorWithPagination(nilBuilderQuery{}, badOrder); err == nil {
		t.Fatal("invalid order-by must fail")
	}
}

// The tree assembly in ListTreeWithPaging keys DTOs by their string ID and
// ParentID fields (ent.Menu's numeric ID would never produce an idMap entry
// and even panics inside crud/pagination.GetStringField), so these tests run
// against scripted list builders over string-keyed rows instead of the DB.

type menuTreeDTO struct {
	ID       string
	ParentID string
	Name     string
	Children []*menuTreeDTO
}

type treelessDTO struct {
	ID       string
	ParentID string
	Name     string
}

type treeRow struct {
	ID       string
	ParentID string
	Name     string
}

type fakeTreeQuery struct{}

func (fakeTreeQuery) Clone() *fakeTreeQuery { return nil }

type fakeTreeSelect struct {
	rows []*treeRow
	err  error
}

func (b *fakeTreeSelect) Modify(modifiers ...func(s *sql.Selector)) *fakeTreeSelect { return b }
func (b *fakeTreeSelect) Clone() *fakeTreeQuery                                     { return nil }
func (b *fakeTreeSelect) All(ctx context.Context) ([]*treeRow, error) {
	return b.rows, b.err
}
func (b *fakeTreeSelect) Count(ctx context.Context) (int, error) {
	return len(b.rows), b.err
}
func (b *fakeTreeSelect) Offset(int) *fakeTreeQuery { return nil }
func (b *fakeTreeSelect) Limit(int) *fakeTreeQuery  { return nil }

type treeMenuRepository = Repository[
	fakeTreeQuery, fakeTreeSelect,
	int, int, int, int, int,
	int, menuTreeDTO, treeRow,
]

type treelessMenuRepository = Repository[
	fakeTreeQuery, fakeTreeSelect,
	int, int, int, int, int,
	int, treelessDTO, treeRow,
]

func newTreeMenuRepository(rows ...*treeRow) *treeMenuRepository {
	return NewRepository[fakeTreeQuery, fakeTreeSelect, int, int, int, int, int, int](
		mapper.NewCopierMapper[menuTreeDTO, treeRow](),
	)
}

func newTreelessMenuRepository(rows ...*treeRow) *treelessMenuRepository {
	return NewRepository[fakeTreeQuery, fakeTreeSelect, int, int, int, int, int, int](
		mapper.NewCopierMapper[treelessDTO, treeRow](),
	)
}

func treeBuilder(rows ...*treeRow) *fakeTreeSelect {
	return &fakeTreeSelect{rows: rows}
}

func TestRepositoryListTreeWithPaging(t *testing.T) {
	repo := newTreeMenuRepository()
	ctx := context.Background()

	req := &paginationV1.PagingRequest{
		Page:     proto.Uint32(1),
		PageSize: proto.Uint32(10),
	}

	// nested child, orphaned child (missing parent), and explicit empty
	// parent all appear in one result set
	builder := treeBuilder(
		&treeRow{ID: "1", Name: "root"},
		&treeRow{ID: "2", ParentID: "1", Name: "child"},
		&treeRow{ID: "3", ParentID: "999", Name: "orphan"},
		&treeRow{ID: "4", ParentID: "", Name: "empty-parent"},
	)
	res, err := repo.ListTreeWithPaging(ctx, builder, treeBuilder(
		&treeRow{ID: "1"}, &treeRow{ID: "2"}, &treeRow{ID: "3"}, &treeRow{ID: "4"},
	), req)
	if err != nil {
		t.Fatalf("ListTreeWithPaging failed: %v", err)
	}
	if res.Total != 4 {
		t.Fatalf("expected total 4, got %d", res.Total)
	}
	if len(res.Items) != 3 {
		t.Fatalf("expected 3 roots (root, orphan, empty-parent), got %d", len(res.Items))
	}
	if res.Items[0].ID != "1" {
		t.Fatalf("unexpected root id %q", res.Items[0].ID)
	}
	if len(res.Items[0].Children) != 1 || res.Items[0].Children[0].Name != "child" {
		t.Fatalf("child was not nested under root: %+v", res.Items[0])
	}

	// builder failures propagate
	failing := &fakeTreeSelect{err: errors.New("boom")}
	if _, err := repo.ListTreeWithPaging(ctx, failing, nil, req); err == nil {
		t.Fatal("builder failure must propagate")
	}

	// errors mirror the flat list path
	if _, err := repo.ListTreeWithPaging(ctx, nil, nil, nil); err == nil {
		t.Fatal("nil request must fail")
	}
}

func TestRepositoryListTree_FallbackBranches(t *testing.T) {
	repo := newTreelessMenuRepository()
	ctx := context.Background()

	req := &paginationV1.PagingRequest{
		Page:     proto.Uint32(1),
		PageSize: proto.Uint32(10),
	}

	// treelessDTO has no Children field: AppendChild fails and the child
	// falls back into the root list
	builder := treeBuilder(
		&treeRow{ID: "1", Name: "root"},
		&treeRow{ID: "2", ParentID: "1", Name: "child"},
	)
	res, err := repo.ListTreeWithPaging(ctx, builder, treeBuilder(&treeRow{ID: "1"}, &treeRow{ID: "2"}), req)
	if err != nil {
		t.Fatalf("ListTreeWithPaging(treeless) failed: %v", err)
	}
	if res.Total != 2 || len(res.Items) != 2 {
		t.Fatalf("expected both rows promoted to roots, got total=%d n=%d", res.Total, len(res.Items))
	}

	// ListTreeWithPagination mirrors the same assembly
	pageReq := &paginationV1.PaginationRequest{
		PaginationType: &paginationV1.PaginationRequest_PageBased{
			PageBased: &paginationV1.PageBasedPagination{Page: 1, PageSize: 10},
		},
	}
	res2, err := repo.ListTreeWithPagination(ctx, treeBuilder(
		&treeRow{ID: "1"}, &treeRow{ID: "2", ParentID: "1"},
	), treeBuilder(&treeRow{ID: "1"}, &treeRow{ID: "2"}), pageReq)
	if err != nil {
		t.Fatalf("ListTreeWithPagination failed: %v", err)
	}
	if res2.Total != 2 || len(res2.Items) != 2 {
		t.Fatalf("unexpected treeless pagination result: total=%d n=%d", res2.Total, len(res2.Items))
	}
	if _, err := repo.ListTreeWithPagination(ctx, nil, nil, nil); err == nil {
		t.Fatal("nil request must fail")
	}

	// full tree assembly through the PaginationRequest variant
	treeRepo := newTreeMenuRepository()
	res3, err := treeRepo.ListTreeWithPagination(ctx, treeBuilder(
		&treeRow{ID: "1"}, &treeRow{ID: "2", ParentID: "1"},
	), treeBuilder(&treeRow{ID: "1"}, &treeRow{ID: "2"}), pageReq)
	if err != nil {
		t.Fatalf("ListTreeWithPagination(tree) failed: %v", err)
	}
	if res3.Total != 2 || len(res3.Items) != 1 || len(res3.Items[0].Children) != 1 {
		t.Fatalf("unexpected tree result: total=%d n=%d", res3.Total, len(res3.Items))
	}

	// builder failures propagate through the pagination variant too
	failing := &fakeTreeSelect{err: errors.New("boom")}
	if _, err := treeRepo.ListTreeWithPagination(ctx, failing, nil, pageReq); err == nil {
		t.Fatal("builder failure must propagate")
	}
}

// ---------------------------------------------------------------------------
// Create / CreateX / BatchCreate (proto DTO over the in-memory database)
// ---------------------------------------------------------------------------

func TestRepositoryCreate_WithProtoDTO(t *testing.T) {
	repo := newProtoMenuRepository(t)
	cli := newIsolatedTestEntClient(t)
	ctx := context.Background()

	// argument validation happens before any statement
	if _, err := repo.Create(ctx, nil, nil, nil, nil); err == nil {
		t.Fatal("nil builder must fail")
	}
	createBuilder := cli.Client().Menu.Create()
	if _, err := repo.Create(ctx, createBuilder, nil, nil, nil); err == nil {
		t.Fatal("nil dto must fail")
	}

	// invalid field mask aborts the create
	badMask := mustMask("no_such_field")
	if _, err := repo.Create(ctx, cli.Client().Menu.Create(), newTestUserDTO("x"), badMask, nil); err == nil {
		t.Fatal("invalid field mask must fail")
	}

	// happy path: the callback copies dto fields onto the builder
	dto := newTestUserDTO("created-by-repo")
	b := cli.Client().Menu.Create()
	created, err := repo.Create(ctx, b, dto, nil, func(d *testUserDTO) {
		b.SetName(d.Name())
	})
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if created == nil {
		t.Fatal("created dto must not be nil")
	}
	stored, err := cli.Client().Menu.Query().Where(menu.NameEQ("created-by-repo")).Only(ctx)
	if err != nil {
		t.Fatalf("created row missing: %v", err)
	}
	_ = stored
}

func TestRepositoryCreateX_WithProtoDTO(t *testing.T) {
	repo := newProtoMenuRepository(t)
	cli := newIsolatedTestEntClient(t)
	ctx := context.Background()

	if err := repo.CreateX(ctx, nil, nil, nil, nil); err == nil {
		t.Fatal("nil builder must fail")
	}
	if err := repo.CreateX(ctx, cli.Client().Menu.Create(), nil, nil, nil); err == nil {
		t.Fatal("nil dto must fail")
	}
	if err := repo.CreateX(ctx, cli.Client().Menu.Create(), newTestUserDTO("x"), mustMask("nope"), nil); err == nil {
		t.Fatal("invalid field mask must fail")
	}

	b := cli.Client().Menu.Create()
	if err := repo.CreateX(ctx, b, newTestUserDTO("created-x"), nil, func(d *testUserDTO) {
		b.SetName(d.Name())
	}); err != nil {
		t.Fatalf("CreateX failed: %v", err)
	}
	count, err := cli.Client().Menu.Query().Where(menu.NameEQ("created-x")).Count(ctx)
	if err != nil || count != 1 {
		t.Fatalf("CreateX row missing: count=%d err=%v", count, err)
	}
}

func TestRepositoryBatchCreate_WithProtoDTO(t *testing.T) {
	repo := newProtoMenuRepository(t)
	cli := newIsolatedTestEntClient(t)
	ctx := context.Background()

	if _, err := repo.BatchCreate(ctx, nil, nil, nil, nil); err == nil {
		t.Fatal("nil builder must fail")
	}
	if _, err := repo.BatchCreate(ctx, cli.Client().Menu.CreateBulk(), nil, nil, nil); err == nil {
		t.Fatal("empty dtos must fail")
	}
	if _, err := repo.BatchCreate(ctx, cli.Client().Menu.CreateBulk(), []*testUserDTO{newTestUserDTO("x")}, mustMask("nope"), nil); err == nil {
		t.Fatal("invalid field mask must fail")
	}

	// Fix: the mapped entities are folded into the bulk create. ent's
	// CreateBulk builder cannot accept records after construction, so the
	// caller (which holds the client) registers an entity bulk factory -
	// here backed by MapCreateBulk - and the dtos no longer need
	// caller-pre-registered creates to be persisted.
	repo.WithBulkEntityFactory(func(ents []*ent.Menu) (CreateBulkBuilder[ent.MenuCreateBulk, ent.Menu], error) {
		return cli.Client().Menu.MapCreateBulk(ents, func(c *ent.MenuCreate, i int) {
			c.SetName(ents[i].Name)
		}), nil
	})

	created, err := repo.BatchCreate(ctx, cli.Client().Menu.CreateBulk(), []*testUserDTO{nil, newTestUserDTO("batch-1"), newTestUserDTO("batch-2")}, nil, nil)
	if err != nil {
		t.Fatalf("BatchCreate failed: %v", err)
	}
	if len(created) != 2 {
		t.Fatalf("expected 2 created dtos, got %d", len(created))
	}
	count, err := cli.Client().Menu.Query().Where(menu.NameIn("batch-1", "batch-2")).Count(ctx)
	if err != nil || count != 2 {
		t.Fatalf("mapped dtos were not persisted: count=%d err=%v", count, err)
	}

	// without a registered factory the builder saves whatever the caller
	// pre-registered on it (legacy fallback path)
	fallbackRepo := newProtoMenuRepository(t)
	c1 := cli.Client().Menu.Create().SetName("pre-1")
	c2 := cli.Client().Menu.Create().SetName("pre-2")
	created, err = fallbackRepo.BatchCreate(ctx, cli.Client().Menu.CreateBulk(c1, c2), []*testUserDTO{nil, newTestUserDTO("pre-1"), newTestUserDTO("pre-2")}, nil, nil)
	if err != nil {
		t.Fatalf("BatchCreate(fallback) failed: %v", err)
	}
	if len(created) != 2 {
		t.Fatalf("expected 2 created dtos, got %d", len(created))
	}
	count, err = cli.Client().Menu.Query().Where(menu.NameIn("pre-1", "pre-2")).Count(ctx)
	if err != nil || count != 2 {
		t.Fatalf("pre-registered rows missing: count=%d err=%v", count, err)
	}
}

// ---------------------------------------------------------------------------
// UpdateOne / UpdateX / Delete (proto DTO over the in-memory database)
// ---------------------------------------------------------------------------

func TestRepositoryUpdateOne_WithProtoDTO(t *testing.T) {
	repo := newProtoMenuRepository(t)
	cli := newIsolatedTestEntClient(t)
	ctx := context.Background()

	if _, err := repo.UpdateOne(ctx, nil, nil, nil, nil); err == nil {
		t.Fatal("nil builder must fail")
	}
	seeded := seedMenus(t, cli, "before")
	if _, err := repo.UpdateOne(ctx, cli.Client().Menu.UpdateOneID(seeded[0].ID), nil, nil, nil); err == nil {
		t.Fatal("nil dto must fail")
	}
	if _, err := repo.UpdateOne(ctx, cli.Client().Menu.UpdateOneID(seeded[0].ID), newTestUserDTO("x"), mustMask("nope"), nil); err == nil {
		t.Fatal("invalid field mask must fail")
	}

	// mask lists "path" but the dto leaves it unset: the nil-value path must
	// NULL the column while untouched fields (name) survive
	b := cli.Client().Menu.UpdateOneID(seeded[0].ID)
	if err := b.SetPath("/old/").Exec(ctx); err != nil {
		t.Fatalf("seed path failed: %v", err)
	}

	dto := newTestUserDTO("")
	b = cli.Client().Menu.UpdateOneID(seeded[0].ID)
	updated, err := repo.UpdateOne(ctx, b, dto, mustMask("path"), nil)
	if err != nil {
		t.Fatalf("UpdateOne failed: %v", err)
	}
	if updated == nil {
		t.Fatal("updated dto must not be nil")
	}
	row, err := cli.Client().Menu.Get(ctx, seeded[0].ID)
	if err != nil {
		t.Fatalf("reload failed: %v", err)
	}
	if row.Path != nil {
		t.Fatalf("path must be NULL after nil-mask update, got %q", *row.Path)
	}
	if row.Name != "before" {
		t.Fatalf("name must be untouched, got %q", row.Name)
	}
}

func TestRepositoryUpdateX_WithProtoDTO(t *testing.T) {
	repo := newProtoMenuRepository(t)
	cli := newIsolatedTestEntClient(t)
	ctx := context.Background()

	if err := repo.UpdateX(ctx, nil, nil, nil, nil); err == nil {
		t.Fatal("nil builder must fail")
	}
	seeded := seedMenus(t, cli, "u1", "u2")
	if err := repo.UpdateX(ctx, cli.Client().Menu.Update(), nil, nil, nil); err == nil {
		t.Fatal("nil dto must fail")
	}
	if err := repo.UpdateX(ctx, cli.Client().Menu.Update(), newTestUserDTO("x"), mustMask("nope"), nil); err == nil {
		t.Fatal("invalid field mask must fail")
	}

	dto := newTestUserDTO("")
	err := repo.UpdateX(ctx, cli.Client().Menu.Update(), dto, nil, func(d *testUserDTO) {
		// no-op: bulk update without field changes only counts affected rows
	}, menu.NameIn("u1", "u2"))
	if err != nil {
		t.Fatalf("UpdateX failed: %v", err)
	}
	count, err := cli.Client().Menu.Query().Count(ctx)
	if err != nil || count != len(seeded) {
		t.Fatalf("rows must survive UpdateX: count=%d err=%v", count, err)
	}
}

func TestRepositoryDelete_WithDB(t *testing.T) {
	repo := newProtoMenuRepository(t)
	cli := newIsolatedTestEntClient(t)
	ctx := context.Background()

	if _, err := repo.Delete(ctx, nil); err == nil {
		t.Fatal("nil builder must fail")
	}

	seeded := seedMenus(t, cli, "d1", "d2")
	affected, err := repo.Delete(ctx, cli.Client().Menu.Delete(), menu.IDEQ(seeded[0].ID))
	if err != nil {
		t.Fatalf("delete failed: %v", err)
	}
	if affected != 1 {
		t.Fatalf("expected 1 affected row, got %d", affected)
	}
	count, err := cli.Client().Menu.Query().Count(ctx)
	if err != nil || count != 1 {
		t.Fatalf("expected 1 surviving row, got %d (err=%v)", count, err)
	}

	affected, err = repo.DeleteWithCache(ctx, cli.Client().Menu.Delete(), seeded[1].ID, menu.IDEQ(seeded[1].ID))
	if err != nil {
		t.Fatalf("DeleteWithCache failed: %v", err)
	}
	if affected != 1 {
		t.Fatalf("DeleteWithCache affected %d, want 1", affected)
	}
	if _, err := repo.DeleteWithCache(ctx, nil, nil); err == nil {
		t.Fatal("DeleteWithCache with nil builder must fail")
	}
}

// ---------------------------------------------------------------------------
// Cache-aware variants (degraded paths only - no redis server is contacted)
// ---------------------------------------------------------------------------

func newCacheCapableViewer(tid uint64) viewer.Context {
	return &cacheKeyViewer{tid: tid}
}

type cacheKeyViewer struct {
	tid       uint64
	platform  bool
	scopes    []viewer.DataScope
	uid       uint64
	orgUnitID uint64
}

func (s *cacheKeyViewer) UserID() uint64                 { return s.uid }
func (s *cacheKeyViewer) TenantID() uint64               { return s.tid }
func (s *cacheKeyViewer) OrgUnitID() uint64              { return s.orgUnitID }
func (s *cacheKeyViewer) Permissions() []string          { return nil }
func (s *cacheKeyViewer) Roles() []string                { return nil }
func (s *cacheKeyViewer) DataScope() []viewer.DataScope  { return s.scopes }
func (s *cacheKeyViewer) TraceID() string                { return "" }
func (s *cacheKeyViewer) HasPermission(_, _ string) bool { return false }
func (s *cacheKeyViewer) IsPlatformContext() bool        { return s.platform }
func (s *cacheKeyViewer) IsTenantContext() bool          { return s.tid > 0 && !s.platform }
func (s *cacheKeyViewer) IsSystemContext() bool          { return false }
func (s *cacheKeyViewer) ShouldAudit() bool              { return false }

func TestRepositoryCacheKeys(t *testing.T) {
	repo := newMenuRepository(t)
	vc := newCacheCapableViewer(7)

	if _, err := repo.generateListCacheKey(vc, nil); err == nil {
		t.Fatal("nil request must fail")
	}
	if _, err := repo.generateListCacheKeyFromPagination(vc, nil); err == nil {
		t.Fatal("nil request must fail")
	}

	validToken := pagination.EncodeAndSign(3, pagination.TokenSecret())

	cases := []*paginationV1.PagingRequest{
		{Page: proto.Uint32(2), PageSize: proto.Uint32(5)},
		{Offset: proto.Uint64(1), Limit: proto.Uint32(9)},
		{Token: proto.String(validToken), Offset: proto.Uint64(3)},
		{FilteringType: &paginationV1.PagingRequest_FilterExpr{FilterExpr: &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{
				{Field: "name", ValueOneof: &paginationV1.FilterCondition_Value{Value: "x"}, Op: paginationV1.Operator_EQ},
			},
		}}},
		{OrderBy: proto.String("name desc")},
		{Sorting: []*paginationV1.Sorting{{Field: "name", Direction: paginationV1.Sorting_DESC}}},
		{FieldMask: &fieldmaskpb.FieldMask{Paths: []string{"name", "id"}}},
	}
	seen := make(map[string]bool)
	for i, req := range cases {
		key, err := repo.generateListCacheKey(vc, req)
		if err != nil {
			t.Fatalf("case %d: unexpected error: %v", i, err)
		}
		if key == "" || seen[key] {
			t.Fatalf("case %d: key must be unique and non-empty, got %q", i, key)
		}
		seen[key] = true
	}

	// invalid token is rejected (fail-closed, no cache entry)
	if _, err := repo.generateListCacheKey(vc, &paginationV1.PagingRequest{Token: proto.String("garbage")}); err == nil {
		t.Fatal("invalid token must fail")
	}

	// data scope dimension changes the key
	scopeViewer := &cacheKeyViewer{tid: 7, scopes: []viewer.DataScope{{ScopeType: viewer.ScopeTypeUnit, TargetIDs: []uint64{3}}}}
	keyA, err := repo.generateListCacheKey(vc, &paginationV1.PagingRequest{})
	if err != nil {
		t.Fatalf("plain key failed: %v", err)
	}
	keyB, err := repo.generateListCacheKey(scopeViewer, &paginationV1.PagingRequest{})
	if err != nil {
		t.Fatalf("scoped key failed: %v", err)
	}
	if keyA == keyB {
		t.Fatal("data scope must change the list cache key")
	}

	// PaginationRequest variants
	pgCases := []*paginationV1.PaginationRequest{
		{PaginationType: &paginationV1.PaginationRequest_OffsetBased{OffsetBased: &paginationV1.OffsetBasedPagination{Offset: 1, Limit: 4}}},
		{PaginationType: &paginationV1.PaginationRequest_PageBased{PageBased: &paginationV1.PageBasedPagination{Page: 1, PageSize: 4}}},
		{PaginationType: &paginationV1.PaginationRequest_TokenBased{TokenBased: &paginationV1.TokenBasedPagination{Token: validToken, PageSize: 4}}},
		{FilteringType: &paginationV1.PaginationRequest_FilterExpr{FilterExpr: &paginationV1.FilterExpr{Type: paginationV1.ExprType_OR}}},
		{OrderBy: proto.String("id")},
		{Sorting: []*paginationV1.Sorting{{Field: "id", Direction: paginationV1.Sorting_ASC}}},
		{FieldMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}}},
	}
	for i, req := range pgCases {
		key, err := repo.generateListCacheKeyFromPagination(vc, req)
		if err != nil {
			t.Fatalf("pagination case %d: unexpected error: %v", i, err)
		}
		if key == "" {
			t.Fatalf("pagination case %d: key must not be empty", i)
		}
	}
	if _, err := repo.generateListCacheKeyFromPagination(vc, &paginationV1.PaginationRequest{
		PaginationType: &paginationV1.PaginationRequest_TokenBased{TokenBased: &paginationV1.TokenBasedPagination{Token: "garbage", PageSize: 4}},
	}); err == nil {
		t.Fatal("invalid token must fail")
	}
}

func TestRepositoryCacheAware_DegradedAndFallback(t *testing.T) {
	cli := newIsolatedTestEntClient(t)
	repo := newMenuRepository(t)
	ctx := context.Background()
	seedMenus(t, cli, "c1", "c2")

	req := &paginationV1.PagingRequest{
		Page:     proto.Uint32(1),
		PageSize: proto.Uint32(1),
	}

	// without cache support every cached method degrades to the plain query
	res, err := repo.ListWithPagingCache(ctx, cli.Client().Menu.Query(), cli.Client().Menu.Query(), req)
	if err != nil || res.Total != 2 {
		t.Fatalf("ListWithPagingCache degraded = %+v, %v", res, err)
	}
	res, err = repo.ListWithPaginationCache(ctx, cli.Client().Menu.Query(), cli.Client().Menu.Query(), &paginationV1.PaginationRequest{})
	if err != nil || res == nil {
		t.Fatalf("ListWithPaginationCache degraded = %+v, %v", res, err)
	}
	// NOTE: ListTree* with a numeric-ID DTO panics inside
	// crud/pagination.GetStringField (uint32 kind is unhandled there), so the
	// tree variants are exercised with the string-ID tree DTO instead.
	treeRepo := newTreeMenuRepository()
	treeBuilderRows := []*treeRow{{ID: "1"}, {ID: "2", ParentID: "1"}}
	resT, err := treeRepo.ListTreeWithPagingCache(ctx, treeBuilder(treeBuilderRows...), treeBuilder(&treeRow{ID: "1"}, &treeRow{ID: "2"}), req)
	if err != nil || resT == nil {
		t.Fatalf("ListTreeWithPagingCache degraded = %+v, %v", resT, err)
	}
	resT, err = treeRepo.ListTreeWithPaginationCache(ctx, treeBuilder(treeBuilderRows...), treeBuilder(&treeRow{ID: "1"}, &treeRow{ID: "2"}), &paginationV1.PaginationRequest{})
	if err != nil || resT == nil {
		t.Fatalf("ListTreeWithPaginationCache degraded = %+v, %v", resT, err)
	}
	// GetByIDWithCache delegates to Get on the caller-provided builder (the
	// id argument is only used for cache keying); uniqueness comes from the
	// builder's own predicate.
	seeded := seedMenus(t, cli, "get-by-id")
	dto, err := repo.GetByIDWithCache(ctx, cli.Client().Menu.Query().Where(menu.IDEQ(seeded[0].ID)), seeded[0].ID, nil)
	if err != nil || dto == nil {
		t.Fatalf("GetByIDWithCache degraded = %+v, %v", dto, err)
	}

	// Create/Update with cache enabled but no support configured: the
	// cache-invalidation step is a no-op and the write still succeeds.
	cacheRepo := newProtoMenuRepository(t)
	cacheRepo.WithCache(redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}), "menu:", time.Minute, 30*time.Second)
	// a configured repo whose key generation fails must fall back to the db
	badReq := &paginationV1.PagingRequest{Token: proto.String("garbage")}
	badPgReq := &paginationV1.PaginationRequest{
		PaginationType: &paginationV1.PaginationRequest_TokenBased{
			TokenBased: &paginationV1.TokenBasedPagination{Token: "garbage", PageSize: 4},
		},
	}
	resP, err := cacheRepo.ListWithPagingCache(ctx, cli.Client().Menu.Query(), cli.Client().Menu.Query(), badReq)
	if err != nil || resP == nil {
		t.Fatalf("fallback on bad key = %+v, %v", resP, err)
	}
	resP, err = cacheRepo.ListWithPaginationCache(ctx, cli.Client().Menu.Query(), cli.Client().Menu.Query(), badPgReq)
	if err != nil || resP == nil {
		t.Fatalf("ListWithPaginationCache fallback = %+v, %v", resP, err)
	}
	resP, err = cacheRepo.ListTreeWithPagingCache(ctx, cli.Client().Menu.Query(), cli.Client().Menu.Query(), badReq)
	if err != nil || resP == nil {
		t.Fatalf("ListTreeWithPagingCache fallback = %+v, %v", resP, err)
	}
	resP, err = cacheRepo.ListTreeWithPaginationCache(ctx, cli.Client().Menu.Query(), cli.Client().Menu.Query(), badPgReq)
	if err != nil || resP == nil {
		t.Fatalf("ListTreeWithPaginationCache fallback = %+v, %v", resP, err)
	}
	if _, err = cacheRepo.ListWithPaginationCache(ctx, cli.Client().Menu.Query(), cli.Client().Menu.Query(), nil); err == nil {
		t.Fatal("nil request must fail")
	}

	// CreateWithCache / UpdateWithCache write through with invalidation a no-op
	// CreateWithCache without cache support: invalidation is a no-op and the
	// write-through completes. (With cache support enabled, invalidateCache
	// would dial redis, which is out of scope for hermetic tests.)
	writeRepo := newProtoMenuRepository(t)
	createB := cli.Client().Menu.Create()
	created, err := writeRepo.CreateWithCache(ctx, createB, newTestUserDTO("cache-create"), nil, func(d *testUserDTO) {
		createB.SetName(d.Name())
	})
	if err != nil || created == nil {
		t.Fatalf("CreateWithCache = %+v, %v", created, err)
	}
	// ToDTO cannot populate the dynamic message (copier only touches exported
	// fields), so the returned DTO is empty and GetId() extracts 0 - the point
	// here is that the write-through + invalidation path completes cleanly.
	if got := created.GetId(); got != 0 {
		t.Fatalf("dynamic DTO roundtrip must stay empty, got id %d", got)
	}
	count, err := cli.Client().Menu.Query().Where(menu.NameEQ("cache-create")).Count(ctx)
	if err != nil || count != 1 {
		t.Fatalf("CreateWithCache row missing: count=%d err=%v", count, err)
	}
}

// ---------------------------------------------------------------------------
// EntClient Query/Exec passthrough
// ---------------------------------------------------------------------------

func TestEntClient_Exec_Query(t *testing.T) {
	cli := newIsolatedTestEntClient(t)
	ctx := context.Background()

	if err := cli.Exec(ctx, "SELECT 1", []any{}, nil); err != nil {
		t.Fatalf("exec failed: %v", err)
	}

	rows := &sql.Rows{}
	if err := cli.Query(ctx, "SELECT 1", []any{}, rows); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatal("expected one row")
	}
	var v int
	if err := rows.Scan(&v); err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	if v != 1 {
		t.Fatalf("expected 1, got %d", v)
	}
}

// ---------------------------------------------------------------------------
// Non-proto DTOs on the write paths (regression: the unchecked
// dto.(proto.Message) assertions used to panic here)
// ---------------------------------------------------------------------------

// plainNonProtoDTO deliberately does NOT implement proto.Message.
type plainNonProtoDTO struct {
	Name string
}

func newNonProtoMenuRepository() *Repository[
	ent.MenuQuery, ent.MenuSelect,
	ent.MenuCreate, ent.MenuCreateBulk,
	ent.MenuUpdate, ent.MenuUpdateOne,
	ent.MenuDelete,
	predicate.Menu, plainNonProtoDTO, ent.Menu,
] {
	return NewRepository[
		ent.MenuQuery, ent.MenuSelect,
		ent.MenuCreate, ent.MenuCreateBulk,
		ent.MenuUpdate, ent.MenuUpdateOne,
		ent.MenuDelete,
		predicate.Menu,
	](mapper.NewCopierMapper[plainNonProtoDTO, ent.Menu]())
}

func TestRepositoryWritePaths_NonProtoDTO(t *testing.T) {
	repo := newNonProtoMenuRepository()
	cli := newIsolatedTestEntClient(t)
	ctx := context.Background()

	dto := &plainNonProtoDTO{Name: "no-proto"}

	if _, err := repo.Create(ctx, cli.Client().Menu.Create(), dto, nil, nil); !errors.Is(err, errDtoNotProtoMessage) {
		t.Fatalf("Create: expected errDtoNotProtoMessage, got %v", err)
	}
	if err := repo.CreateX(ctx, cli.Client().Menu.Create(), dto, nil, nil); !errors.Is(err, errDtoNotProtoMessage) {
		t.Fatalf("CreateX: expected errDtoNotProtoMessage, got %v", err)
	}

	seeded := seedMenus(t, cli, "np-1")
	if _, err := repo.UpdateOne(ctx, cli.Client().Menu.UpdateOneID(seeded[0].ID), dto, nil, nil); !errors.Is(err, errDtoNotProtoMessage) {
		t.Fatalf("UpdateOne: expected errDtoNotProtoMessage, got %v", err)
	}
	if err := repo.UpdateX(ctx, cli.Client().Menu.Update(), dto, nil, nil); !errors.Is(err, errDtoNotProtoMessage) {
		t.Fatalf("UpdateX: expected errDtoNotProtoMessage, got %v", err)
	}

	if _, err := repo.BatchCreate(ctx, cli.Client().Menu.CreateBulk(), []*plainNonProtoDTO{dto}, nil, nil); !errors.Is(err, errDtoNotProtoMessage) {
		t.Fatalf("BatchCreate: expected errDtoNotProtoMessage, got %v", err)
	}

	// the failed writes must not have touched the database
	count, err := cli.Client().Menu.Query().Count(ctx)
	if err != nil || count != 1 {
		t.Fatalf("failed writes must not persist rows: count=%d err=%v", count, err)
	}
}
