package entgo

import (
	_ "github.com/xiaoqidun/entps"
)

//func createUserRepo(m *mapper.CopierMapper[User, ent.User]) *Repository[
//	ent.UserQuery, ent.UserSelect,
//	ent.UserCreate, ent.UserCreateBulk,
//	ent.UserUpdate, ent.UserUpdateOne,
//	ent.UserDelete,
//	predicate.User, User, ent.User,
//] {
//	return NewRepository[
//		ent.UserQuery, ent.UserSelect,
//		ent.UserCreate, ent.UserCreateBulk,
//		ent.UserUpdate, ent.UserUpdateOne,
//		ent.UserDelete,
//		predicate.User, User, ent.User,
//	](m)
//}
//
//func TestCount_NilBuilder_ReturnsError(t *testing.T) {
//	ctx := context.Background()
//	//cli := createTestEntClient(t)
//	m := &mapper.CopierMapper[User, ent.User]{}
//	r := createUserRepo(m)
//
//	//builder := cli.User.Query()
//
//	_, err := r.Count(ctx, nil, nil)
//	if err == nil || !strings.Contains(err.Error(), "query builder is nil") {
//		t.Fatalf("expected 'query builder is nil' error, got: %v", err)
//	}
//}
//
//func TestCount_ReturnsNoError(t *testing.T) {
//	ctx := context.Background()
//	cli := createTestEntClient(t)
//	m := &mapper.CopierMapper[User, ent.User]{}
//	r := createUserRepo(m)
//
//	builder := cli.User.Query()
//
//	_, err := r.Count(ctx, builder, nil)
//	if err != nil {
//		t.Fatalf("expected no error, got: %v", err)
//	}
//}
//
//func TestExists_NilBuilder_ReturnsError(t *testing.T) {
//	ctx := context.Background()
//	//cli := createTestEntClient(t)
//	m := &mapper.CopierMapper[User, ent.User]{}
//	r := createUserRepo(m)
//
//	_, err := r.Exists(ctx, nil, nil)
//	if err == nil || !strings.Contains(err.Error(), "query builder is nil") {
//		t.Fatalf("expected 'query builder is nil' error, got: %v", err)
//	}
//}
//
//func TestListWithPaging_NilReq_ReturnsError(t *testing.T) {
//	ctx := context.Background()
//	//cli := createTestEntClient(t)
//	m := &mapper.CopierMapper[User, ent.User]{}
//	r := createUserRepo(m)
//
//	_, err := r.ListWithPaging(ctx, nil, nil, nil)
//	if err == nil || !strings.Contains(err.Error(), "paging request is nil") {
//		t.Fatalf("expected 'paging request is nil' error, got: %v", err)
//	}
//}
//
//func TestListWithPaging_NilBuilder_ReturnsError(t *testing.T) {
//	ctx := context.Background()
//	//cli := createTestEntClient(t)
//	m := &mapper.CopierMapper[User, ent.User]{}
//	r := createUserRepo(m)
//
//	req := &paginationV1.PagingRequest{}
//	_, err := r.ListWithPaging(ctx, nil, nil, req)
//	if err == nil || !strings.Contains(err.Error(), "query builder is nil") {
//		t.Fatalf("expected 'query builder is nil' error, got: %v", err)
//	}
//}
//
//func TestGet_NilBuilder_ReturnsError(t *testing.T) {
//	ctx := context.Background()
//	//cli := createTestEntClient(t)
//	m := &mapper.CopierMapper[User, ent.User]{}
//	r := createUserRepo(m)
//
//	_, err := r.Get(ctx, nil, nil, nil)
//	if err == nil || !strings.Contains(err.Error(), "query builder is nil") {
//		t.Fatalf("expected 'query builder is nil' error, got: %v", err)
//	}
//}
//
//func TestCreate_NilBuilder_ReturnsError(t *testing.T) {
//	ctx := context.Background()
//	//cli := createTestEntClient(t)
//	m := &mapper.CopierMapper[User, ent.User]{}
//	r := createUserRepo(m)
//
//	_, err := r.Create(ctx, nil, nil, nil, nil)
//	if err == nil || !strings.Contains(err.Error(), "query builder is nil") {
//		t.Fatalf("expected 'query builder is nil' error, got: %v", err)
//	}
//}
//
//func TestCreate_NilDTO_ReturnsError(t *testing.T) {
//	ctx := context.Background()
//	cli := createTestEntClient(t)
//	m := &mapper.CopierMapper[User, ent.User]{}
//	r := createUserRepo(m)
//
//	builder := cli.User.Create()
//
//	_, err := r.Create(ctx, builder, nil, nil, nil)
//	if err == nil || !strings.Contains(err.Error(), "dto is nil") {
//		t.Fatalf("expected 'dto is nil' error, got: %v", err)
//	}
//}
//
//func TestCreateX_NilBuilder_ReturnsError(t *testing.T) {
//	ctx := context.Background()
//	//cli := createTestEntClient(t)
//	m := &mapper.CopierMapper[User, ent.User]{}
//	r := createUserRepo(m)
//
//	err := r.CreateX(ctx, nil, nil, nil, nil)
//	if err == nil || !strings.Contains(err.Error(), "query builder is nil") {
//		t.Fatalf("expected 'query builder is nil' error, got: %v", err)
//	}
//}
//
//func TestCreateX_NilDTO_ReturnsError(t *testing.T) {
//	ctx := context.Background()
//	cli := createTestEntClient(t)
//	m := &mapper.CopierMapper[User, ent.User]{}
//	r := createUserRepo(m)
//
//	builder := cli.User.Create()
//
//	err := r.CreateX(ctx, builder, nil, nil, nil)
//	if err == nil || !strings.Contains(err.Error(), "dto is nil") {
//		t.Fatalf("expected 'dto is nil' error, got: %v", err)
//	}
//}
//
//func TestUpdateOne_NilBuilder_ReturnsError(t *testing.T) {
//	ctx := context.Background()
//	//cli := createTestEntClient(t)
//	m := &mapper.CopierMapper[User, ent.User]{}
//	r := createUserRepo(m)
//
//	//builder := cli.User.Update()
//
//	_, err := r.UpdateOne(ctx, nil, nil, nil, nil, nil)
//	if err == nil || !strings.Contains(err.Error(), "query builder is nil") {
//		t.Fatalf("expected 'query builder is nil' error, got: %v", err)
//	}
//}
//
//func TestUpdateOne_NilDTO_ReturnsError(t *testing.T) {
//	ctx := context.Background()
//	cli := createTestEntClient(t)
//	m := &mapper.CopierMapper[User, ent.User]{}
//	r := createUserRepo(m)
//
//	builder := cli.User.UpdateOneID(1)
//
//	_, err := r.UpdateOne(ctx, builder, nil, nil, nil, nil)
//	if err == nil || !strings.Contains(err.Error(), "dto is nil") {
//		t.Fatalf("expected 'dto is nil' error, got: %v", err)
//	}
//}
//
//func TestUpdateX_NilBuilder_ReturnsError(t *testing.T) {
//	ctx := context.Background()
//	//cli := createTestEntClient(t)
//	m := &mapper.CopierMapper[User, ent.User]{}
//	r := createUserRepo(m)
//
//	//builder := cli.User.Update()
//
//	err := r.UpdateX(ctx, nil, nil, nil, nil, nil)
//	if err == nil || !strings.Contains(err.Error(), "query builder is nil") {
//		t.Fatalf("expected 'query builder is nil' error, got: %v", err)
//	}
//}
//
//func TestUpdateX_NilDTO_ReturnsError(t *testing.T) {
//	ctx := context.Background()
//	cli := createTestEntClient(t)
//	m := &mapper.CopierMapper[User, ent.User]{}
//	r := createUserRepo(m)
//
//	builder := cli.User.Update()
//
//	err := r.UpdateX(ctx, builder, nil, nil, nil, nil)
//	if err == nil || !strings.Contains(err.Error(), "dto is nil") {
//		t.Fatalf("expected 'dto is nil' error, got: %v", err)
//	}
//}
//
//func TestBatchCreate_NilBuilder_ReturnsError(t *testing.T) {
//	ctx := context.Background()
//	//cli := createTestEntClient(t)
//	m := &mapper.CopierMapper[User, ent.User]{}
//	r := createUserRepo(m)
//
//	_, err := r.BatchCreate(ctx, nil, nil, nil, nil)
//	if err == nil || !strings.Contains(err.Error(), "query builder is nil") {
//		t.Fatalf("expected 'query builder is nil' error, got: %v", err)
//	}
//}
//
//func TestDelete_NilBuilder_ReturnsError(t *testing.T) {
//	ctx := context.Background()
//	//cli := createTestEntClient(t)
//	m := &mapper.CopierMapper[User, ent.User]{}
//	r := createUserRepo(m)
//
//	_, err := r.Delete(ctx, nil, nil)
//	if err == nil || !strings.Contains(err.Error(), "query builder is nil") {
//		t.Fatalf("expected 'query builder is nil' error, got: %v", err)
//	}
//}
//
//func TestDelete_WithFakeBuilder_NoError(t *testing.T) {
//	ctx := context.Background()
//	cli := createTestEntClient(t)
//	m := &mapper.CopierMapper[User, ent.User]{}
//	r := createUserRepo(m)
//
//	builder := cli.User.Delete()
//
//	_, err := r.Delete(ctx, builder, nil)
//	if err != nil {
//		t.Fatalf("expected no error with fake delete builder, got: %v", err)
//	}
//}
//
//func TestCurd(t *testing.T) {
//	ctx := context.Background()
//	cli := createTestEntClient(t)
//	m := &mapper.CopierMapper[User, ent.User]{}
//	r := createUserRepo(m)
//
//	// 初始 count 应为 0
//	cnt, err := r.Count(ctx, cli.User.Query(), nil)
//	if err != nil {
//		t.Fatalf("count failed: %v", err)
//	}
//	if cnt != 0 {
//		t.Fatalf("expected count 0, got %d", cnt)
//	}
//
//	// 创建一条记录（使用 ent client 直接创建）
//	createBuilder := cli.User.Create()
//	u, err := r.Create(ctx, createBuilder, &User{Name: "Alice", Age: trans.Ptr(uint32(30))}, nil, func(dto *User) {
//		createBuilder.
//			SetName(dto.GetName()).
//			SetNillableAge(dto.Age)
//	})
//	if err != nil {
//		t.Fatalf("create user failed: %v", err)
//	}
//	if u == nil {
//		t.Fatalf("created user is nil")
//	}
//
//	// 创建后 count 应为 1
//	cnt2, err := r.Count(ctx, cli.User.Query(), nil)
//	if err != nil {
//		t.Fatalf("count after create failed: %v", err)
//	}
//	if cnt2 != 1 {
//		t.Fatalf("expected count 1 after create, got %d", cnt2)
//	}
//
//	// Exists 应返回 true
//	exists, err := r.Exists(ctx, cli.User.Query(), nil)
//	if err != nil {
//		t.Fatalf("exists check failed: %v", err)
//	}
//	if !exists {
//		t.Fatalf("expected exists == true")
//	}
//
//	// ListWithPaging 应返回包含该条记录的结果
//	req := &paginationV1.PagingRequest{
//		PageSize: trans.Ptr(uint32(10)),
//		Page:     trans.Ptr(uint32(1)),
//		FilterExpr: &paginationV1.FilterExpr{
//			Conditions: []*paginationV1.FilterCondition{
//				{
//					Field: "name",
//					Value: trans.Ptr("Alice"),
//					Op:    paginationV1.Operator_EQ,
//				},
//			},
//			Type: paginationV1.ExprType_AND,
//		},
//	}
//	listQuery := cli.Debug().User.Query()
//	res, err := r.ListWithPaging(ctx, listQuery, listQuery.Clone(), req)
//	if err != nil {
//		t.Fatalf("list with paging failed: %v", err)
//	}
//	if res == nil {
//		t.Fatalf("expected non-nil result from ListWithPaging")
//	}
//	if res.Total != 1 {
//		t.Fatalf("expected total 1 in list result, got %d", res.Total)
//	}
//
//	// Delete 调用
//	delBuilder := cli.User.Delete()
//	delBuilder.Where()
//	if _, err = r.Delete(ctx, delBuilder, nil); err != nil {
//		t.Fatalf("delete via repository failed: %v", err)
//	}
//
//	// 删除后 count 应为 0
//	cnt3, err := r.Count(ctx, cli.User.Query(), nil)
//	if err != nil {
//		t.Fatalf("count failed: %v", err)
//	}
//	if cnt3 != 0 {
//		t.Fatalf("expected count 0, got %d", cnt)
//	}
//}
