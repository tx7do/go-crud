package gorm

import (
	"context"
	"testing"

	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/tx7do/go-utils/mapper"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

// 测试用实体与 DTO
type testUserEntity struct {
	ID   uint `gorm:"primarykey"`
	Name string
	Age  int
}

func openTestDBForRepository(t *testing.T) *gorm.DB {
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.AutoMigrate(&testUserEntity{}); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}
	return db
}

func seedUsers(t *testing.T, db *gorm.DB, users ...testUserEntity) {
	for _, u := range users {
		if err := db.Create(&u).Error; err != nil {
			t.Fatalf("seed user failed: %v", err)
		}
	}
}

func seedUsersForUpdater(t *testing.T, db *gorm.DB, users ...testUserEntity) {
	for _, u := range users {
		if err := db.Create(&u).Error; err != nil {
			t.Fatalf("seed user failed: %v", err)
		}
	}
}

func TestRepository_Count_List_Get(t *testing.T) {
	db := openTestDBForRepository(t)
	ctx := context.Background()

	// seed data
	seedUsers(t, db,
		testUserEntity{Name: "alice", Age: 20},
		testUserEntity{Name: "bob", Age: 30},
		testUserEntity{Name: "carol", Age: 40},
	)

	// 使用零值 CopierMapper（如果项目有构造函数可替换）
	m := &mapper.CopierMapper[User, testUserEntity]{}
	q := NewRepository[User, testUserEntity](m)

	// Count: 无 selectors -> 返回全部数量
	cnt, err := q.Count(ctx, db, nil)
	if err != nil {
		t.Fatalf("Count error: %v", err)
	}
	if cnt != 3 {
		t.Fatalf("expected count 3, got %d", cnt)
	}

	// ListWithPaging: 空请求应返回所有记录（默认不出错）
	res, err := q.ListWithPaging(ctx, db, &paginationV1.PagingRequest{})
	if err != nil {
		t.Fatalf("ListWithPaging error: %v", err)
	}
	if res == nil {
		t.Fatalf("ListWithPaging returned nil result")
	}
	if int(res.Total) != 3 {
		t.Fatalf("expected total 3, got %d", res.Total)
	}
	// Items 长度至少为 0，mapper 可能需要有效实现以返回 DTO 内容；此处主要断言数量
	if len(res.Items) != 3 {
		t.Fatalf("expected 3 items, got %d", len(res.Items))
	}

	// Get: 取第一条记录
	dto, err := q.Get(ctx, db, nil)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if dto == nil {
		t.Fatalf("Get returned nil dto")
	}

	// Only alias
	dto2, err := q.Only(ctx, db, nil)
	if err != nil {
		t.Fatalf("Only error: %v", err)
	}
	if dto2 == nil {
		t.Fatalf("Only returned nil dto")
	}
}

func TestRepository_ListWithPagination_Various(t *testing.T) {
	db := openTestDBForRepository(t)
	ctx := context.Background()

	seedUsers(t, db,
		testUserEntity{Name: "alice", Age: 20},
		testUserEntity{Name: "bob", Age: 30},
		testUserEntity{Name: "carol", Age: 40},
	)

	m := &mapper.CopierMapper[User, testUserEntity]{}
	q := NewRepository[User, testUserEntity](m)

	cases := []struct {
		name string
		req  *paginationV1.PaginationRequest
		want int
	}{
		{
			name: "no_paging_all",
			req: &paginationV1.PaginationRequest{
				PaginationType: &paginationV1.PaginationRequest_NoPaging{},
			},
			want: 3,
		},
		{
			name: "field_mask_name_only",
			req: &paginationV1.PaginationRequest{
				PaginationType: &paginationV1.PaginationRequest_NoPaging{},
				FieldMask:      &fieldmaskpb.FieldMask{Paths: []string{"Name"}},
			},
			want: 3,
		},
		{
			name: "order_by_age_desc",
			req: &paginationV1.PaginationRequest{
				PaginationType: &paginationV1.PaginationRequest_NoPaging{},
				OrderBy:        []string{"age desc"},
			},
			want: 3,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := q.ListWithPagination(ctx, db, tc.req)
			if err != nil {
				t.Fatalf("ListWithPagination(%s) error: %v", tc.name, err)
			}
			if res == nil {
				t.Fatalf("ListWithPagination(%s) returned nil", tc.name)
			}
			if int(res.Total) != tc.want {
				t.Fatalf("ListWithPagination(%s) expected total %d, got %d", tc.name, tc.want, res.Total)
			}
			if len(res.Items) != tc.want {
				t.Fatalf("ListWithPagination(%s) expected %d items, got %d", tc.name, tc.want, len(res.Items))
			}
		})
	}
}

func TestRepository_Create_Get_Update_Delete_Exists_Count_Upsert(t *testing.T) {
	db := openTestDBForRepository(t)
	ctx := context.Background()

	// 创建 mapper 与 repository
	m := mapper.NewCopierMapper[User, testUserEntity]()
	r := NewRepository[User, testUserEntity](m)

	// 初始计数应为 0
	cnt, err := r.Count(ctx, db, nil)
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if cnt != 0 {
		t.Fatalf("expected initial count 0, got %d", cnt)
	}

	// Create
	dto := &User{
		Name: "alice",
		Age:  30,
	}
	created, err := r.Create(ctx, db, dto, nil)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if created == nil || created.Id == 0 {
		t.Fatalf("Create returned invalid dto: %+v", created)
	}

	// Count should be 1
	cnt, err = r.Count(ctx, db, nil)
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("expected count 1, got %d", cnt)
	}

	// Get by where
	got, err := r.Get(ctx, db.Where("id = ?", created.Id), nil)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got == nil || got.Name != "alice" || got.Age != 30 {
		t.Fatalf("Get returned wrong data: %+v", got)
	}

	// Update (modify name & age). 把 ID 赋给 dto 以便 mapper 转换包含主键
	updatedDTO := &User{Id: created.Id, Name: "alice-updated", Age: 31}
	updated, err := r.Update(ctx, db.Where("id = ?", created.Id), updatedDTO, nil)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if updated == nil || updated.Name != "alice-updated" || updated.Age != 31 {
		t.Fatalf("Update returned wrong data: %+v", updated)
	}

	// Exists true
	ok, err := r.Exists(ctx, db.Where("id = ?", created.Id))
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !ok {
		t.Fatalf("Exists expected true but got false")
	}

	// UpdateX 返回受影响行数
	rows, err := r.UpdateX(ctx, db.Where("id = ?", created.Id), &User{Id: created.Id, Name: "alice-x", Age: 32}, nil)
	if err != nil {
		t.Fatalf("UpdateX failed: %v", err)
	}
	if rows == 0 {
		t.Fatalf("UpdateX expected rows>0 but got %d", rows)
	}

	// Delete
	delRows, err := r.Delete(ctx, db.Where("id = ?", created.Id), false)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if delRows == 0 {
		t.Fatalf("Delete expected rows>0 but got %d", delRows)
	}

	// Exists false after delete
	ok, err = r.Exists(ctx, db.Where("id = ?", created.Id))
	if err != nil {
		t.Fatalf("Exists after delete failed: %v", err)
	}
	if ok {
		t.Fatalf("Exists after delete expected false but got true")
	}

	// Upsert: 插入新记录（无约束时相当于 Insert）
	upDto := &User{Name: "upsert-user", Age: 20}
	upRes, err := r.Upsert(ctx, db, upDto, nil)
	if err != nil {
		t.Fatalf("Upsert failed: %v", err)
	}
	if upRes == nil || upRes.Id == 0 {
		t.Fatalf("Upsert returned invalid dto: %+v", upRes)
	}

	// UpsertX 返回受影响行数
	rowsAffected, err := r.UpsertX(ctx, db, &User{Name: "upsert2", Age: 21}, nil)
	if err != nil {
		t.Fatalf("UpsertX failed: %v", err)
	}
	if rowsAffected == 0 {
		t.Fatalf("UpsertX expected rows>0 but got %d", rowsAffected)
	}
}

func TestRepository_CreateX_UpdateX_DeleteWithFilters(t *testing.T) {
	db := openTestDBForRepository(t)
	ctx := context.Background()

	m := mapper.NewCopierMapper[User, testUserEntity]()
	r := NewRepository[User, testUserEntity](m)

	// 使用 CreateX 插入多条
	rows, err := r.CreateX(ctx, db, &User{Name: "u1", Age: 10}, nil)
	if err != nil {
		t.Fatalf("CreateX failed: %v", err)
	}
	if rows == 0 {
		t.Fatalf("CreateX expected rows>0 but got %d", rows)
	}

	rows2, err := r.CreateXWithFilters(ctx, db, nil, &User{Name: "u2", Age: 11}, nil)
	if err != nil {
		t.Fatalf("CreateXWithFilters failed: %v", err)
	}
	if rows2 == 0 {
		t.Fatalf("CreateXWithFilters expected rows>0 but got %d", rows2)
	}

	// 更新所有 age < 20 的用户为 age = 99，使用 whereSelectors
	whereSelectors := []func(*gorm.DB) *gorm.DB{
		func(db *gorm.DB) *gorm.DB { return db.Where("age < ?", 20) },
	}
	updatedRows, err := r.UpdateXWithFilters(ctx, db, whereSelectors, &User{Age: 99}, nil)
	if err != nil {
		t.Fatalf("UpdateXWithFilters failed: %v", err)
	}
	if updatedRows == 0 {
		t.Fatalf("UpdateXWithFilters expected rows>0 but got %d", updatedRows)
	}

	// 删除 age = 99 的用户
	delRows, err := r.DeleteWithFilters(ctx, db, []func(*gorm.DB) *gorm.DB{
		func(db *gorm.DB) *gorm.DB { return db.Where("age = ?", 99) },
	})
	if err != nil {
		t.Fatalf("DeleteWithFilters failed: %v", err)
	}
	if delRows == 0 {
		t.Fatalf("DeleteWithFilters expected rows>0 but got %d", delRows)
	}
}

func TestRepository_UpdateAndUpdateX(t *testing.T) {
	db := openTestDBForRepository(t)
	ctx := context.Background()

	m := mapper.NewCopierMapper[User, testUserEntity]()
	r := NewRepository[User, testUserEntity](m)

	// Create 初始记录
	created, err := r.Create(ctx, db, &User{Name: "bob", Age: 25}, nil)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if created == nil || created.Id == 0 {
		t.Fatalf("Create returned invalid result: %+v", created)
	}

	// Update: 修改 name 和 age
	updateDTO := &User{Id: created.Id, Name: "bob-upd", Age: 26}
	updated, err := r.Update(ctx, db.Where("id = ?", created.Id), updateDTO, nil)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if updated.Name != "bob-upd" || updated.Age != 26 {
		t.Fatalf("Update did not apply correctly, got: %+v", updated)
	}

	// Update 使用 updateMask 仅修改 name，不改变 age
	mask := &fieldmaskpb.FieldMask{Paths: []string{"id", "name"}}
	updateMaskDTO := &User{Id: created.Id, Name: "bob-name-only", Age: 99} // age 应被忽略
	updated2, err := r.Update(ctx, db.Where("id = ?", created.Id), updateMaskDTO, mask)
	if err != nil {
		t.Fatalf("Update with mask failed: %v", err)
	}
	if updated2.Name != "bob-name-only" {
		t.Fatalf("Update with mask didn't update name, got: %+v", updated2)
	}
	if updated2.Age != 26 { // age 应保持为上一次的 26
		t.Fatalf("Update with mask unexpectedly changed age, got: %+v", updated2)
	}

	// UpdateX: 通过 UpdateX 返回受影响行数并验证结果
	rows, err := r.UpdateX(ctx, db.Where("id = ?", created.Id), &User{Id: created.Id, Age: 30}, nil)
	if err != nil {
		t.Fatalf("UpdateX failed: %v", err)
	}
	if rows == 0 {
		t.Fatalf("UpdateX expected rows>0 but got %d", rows)
	}
	got, err := r.Get(ctx, db.Where("id = ?", created.Id), nil)
	if err != nil {
		t.Fatalf("Get after UpdateX failed: %v", err)
	}
	if got.Age != 30 {
		t.Fatalf("UpdateX did not change age, expected 30 got %d", got.Age)
	}

	// UpdateXWithFilters: 更新满足条件的记录
	whereSelectors := []func(*gorm.DB) *gorm.DB{
		func(db *gorm.DB) *gorm.DB { return db.Where("age >= ?", 30) },
	}
	rows2, err := r.UpdateXWithFilters(ctx, db, whereSelectors, &User{Age: 40}, nil)
	if err != nil {
		t.Fatalf("UpdateXWithFilters failed: %v", err)
	}
	if rows2 == 0 {
		t.Fatalf("UpdateXWithFilters expected rows>0 but got %d", rows2)
	}
	got2, err := r.Get(ctx, db.Where("id = ?", created.Id), nil)
	if err != nil {
		t.Fatalf("Get after UpdateXWithFilters failed: %v", err)
	}
	if got2.Age != 40 {
		t.Fatalf("UpdateXWithFilters did not set age to 40, got %d", got2.Age)
	}
}
