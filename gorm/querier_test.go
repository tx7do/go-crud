package gorm

import (
	"context"
	"testing"

	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/tx7do/go-utils/mapper"

	paginationV1 "github.com/tx7do/go-curd/api/gen/go/pagination/v1"
)

// 测试用实体与 DTO
type testUserEntity struct {
	ID   uint `gorm:"primarykey"`
	Name string
	Age  int
}

type testUserDTO struct {
	ID   uint
	Name string
	Age  int
}

func openTestDBForQuerier(t *testing.T) *gorm.DB {
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

func TestQuerier_Count_List_Get(t *testing.T) {
	db := openTestDBForQuerier(t)
	ctx := context.Background()

	// seed data
	seedUsers(t, db,
		testUserEntity{Name: "alice", Age: 20},
		testUserEntity{Name: "bob", Age: 30},
		testUserEntity{Name: "carol", Age: 40},
	)

	// 使用零值 CopierMapper（如果项目有构造函数可替换）
	m := &mapper.CopierMapper[testUserDTO, testUserEntity]{}
	q := NewQuerier[testUserDTO, testUserEntity](m)

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

func TestQuerier_ListWithPagination_Various(t *testing.T) {
	db := openTestDBForQuerier(t)
	ctx := context.Background()

	seedUsers(t, db,
		testUserEntity{Name: "alice", Age: 20},
		testUserEntity{Name: "bob", Age: 30},
		testUserEntity{Name: "carol", Age: 40},
	)

	m := &mapper.CopierMapper[testUserDTO, testUserEntity]{}
	q := NewQuerier[testUserDTO, testUserEntity](m)

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
