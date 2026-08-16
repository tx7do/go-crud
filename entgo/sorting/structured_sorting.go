package sorting

import (
	"strings"

	"entgo.io/ent/dialect/sql"
	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/entgo/ent"
)

// columnAllowed 判断字段是否应被允许用于当前表。
// 仅当表已被白名单映射（实体表）且列不属于该表时拒绝；表未映射时 fail-open，
// 保持与无白名单时一致的旧行为。
func columnAllowed(table, col string) bool {
	err := ent.CheckColumn(table, col)
	if err == nil {
		return true
	}
	if strings.Contains(err.Error(), "unknown table") {
		return true
	}
	return false
}

type StructuredSorting struct {
}

func NewStructuredSorting() *StructuredSorting {
	return &StructuredSorting{}
}

func (ss StructuredSorting) BuildSelector(orders []*paginationV1.Sorting) (func(s *sql.Selector), error) {
	if len(orders) == 0 {
		return nil, nil
	}

	return func(s *sql.Selector) {
		for _, order := range orders {
			if order == nil || order.GetField() == "" {
				continue
			}
			// 硬性标识符校验（先于列白名单）：白名单对未注册表 fail-open，
			// 含 SQL 元字符的排序字段必须无条件拒绝，防止 ent Ident 原样写入。
			if !ent.IsValidFieldName(order.GetField()) {
				continue
			}
			// 字段白名单：仅允许属于当前表（s.TableName()）的真实列，拒绝跨列访问。
			if !columnAllowed(s.TableName(), order.GetField()) {
				continue
			}

			buildOrderBySelector(s, order.Field, order.GetDirection() == paginationV1.Sorting_DESC)
		}
	}, nil
}

// BuildSelectorWithDefaultField 构建排序选择器
// - orderBys: 排序字段列表
// - defaultOrderField: 默认排序字段
// - defaultDesc: 默认是否降序
func (ss StructuredSorting) BuildSelectorWithDefaultField(orders []*paginationV1.Sorting, defaultOrderField string, defaultDesc bool) (func(s *sql.Selector), error) {
	if len(orders) == 0 && defaultOrderField != "" {
		return func(s *sql.Selector) {
			// 默认排序字段同样需通过硬性标识符与白名单校验。
			if !ent.IsValidFieldName(defaultOrderField) {
				return
			}
			if !columnAllowed(s.TableName(), defaultOrderField) {
				return
			}
			buildOrderBySelector(s, defaultOrderField, defaultDesc)
		}, nil
	} else {
		return ss.BuildSelector(orders)
	}
}
