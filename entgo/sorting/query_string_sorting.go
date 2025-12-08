package sorting

import (
	"strings"

	"entgo.io/ent/dialect/sql"
)

type QueryStringSorting struct {
}

func NewQueryStringSorting() *QueryStringSorting {
	return &QueryStringSorting{}
}

// BuildSelector 构建排序选择器
// - orderBys: 排序命令列表
func (qss QueryStringSorting) BuildSelector(orderBys []string) (func(s *sql.Selector), error) {
	if len(orderBys) == 0 {
		return nil, nil
	}

	return func(s *sql.Selector) {
		for _, v := range orderBys {
			v = strings.TrimSpace(v)

			if strings.HasPrefix(v, "-") {
				// 降序
				key := v[1:]
				if len(key) == 0 {
					continue
				}

				buildOrderBySelector(s, key, true)
			} else {
				// 升序
				if len(v) == 0 {
					continue
				}

				buildOrderBySelector(s, v, false)
			}
		}
	}, nil
}

// BuildSelectorWithDefaultField 构建排序选择器
// - orderBys: 排序字段列表
// - defaultOrderField: 默认排序字段
// - defaultDesc: 默认是否降序
func (qss QueryStringSorting) BuildSelectorWithDefaultField(orderBys []string, defaultOrderField string, defaultDesc bool) (func(s *sql.Selector), error) {
	if len(orderBys) == 0 && defaultOrderField != "" {
		return func(s *sql.Selector) {
			buildOrderBySelector(s, defaultOrderField, defaultDesc)
		}, nil
	} else {
		return qss.BuildSelector(orderBys)
	}
}
