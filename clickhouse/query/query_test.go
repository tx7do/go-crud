package query

import (
	"testing"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/stretchr/testify/assert"
)

func TestQueryBuilder(t *testing.T) {
	logger := log.NewHelper(log.DefaultLogger)
	qb := NewQueryBuilder("test_table", logger)

	// 测试 Select 方法
	qb.Select("id", "name")
	query, params := qb.Build()
	assert.Contains(t, query, "SELECT id, name FROM test_table")

	// 测试 Distinct 方法
	qb.Distinct()
	query, _ = qb.Build()
	assert.Contains(t, query, "SELECT DISTINCT id, name FROM test_table")

	// 测试 Where 方法
	qb.Where("id > ?", 10).Where("name = ?", "example")
	query, params = qb.Build()
	assert.Contains(t, query, "WHERE id > ? AND name = ?")
	assert.Equal(t, []interface{}{10, "example"}, params)

	// 测试 OrderBy 方法
	qb.OrderBy("name", false)
	query, _ = qb.Build()
	assert.Contains(t, query, "ORDER BY name ASC")

	// 测试 GroupBy 方法
	qb.GroupBy("category")
	query, _ = qb.Build()
	assert.Contains(t, query, "GROUP BY category")

	// 测试 Having 方法
	qb.Having("COUNT(id) > ?", 5)
	query, params = qb.Build()
	assert.Contains(t, query, "HAVING COUNT(id) > ?")
	assert.Equal(t, []interface{}{10, "example", 5}, params)

	// Limit 和 Offset
	qb.Limit(10).Offset(20)
	query, _ = qb.Build()
	assert.Contains(t, query, "LIMIT 10")
	assert.Contains(t, query, "OFFSET 20")

	// UseIndex / CacheResult / EnableDebug
	qb.UseIndex("idx_name")
	qb.CacheResult()
	qb.EnableDebug()
	query, _ = qb.Build()
	assert.Contains(t, query, "USE INDEX (idx_name)")
	assert.Contains(t, query, "/* CACHE */")
	assert.True(t, qb.debug)

	// Final / Sample 会修改 table 字符串并应出现在 FROM 子句中
	qb.Final()
	query, _ = qb.Build()
	assert.Contains(t, query, "test_table FINAL")

	qb.Sample(0.1)
	query, _ = qb.Build()
	assert.Contains(t, query, "SAMPLE")

	// LimitBy 应当在 orderBy 中包含 LIMIT BY 片段
	qb.LimitBy(5, "name")
	query, _ = qb.Build()
	assert.Contains(t, query, "LIMIT 5 BY (name)")

	// PreWhere 前置参数应出现在 params 首位，Build 输出仍为 WHERE ...
	qb.PreWhere("status = ?", "active")
	query, params = qb.Build()
	assert.Contains(t, query, "WHERE status = ?")
	assert.GreaterOrEqual(t, len(params), 1)
	assert.Equal(t, "active", params[0])

	// 边界情况：空列名与无效条件应 panic
	assert.Panics(t, func() {
		qb.Select("")
	})
	assert.Panics(t, func() {
		qb.Where("id = 1; DROP TABLE test_table")
	})
}

func TestBuilder_BasicAndAdvanced(t *testing.T) {
	logger := log.NewHelper(log.DefaultLogger)
	qb := NewQueryBuilder("test_table", logger)

	// Select / Build basic
	qb.Select("id", "name")
	query, params := qb.Build()
	assert.Contains(t, query, "SELECT id, name FROM test_table")
	assert.Equal(t, 0, len(params))

	// Distinct
	qb.Distinct()
	query, _ = qb.Build()
	assert.Contains(t, query, "SELECT DISTINCT id, name FROM test_table")

	// Where + params accumulation
	qb.Where("id > ?", 10).Where("name = ?", "example")
	query, params = qb.Build()
	assert.Contains(t, query, "WHERE id > ? AND name = ?")
	assert.Equal(t, []interface{}{10, "example"}, params)

	// OrderBy / GroupBy / Having (having adds param)
	qb.OrderBy("name", false)
	query, _ = qb.Build()
	assert.Contains(t, query, "ORDER BY name ASC")

	qb.GroupBy("category")
	query, _ = qb.Build()
	assert.Contains(t, query, "GROUP BY category")

	qb.Having("COUNT(id) > ?", 5)
	query, params = qb.Build()
	assert.Contains(t, query, "HAVING COUNT(id) > ?")
	assert.Equal(t, []interface{}{10, "example", 5}, params)

	// Limit / Offset
	qb.Limit(10).Offset(20)
	query, _ = qb.Build()
	assert.Contains(t, query, "LIMIT 10")
	assert.Contains(t, query, "OFFSET 20")

	// UseIndex / CacheResult / EnableDebug
	qb.UseIndex("idx_name")
	qb.CacheResult()
	qb.EnableDebug()
	query, _ = qb.Build()
	assert.Contains(t, query, "USE INDEX (idx_name)")
	assert.Contains(t, query, "/* CACHE */")
	assert.True(t, qb.debug)

	// Final / Sample affect table string
	qb.Final()
	query, _ = qb.Build()
	assert.Contains(t, query, "test_table FINAL")

	qb.Sample(0.1)
	query, _ = qb.Build()
	assert.Contains(t, query, "SAMPLE")

	// LimitBy appends into orderBy slice and should appear in ORDER BY clause when built
	qb.LimitBy(5, "name")
	query, _ = qb.Build()
	assert.Contains(t, query, "LIMIT BY 5 (name)")

	// PreWhere should prepend condition and its args should be first in params
	qb.PreWhere("status = ?", "active")
	query, params = qb.Build()
	assert.Contains(t, query, "WHERE status = ?")
	assert.GreaterOrEqual(t, len(params), 1)
	assert.Equal(t, "active", params[0])
}

func TestBuilder_InvalidInputs_Panic(t *testing.T) {
	logger := log.NewHelper(log.DefaultLogger)
	qb := NewQueryBuilder("test_table", logger)

	// invalid column name should panic
	assert.Panics(t, func() {
		qb.Select("")
	})

	// invalid condition should panic
	assert.Panics(t, func() {
		qb.Where("id = 1; DROP TABLE test_table")
	})
}
