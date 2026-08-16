package filter

import (
	"context"
	"fmt"
	"strings"

	"gorm.io/gorm"

	"github.com/tx7do/go-wind-plugins/encoding"
	_ "github.com/tx7do/go-wind-plugins/encoding/json"
	"github.com/tx7do/go-wind/log"

	"github.com/tx7do/go-utils/stringcase"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

// StructuredFilter 基于 FilterExpr 的 GORM 过滤器
type StructuredFilter struct {
	codec     encoding.Codec
	processor *Processor
}

func NewStructuredFilter() *StructuredFilter {
	return &StructuredFilter{
		codec:     encoding.GetCodec("json"),
		processor: NewProcessor(),
	}
}

// BuildSelectors 将 FilterExpr 转为一组可应用于 *gorm.DB 的闭包
func (sf StructuredFilter) BuildSelectors(expr *paginationV1.FilterExpr) ([]func(*gorm.DB) *gorm.DB, error) {
	var sels []func(*gorm.DB) *gorm.DB

	if expr == nil {
		// 返回空 slice 以保持兼容测试（也可返回 nil）
		return sels, nil
	}

	// 未指定类型视为跳过（测试期望返回 nil）
	if expr.GetType() == paginationV1.ExprType_EXPR_TYPE_UNSPECIFIED {
		log.Warn(context.Background(), "Skipping unspecified FilterExpr")
		return nil, nil
	}

	sel, err := sf.buildFilterSelector(expr)
	if err != nil {
		return nil, err
	}
	if sel != nil {
		sels = append(sels, sel)
	}
	return sels, nil
}

// buildFilterSelector 将单个 FilterExpr 转为 *gorm.DB 闭包（递归处理组）
//
// 注意：gorm v1.31.2 的 Statement.BuildCondition 不支持 func(*gorm.DB) *gorm.DB
// 闭包（WHERE/OR 里的闭包永不执行，整个过滤静默消失）。因此这里把表达式树
// 递归展开为参数化 SQL 片段（值全部走 ? 占位符），再按 AND/OR 连接成单个
// Where 调用；字段经 Processor.BuildExpression 的白名单校验，非法即报错
// （fail-closed，而不是静默丢弃过滤）。
func (sf StructuredFilter) buildFilterSelector(expr *paginationV1.FilterExpr) (func(*gorm.DB) *gorm.DB, error) {
	if expr == nil {
		log.Warn(context.Background(), "Skipping nil FilterExpr")
		return nil, nil
	}
	if expr.GetType() == paginationV1.ExprType_EXPR_TYPE_UNSPECIFIED {
		log.Warn(context.Background(), "Skipping unspecified FilterExpr")
		return nil, nil
	}

	closure := func(db *gorm.DB) *gorm.DB {
		if db == nil {
			return db
		}
		frags, args, err := sf.buildExpr(db, expr)
		if err != nil {
			_ = db.AddError(err)
			return db
		}
		if len(frags) == 0 {
			return db
		}
		sep := " AND "
		if expr.GetType() == paginationV1.ExprType_OR {
			sep = " OR "
		}
		return db.Where(strings.Join(frags, sep), args...)
	}

	return closure, nil
}

// buildExpr 将 FilterExpr 递归展开为参数化 SQL 片段列表与参数。
// 片段按组内语义连接：AND 组的片段用 AND 连接，OR 组用 OR 连接；
// 子组整体作为一个带括号的片段。参数顺序与片段顺序一一对应。
func (sf StructuredFilter) buildExpr(db *gorm.DB, expr *paginationV1.FilterExpr) ([]string, []any, error) {
	if expr == nil {
		return nil, nil, nil
	}
	condFrags, condArgs, err := sf.condExprs(db, expr.GetConditions())
	if err != nil {
		return nil, nil, err
	}

	var groupFrags []string
	var groupArgs []any
	for _, g := range expr.GetGroups() {
		subFrags, subArgs, err := sf.buildExpr(db, g)
		if err != nil {
			return nil, nil, err
		}
		if len(subFrags) == 0 {
			continue
		}
		sep := " AND "
		if g.GetType() == paginationV1.ExprType_OR {
			sep = " OR "
		}
		groupFrags = append(groupFrags, "("+strings.Join(subFrags, sep)+")")
		groupArgs = append(groupArgs, subArgs...)
	}

	frags := append(condFrags, groupFrags...)
	args := append(condArgs, groupArgs...)
	return frags, args, nil
}

// condExprs 将条件列表展开为片段与参数。
func (sf StructuredFilter) condExprs(db *gorm.DB, conditions []*paginationV1.FilterCondition) ([]string, []any, error) {
	if len(conditions) == 0 {
		return nil, nil, nil
	}
	var frags []string
	var args []any
	for _, cond := range conditions {
		f, a, err := sf.condExpr(db, cond)
		if err != nil {
			return nil, nil, err
		}
		if f == "" {
			continue
		}
		frags = append(frags, f)
		args = append(args, a...)
	}
	return frags, args, nil
}

// condExpr 将单个 Condition 生成参数化片段，字段处理与 applyCond 一致
// （JSON 字段经 JsonbFieldExpr 生成表达式），校验失败返回错误（fail-closed）。
func (sf StructuredFilter) condExpr(db *gorm.DB, cond *paginationV1.FilterCondition) (string, []any, error) {
	if cond == nil {
		return "", nil, nil
	}
	val := ""
	switch cond.ValueOneof.(type) {
	case *paginationV1.FilterCondition_Value:
		val = cond.GetValue()
	default:
	}

	var field string
	if strings.Contains(cond.GetField(), ".") {
		// 支持 JSON 字段 (e.g. preferences.daily_email)
		parts := strings.SplitN(cond.GetField(), ".", 2)
		col := stringcase.ToSnakeCase(parts[0])
		jsonKey := parts[1]
		exprStr, _ := sf.processor.JsonbFieldExpr(db, jsonKey, col)
		if exprStr == "" {
			return "", nil, fmt.Errorf("invalid filter field %q", cond.GetField())
		}
		field = exprStr
	} else {
		field = stringcase.ToSnakeCase(cond.GetField())
	}

	expr, args, ok := sf.processor.BuildExpression(db, cond.GetOp(), field, val, cond.GetValues())
	if !ok {
		// 与各算子方法一致：空值条件跳过；非法字段/无法生成的方言报错
		if expr == "" && isSkippableEmpty(cond, val) {
			return "", nil, nil
		}
		return "", nil, fmt.Errorf("invalid filter condition on field %q", cond.GetField())
	}
	return expr, args, nil
}

// isSkippableEmpty 判断空值条件（既有行为：空值不添加条件）。
func isSkippableEmpty(cond *paginationV1.FilterCondition, val string) bool {
	if val != "" {
		return false
	}
	switch cond.GetOp() {
	case paginationV1.Operator_IN, paginationV1.Operator_NIN, paginationV1.Operator_BETWEEN:
		return len(cond.GetValues()) == 0
	default:
		return true
	}
}
