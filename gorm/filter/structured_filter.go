package filter

import (
	"strings"

	"gorm.io/gorm"

	"github.com/go-kratos/kratos/v2/encoding"
	_ "github.com/go-kratos/kratos/v2/encoding/json"
	"github.com/go-kratos/kratos/v2/log"

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
		log.Warn("Skipping unspecified FilterExpr")
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
func (sf StructuredFilter) buildFilterSelector(expr *paginationV1.FilterExpr) (func(*gorm.DB) *gorm.DB, error) {
	if expr == nil {
		log.Warn("Skipping nil FilterExpr")
		return nil, nil
	}
	if expr.GetType() == paginationV1.ExprType_EXPR_TYPE_UNSPECIFIED {
		log.Warn("Skipping unspecified FilterExpr")
		return nil, nil
	}

	// helper: 将单个 Condition 应用到 db 上
	applyCond := func(db *gorm.DB, cond *paginationV1.FilterCondition) *gorm.DB {
		if db == nil || cond == nil {
			return db
		}
		val := ""
		switch cond.ValueOneof.(type) {
		case *paginationV1.FilterCondition_Value:
			val = cond.GetValue()
		default:
		}

		// 支持 JSON 字段 (e.g. preferences.daily_email)
		if strings.Contains(cond.GetField(), ".") {
			parts := strings.SplitN(cond.GetField(), ".", 2)
			col := stringcase.ToSnakeCase(parts[0])
			jsonKey := parts[1]
			// 在运行时根据 db 方言生成表达式
			exprStr, _ := sf.processor.JsonbFieldExpr(db, jsonKey, col)
			if exprStr == "" {
				return db
			}
			return sf.processor.Process(db, cond.GetOp(), exprStr, val, cond.GetValues())
		}

		col := stringcase.ToSnakeCase(cond.GetField())
		return sf.processor.Process(db, cond.GetOp(), col, val, cond.GetValues())
	}

	// 构造闭包
	closure := func(db *gorm.DB) *gorm.DB {
		if db == nil {
			return db
		}

		switch expr.GetType() {
		case paginationV1.ExprType_AND:
			// 先处理条件（顺序 AND）
			for _, cond := range expr.GetConditions() {
				db = applyCond(db, cond)
			}
			// 再处理子组（每个子组也是 AND 语义：子组内部依据其类型处理）
			for _, g := range expr.GetGroups() {
				subSel, err := sf.buildFilterSelector(g)
				if err != nil {
					// 忽略错误，但记录
					log.Errorf("buildFilterSelector sub-group error: %v", err)
					continue
				}
				if subSel != nil {
					db = subSel(db)
				}
			}
			return db

		case paginationV1.ExprType_OR:
			// 为 OR，把所有条件和子组合并为一个 WHERE 子表达式，内部使用 Or 组合
			db = db.Where(func(tx *gorm.DB) *gorm.DB {
				first := true
				// 条件集合
				for _, cond := range expr.GetConditions() {
					if first {
						tx = applyCond(tx, cond)
						first = false
					} else {
						// 每个后续项作为 OR 子句加入
						c := cond // capture
						tx = tx.Or(func(t2 *gorm.DB) *gorm.DB {
							return applyCond(t2, c)
						})
					}
				}
				// 子组集合
				for _, g := range expr.GetGroups() {
					subSel, err := sf.buildFilterSelector(g)
					if err != nil {
						log.Errorf("buildFilterSelector sub-group error: %v", err)
						continue
					}
					if subSel == nil {
						continue
					}
					if first {
						tx = subSel(tx)
						first = false
					} else {
						s := subSel // capture
						tx = tx.Or(func(t2 *gorm.DB) *gorm.DB {
							return s(t2)
						})
					}
				}
				return tx
			})
			return db
		default:
			// 未知类型，直接返回原 db
			return db
		}
	}

	return closure, nil
}
