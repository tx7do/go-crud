package filter

import (
	"fmt"
	"strings"

	_ "github.com/go-kratos/kratos/v2/encoding/json"
	"go.einride.tech/aip/filtering"
	v1alpha1 "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

type FilterStringConverter struct {
}

func NewFilterStringConverter() *FilterStringConverter {
	return &FilterStringConverter{}
}

func (fsc *FilterStringConverter) Convert(filterString string) (*paginationV1.FilterExpr, error) {
	if len(filterString) == 0 {
		return nil, nil
	}

	var parser filtering.Parser
	parser.Init(filterString)
	parsedExpr, err := parser.Parse()
	if err != nil {
		return nil, err
	}

	filterExpr := &paginationV1.FilterExpr{
		Type: paginationV1.ExprType_AND,
	}

	fsc.walk(filterExpr, parsedExpr.GetExpr())

	return filterExpr, nil
}

// mapOperator 映射 AIP 运算符到 paginationV1.Operator
func (fsc *FilterStringConverter) mapOperator(op string) paginationV1.Operator {
	switch op {
	case "=":
		return paginationV1.Operator_EQ
	case "!=", "not":
		return paginationV1.Operator_NEQ
	case "<":
		return paginationV1.Operator_LT
	case "<=":
		return paginationV1.Operator_LTE
	case ">":
		return paginationV1.Operator_GT
	case ">=":
		return paginationV1.Operator_GTE
	case "isnull":
		return paginationV1.Operator_IS_NULL
	case "isnotnull":
		return paginationV1.Operator_IS_NOT_NULL
	case "contains":
		return paginationV1.Operator_CONTAINS
	case "startswith", ":":
		return paginationV1.Operator_STARTS_WITH
	case "endswith":
		return paginationV1.Operator_ENDS_WITH
	case "in":
		return paginationV1.Operator_IN
	case "notin":
		return paginationV1.Operator_NIN
	default:
		return paginationV1.Operator_OPERATOR_UNSPECIFIED
	}
}

// ConstantString 将 AIP Constant 转换为字符串表示
func ConstantString(expr *v1alpha1.Constant) string {
	if expr == nil {
		return ""
	}

	switch expr.ConstantKind.(type) {
	case *v1alpha1.Constant_StringValue:
		return expr.GetStringValue()
	case *v1alpha1.Constant_BoolValue:
		if expr.GetBoolValue() {
			return "true"
		}
		return "false"
	case *v1alpha1.Constant_Int64Value:
		return fmt.Sprintf("%d", expr.GetInt64Value())
	case *v1alpha1.Constant_Uint64Value:
		return fmt.Sprintf("%d", expr.GetUint64Value())
	case *v1alpha1.Constant_DoubleValue:
		return fmt.Sprintf("%f", expr.GetDoubleValue())
	default:
		return ""
	}
}

// invertOperator 将 Operator 取反（尽量映射到有意义的否定枚举）
func (fsc *FilterStringConverter) invertOperator(op paginationV1.Operator) paginationV1.Operator {
	switch op {
	case paginationV1.Operator_EQ:
		return paginationV1.Operator_NEQ
	case paginationV1.Operator_NEQ:
		return paginationV1.Operator_EQ
	case paginationV1.Operator_IN:
		return paginationV1.Operator_NIN
	case paginationV1.Operator_NIN:
		return paginationV1.Operator_IN
	case paginationV1.Operator_IS_NULL:
		return paginationV1.Operator_IS_NOT_NULL
	case paginationV1.Operator_IS_NOT_NULL:
		return paginationV1.Operator_IS_NULL
	// 对于没有明确否定的操作符，退回到 NEQ 或保持不变
	default:
		return paginationV1.Operator_NEQ
	}
}

// invertFilterExpr 对 FilterExpr 及其子组做取反（修改原对象）
func (fsc *FilterStringConverter) invertFilterExpr(fe *paginationV1.FilterExpr) {
	if fe == nil {
		return
	}

	// 切换组类型（德摩根律）
	if fe.Type == paginationV1.ExprType_AND {
		fe.Type = paginationV1.ExprType_OR
	} else if fe.Type == paginationV1.ExprType_OR {
		fe.Type = paginationV1.ExprType_AND
	}

	// 取反当前条件的运算符
	for _, c := range fe.Conditions {
		if c == nil {
			continue
		}
		c.Op = fsc.invertOperator(c.Op)
	}

	// 递归处理子组
	for _, g := range fe.Groups {
		fsc.invertFilterExpr(g)
	}
}

// walk 递归遍历 AIP Expr 并构建 FilterExpr
func (fsc *FilterStringConverter) walk(out *paginationV1.FilterExpr, in *v1alpha1.Expr) {
	if in == nil {
		return
	}

	switch kind := in.ExprKind.(type) {
	case *v1alpha1.Expr_ConstExpr:
		// 处理常量表达式
		out.Conditions = append(out.Conditions, &paginationV1.FilterCondition{
			ValueOneof: &paginationV1.FilterCondition_Value{
				Value: ConstantString(kind.ConstExpr),
			},
		})

	case *v1alpha1.Expr_IdentExpr:
		// 处理标识符表达式
		out.Conditions = append(out.Conditions, &paginationV1.FilterCondition{
			Field: kind.IdentExpr.Name,
		})

	case *v1alpha1.Expr_CallExpr:
		// 处理函数调用表达式（运算符）
		op := kind.CallExpr.Function
		op = strings.ToLower(op)
		//fmt.Printf("Processing operator: %s\n", op)

		if op == "and" || op == "or" {
			if op == "and" {
				out.Type = paginationV1.ExprType_AND
			} else {
				out.Type = paginationV1.ExprType_OR
			}

			for _, arg := range kind.CallExpr.Args {
				subExpr := &paginationV1.FilterExpr{}
				fsc.walk(subExpr, arg)
				if len(subExpr.Conditions) > 0 {
					out.Conditions = append(out.Conditions, subExpr.Conditions...)
				}
				if len(subExpr.Groups) > 0 {
					out.Groups = append(out.Groups, subExpr.Groups...)
				}
			}
		} else {
			// 处理其他运算符
			condition := &paginationV1.FilterCondition{
				Op: fsc.mapOperator(op),
			}
			//fmt.Println("Operator mapped to:", condition.Op, kind.CallExpr.Args)

			switch op {
			case "isnull", "isnotnull":
				// 这些运算符只需要一个字段参数
				if len(kind.CallExpr.Args) >= 1 {
					if identExpr, ok := kind.CallExpr.Args[0].ExprKind.(*v1alpha1.Expr_IdentExpr); ok {
						condition.Field = identExpr.IdentExpr.Name
					}
				}
				out.Conditions = append(out.Conditions, condition)
				return

			case "in", "notin":
				if len(kind.CallExpr.Args) >= 2 {
					// 假设第一个参数是字段，后续参数是值

					if identExpr, ok := kind.CallExpr.Args[0].ExprKind.(*v1alpha1.Expr_IdentExpr); ok {
						condition.Field = identExpr.IdentExpr.Name
					}

					for _, arg := range kind.CallExpr.Args {
						if identExpr, ok := arg.ExprKind.(*v1alpha1.Expr_IdentExpr); ok {
							condition.Field = identExpr.IdentExpr.Name
						}

						if constExpr, ok := arg.ExprKind.(*v1alpha1.Expr_ConstExpr); ok {
							condition.Values = append(condition.Values, ConstantString(constExpr.ConstExpr))
						}
					}

					out.Conditions = append(out.Conditions, condition)
					return
				}

			case "not":
				if len(kind.CallExpr.Args) == 1 {
					inner := kind.CallExpr.Args[0]

					// 先把内部表达式解析成子 FilterExpr
					sub := &paginationV1.FilterExpr{
						Type: paginationV1.ExprType_AND,
					}
					fsc.walk(sub, inner)

					// 防御性重试（如果需要）
					if len(sub.Conditions) == 0 && len(sub.Groups) == 0 {
						if inner.GetCallExpr() != nil {
							tmp := &paginationV1.FilterExpr{}
							fsc.walk(tmp, inner)
							sub = tmp
						}
					}

					// 无法解析则返回
					if sub == nil || (len(sub.Conditions) == 0 && len(sub.Groups) == 0) {
						return
					}

					// 对解析结果取反（运算符与组类型）
					fsc.invertFilterExpr(sub)

					// 如果当前 out 为空：提升 sub 的类型与内容到 out
					if len(out.Conditions) == 0 && len(out.Groups) == 0 {
						out.Type = sub.Type
						if len(sub.Conditions) > 0 {
							out.Conditions = append(out.Conditions, sub.Conditions...)
						}
						if len(sub.Groups) > 0 {
							out.Groups = append(out.Groups, sub.Groups...)
						}
						return
					}

					// 否则把 sub 作为子组追加，保留父节点类型
					out.Groups = append(out.Groups, sub)
					return
				}
			}

			if len(kind.CallExpr.Args) >= 2 {
				// 假设第一个参数是字段，第二个参数是值
				if identExpr, ok := kind.CallExpr.Args[0].ExprKind.(*v1alpha1.Expr_IdentExpr); ok {
					condition.Field = identExpr.IdentExpr.Name
				}
				if constExpr, ok := kind.CallExpr.Args[1].ExprKind.(*v1alpha1.Expr_ConstExpr); ok {
					condition.ValueOneof = &paginationV1.FilterCondition_Value{
						Value: ConstantString(constExpr.ConstExpr),
					}
				}
			}
			out.Conditions = append(out.Conditions, condition)
		}

	case *v1alpha1.Expr_SelectExpr:
		// 处理字段选择表达式
		if operandIdent, ok := kind.SelectExpr.Operand.ExprKind.(*v1alpha1.Expr_IdentExpr); ok {
			out.Conditions = append(out.Conditions, &paginationV1.FilterCondition{
				Field: operandIdent.IdentExpr.Name + "." + kind.SelectExpr.Field,
			})
		}

	default:
		// 处理其他类型的表达式（如果有需要）
	}
}
