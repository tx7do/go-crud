package filter

import (
	"strings"

	"github.com/go-kratos/kratos/v2/encoding"
	_ "github.com/go-kratos/kratos/v2/encoding/json"
	"go.einride.tech/aip/filtering"
	v1alpha1 "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

type FilterStringConverter struct {
	codec encoding.Codec
}

func NewFilterStringConverter() *FilterStringConverter {
	return &FilterStringConverter{
		codec: encoding.GetCodec("json"),
	}
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

	filterExpr := &paginationV1.FilterExpr{}

	fsc.walk(filterExpr, parsedExpr.GetExpr())

	return filterExpr, nil
}

// mapOperator 映射 AIP 运算符到 paginationV1.Operator
func (fsc *FilterStringConverter) mapOperator(op string) paginationV1.Operator {
	switch op {
	case "=", "==", "EQ", "EQUAL":
		return paginationV1.Operator_EQ
	case "!=", "<>", "NE", "NOT_EQ":
		return paginationV1.Operator_NEQ
	case "<", "LT":
		return paginationV1.Operator_LT
	case "<=":
		return paginationV1.Operator_LTE
	case ">":
		return paginationV1.Operator_GT
	case ">=":
		return paginationV1.Operator_GTE
	case "in":
		return paginationV1.Operator_IN
	case "not in":
		return paginationV1.Operator_NIN
	case "contains":
		return paginationV1.Operator_CONTAINS
	case "startsWith":
		return paginationV1.Operator_STARTS_WITH
	case "endsWith":
		return paginationV1.Operator_ENDS_WITH
	case "is null":
		return paginationV1.Operator_IS_NULL
	case "is not null":
		return paginationV1.Operator_IS_NOT_NULL
	default:
		return paginationV1.Operator_OPERATOR_UNSPECIFIED
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
				Value: kind.ConstExpr.String(),
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
			}
		} else {
			// 处理其他运算符
			condition := &paginationV1.FilterCondition{
				Op: fsc.mapOperator(op),
			}
			if len(kind.CallExpr.Args) >= 2 {
				// 假设第一个参数是字段，第二个参数是值
				if identExpr, ok := kind.CallExpr.Args[0].ExprKind.(*v1alpha1.Expr_IdentExpr); ok {
					condition.Field = identExpr.IdentExpr.Name
				}
				if constExpr, ok := kind.CallExpr.Args[1].ExprKind.(*v1alpha1.Expr_ConstExpr); ok {
					condition.ValueOneof = &paginationV1.FilterCondition_Value{
						Value: constExpr.ConstExpr.String(),
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
