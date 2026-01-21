package filter

import (
	"strings"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"

	"github.com/go-kratos/kratos/v2/encoding"
	_ "github.com/go-kratos/kratos/v2/encoding/json"
	"github.com/go-kratos/kratos/v2/log"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-crud/pagination/filter"
)

// StructuredFilter 基于 FilterExpr 的过滤器
type StructuredFilter struct {
	codec encoding.Codec
}

func NewStructuredFilter() *StructuredFilter {
	return &StructuredFilter{
		codec: encoding.GetCodec("json"),
	}
}

// BuildSelectors 构建过滤选择器
func (sf StructuredFilter) BuildSelectors(expr *paginationV1.FilterExpr) ([]func(s *sql.Selector), error) {
	if expr == nil {
		return nil, nil
	}

	// Skip unspecified expressions
	if expr.GetType() == paginationV1.ExprType_EXPR_TYPE_UNSPECIFIED {
		log.Warn("Skipping unspecified FilterExpr")
		return nil, nil
	}

	selector, err := sf.buildFilterSelector(expr)
	if err != nil {
		return nil, err
	}

	var queryConditions []func(s *sql.Selector)
	if selector != nil {
		queryConditions = append(queryConditions, selector)
	}

	return queryConditions, nil
}

func (sf StructuredFilter) buildFilterSelector(expr *paginationV1.FilterExpr) (func(s *sql.Selector), error) {
	var selector func(s *sql.Selector)

	// Skip nil expressions
	if expr == nil {
		log.Warn("Skipping nil FilterExpr")
		return nil, nil
	}

	// Skip unspecified expressions
	if expr.GetType() == paginationV1.ExprType_EXPR_TYPE_UNSPECIFIED {
		log.Warn("Skipping unspecified FilterExpr")
		return nil, nil
	}

	// Process conditions
	selector = func(s *sql.Selector) {
		// Process groups recursively
		for _, cond := range expr.GetGroups() {
			subSelector, err := sf.buildFilterSelector(cond)
			if err != nil {
				log.Errorf("Error processing sub-group: %v", err)
				continue
			}
			if subSelector != nil {
				subSelector(s)
			}
		}

		// Process current level conditions
		ps, err := sf.processCondition(s, expr.GetConditions())
		if err != nil {
			return
		}

		// Combine predicates based on expression type
		if len(ps) > 0 {
			switch expr.GetType() {
			case paginationV1.ExprType_AND:
				s.Where(sql.And(ps...))
			case paginationV1.ExprType_OR:
				s.Where(sql.Or(ps...))
			}
		}
	}

	return selector, nil
}

// processCondition 处理条件
func (sf StructuredFilter) processCondition(s *sql.Selector, conditions []*paginationV1.FilterCondition) ([]*sql.Predicate, error) {
	if len(conditions) == 0 {
		return nil, nil
	}

	var ps []*sql.Predicate
	for _, cond := range conditions {
		p := sql.P()
		if cp := sf.Process(s, p, cond); cp != nil {
			ps = append(ps, cp)
		}
	}

	return ps, nil
}

// Process 处理过滤条件
func (sf StructuredFilter) Process(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	switch condition.GetOp() {
	case paginationV1.Operator_EQ:
		return sf.Equal(s, p, condition)
	case paginationV1.Operator_NEQ:
		return sf.NotEqual(s, p, condition)
	case paginationV1.Operator_IN:
		return sf.In(s, p, condition)
	case paginationV1.Operator_NIN:
		return sf.NotIn(s, p, condition)
	case paginationV1.Operator_GTE:
		return sf.GTE(s, p, condition)
	case paginationV1.Operator_GT:
		return sf.GT(s, p, condition)
	case paginationV1.Operator_LTE:
		return sf.LTE(s, p, condition)
	case paginationV1.Operator_LT:
		return sf.LT(s, p, condition)
	case paginationV1.Operator_BETWEEN:
		return sf.Range(s, p, condition)
	case paginationV1.Operator_IS_NULL:
		return sf.IsNull(s, p, condition)
	case paginationV1.Operator_IS_NOT_NULL:
		return sf.IsNotNull(s, p, condition)
	case paginationV1.Operator_CONTAINS:
		return sf.Contains(s, p, condition)
	case paginationV1.Operator_ICONTAINS:
		return sf.InsensitiveContains(s, p, condition)
	case paginationV1.Operator_STARTS_WITH:
		return sf.StartsWith(s, p, condition)
	case paginationV1.Operator_ISTARTS_WITH:
		return sf.InsensitiveStartsWith(s, p, condition)
	case paginationV1.Operator_ENDS_WITH:
		return sf.EndsWith(s, p, condition)
	case paginationV1.Operator_IENDS_WITH:
		return sf.InsensitiveEndsWith(s, p, condition)
	case paginationV1.Operator_EXACT:
		return sf.Exact(s, p, condition)
	case paginationV1.Operator_IEXACT:
		return sf.InsensitiveExact(s, p, condition)
	case paginationV1.Operator_REGEXP:
		return sf.Regex(s, p, condition)
	case paginationV1.Operator_IREGEXP:
		return sf.InsensitiveRegex(s, p, condition)
	case paginationV1.Operator_SEARCH:
		return sf.Search(s, p, condition)
	default:
		return nil
	}
}

// Equal = 相等操作
// SQL: WHERE "name" = "tom"
func (sf StructuredFilter) Equal(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	return p.EQ(s.C(condition.GetField()), condition.GetValue())
}

// NotEqual NOT 不相等操作
// SQL: WHERE NOT ("name" = "tom")
// 或者： WHERE "name" <> "tom"
// 用NOT可以过滤出NULL，而用<>、!=则不能。
func (sf StructuredFilter) NotEqual(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	return p.Not().EQ(s.C(condition.GetField()), condition.GetValue())
}

// In IN操作
// SQL: WHERE name IN ("tom", "jimmy")
func (sf StructuredFilter) In(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	if len(condition.GetValue()) > 0 {
		var jsonValues []any
		if err := sf.codec.Unmarshal([]byte(condition.GetValue()), &jsonValues); err == nil {
			return p.In(s.C(condition.GetField()), jsonValues...)
		}
	} else if len(condition.GetValues()) > 0 {
		var anyValues []any
		for _, v := range condition.GetValues() {
			anyValues = append(anyValues, v)
		}
		return p.In(s.C(condition.GetField()), anyValues...)
	}

	return nil
}

// NotIn NOT IN操作
// SQL: WHERE name NOT IN ("tom", "jimmy")`
func (sf StructuredFilter) NotIn(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	if len(condition.GetValue()) > 0 {
		var jsonValues []any
		if err := sf.codec.Unmarshal([]byte(condition.GetValue()), &jsonValues); err == nil {
			return p.NotIn(s.C(condition.GetField()), jsonValues...)
		}
	} else if len(condition.GetValues()) > 0 {
		var anyValues []any
		for _, v := range condition.GetValues() {
			anyValues = append(anyValues, v)
		}
		return p.NotIn(s.C(condition.GetField()), anyValues...)
	}

	return nil
}

// GTE (Greater Than or Equal) 大于等于 >= 操作
// SQL: WHERE "create_time" >= "2023-10-25"
func (sf StructuredFilter) GTE(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	return p.GTE(s.C(condition.GetField()), condition.GetValue())
}

// GT (Greater than) 大于 > 操作
// SQL: WHERE "create_time" > "2023-10-25"
func (sf StructuredFilter) GT(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	return p.GT(s.C(condition.GetField()), condition.GetValue())
}

// LTE LTE (Less Than or Equal) 小于等于 <=操作
// SQL: WHERE "create_time" <= "2023-10-25"
func (sf StructuredFilter) LTE(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	return p.LTE(s.C(condition.GetField()), condition.GetValue())
}

// LT (Less than) 小于 <操作
// SQL: WHERE "create_time" < "2023-10-25"
func (sf StructuredFilter) LT(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	return p.LT(s.C(condition.GetField()), condition.GetValue())
}

// Range 在值域之中 BETWEEN操作
// SQL: WHERE "create_time" BETWEEN "2023-10-25" AND "2024-10-25"
// 或者： WHERE "create_time" >= "2023-10-25" AND "create_time" <= "2024-10-25"
func (sf StructuredFilter) Range(s *sql.Selector, _ *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	if len(condition.GetValue()) > 0 {
		var jsonValues []any
		if err := sf.codec.Unmarshal([]byte(condition.GetValue()), &jsonValues); err == nil {
			if len(jsonValues) != 2 {
				return nil
			}

			return sql.And(
				sql.GTE(s.C(condition.GetField()), jsonValues[0]),
				sql.LTE(s.C(condition.GetField()), jsonValues[1]),
			)
		}
	} else if len(condition.GetValues()) == 2 {
		return sql.And(
			sql.GTE(s.C(condition.GetField()), condition.GetValues()[0]),
			sql.LTE(s.C(condition.GetField()), condition.GetValues()[1]),
		)
	}

	return nil
}

// IsNull 为空 IS NULL操作
// SQL: WHERE name IS NULL
func (sf StructuredFilter) IsNull(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	return p.IsNull(s.C(condition.GetField()))
}

// IsNotNull 不为空 IS NOT NULL操作
// SQL: WHERE name IS NOT NULL
func (sf StructuredFilter) IsNotNull(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	return p.Not().IsNull(s.C(condition.GetField()))
}

// Contains LIKE 前后模糊查询
// SQL: WHERE name LIKE '%L%';
func (sf StructuredFilter) Contains(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	return p.Contains(s.C(condition.GetField()), condition.GetValue())
}

// InsensitiveContains ILIKE 前后模糊查询
// SQL: WHERE name ILIKE '%L%';
func (sf StructuredFilter) InsensitiveContains(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	return p.ContainsFold(s.C(condition.GetField()), condition.GetValue())
}

// StartsWith LIKE 前缀+模糊查询
// SQL: WHERE name LIKE 'La%';
func (sf StructuredFilter) StartsWith(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	return p.HasPrefix(s.C(condition.GetField()), condition.GetValue())
}

// InsensitiveStartsWith ILIKE 前缀+模糊查询
// SQL: WHERE name ILIKE 'La%';
func (sf StructuredFilter) InsensitiveStartsWith(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	return p.EqualFold(s.C(condition.GetField()), condition.GetValue()+"%")
}

// EndsWith LIKE 后缀+模糊查询
// SQL: WHERE name LIKE '%a';
func (sf StructuredFilter) EndsWith(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	return p.HasSuffix(s.C(condition.GetField()), condition.GetValue())
}

// InsensitiveEndsWith ILIKE 后缀+模糊查询
// SQL: WHERE name ILIKE '%a';
func (sf StructuredFilter) InsensitiveEndsWith(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	return p.EqualFold(s.C(condition.GetField()), "%"+condition.GetValue())
}

// Exact LIKE 操作 精确比对
// SQL: WHERE name LIKE 'a';
func (sf StructuredFilter) Exact(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	return p.Like(s.C(condition.GetField()), condition.GetValue())
}

// InsensitiveExact ILIKE 操作 不区分大小写，精确比对
// SQL: WHERE name ILIKE 'a';
func (sf StructuredFilter) InsensitiveExact(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	return p.EqualFold(s.C(condition.GetField()), condition.GetValue())
}

// Regex 正则查找
// MySQL: WHERE title REGEXP BINARY '^(An?|The) +'
// Oracle: WHERE REGEXP_LIKE(title, '^(An?|The) +', 'c');
// PostgreSQL: WHERE title ~ '^(An?|The) +';
// SQLite: WHERE title REGEXP '^(An?|The) +';
func (sf StructuredFilter) Regex(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	p.Append(func(b *sql.Builder) {
		switch s.Builder.Dialect() {
		case dialect.Postgres:
			b.Ident(s.C(condition.GetField())).WriteString(" ~ ")
			b.Arg(condition.GetValue())
			break

		case dialect.MySQL:
			b.Ident(s.C(condition.GetField())).WriteString(" REGEXP BINARY ")
			b.Arg(condition.GetValue())
			break

		case dialect.SQLite:
			b.Ident(s.C(condition.GetField())).WriteString(" REGEXP ")
			b.Arg(condition.GetValue())
			break

		case dialect.Gremlin:
			break
		}
	})
	return p
}

// InsensitiveRegex 正则查找 不区分大小写
// MySQL: WHERE title REGEXP '^(an?|the) +'
// Oracle: WHERE REGEXP_LIKE(title, '^(an?|the) +', 'i');
// PostgreSQL: WHERE title ~* '^(an?|the) +';
// SQLite: WHERE title REGEXP '(?i)^(an?|the) +';
func (sf StructuredFilter) InsensitiveRegex(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	p.Append(func(b *sql.Builder) {
		switch s.Builder.Dialect() {
		case dialect.Postgres:
			b.Ident(s.C(condition.GetField())).WriteString(" ~* ")
			b.Arg(strings.ToLower(condition.GetValue()))
			break

		case dialect.MySQL:
			b.Ident(s.C(condition.GetField())).WriteString(" REGEXP ")
			b.Arg(strings.ToLower(condition.GetValue()))
			break

		case dialect.SQLite:
			value := condition.GetValue()
			b.Ident(s.C(condition.GetField())).WriteString(" REGEXP ")
			if !strings.HasPrefix(condition.GetValue(), "(?i)") {
				value = "(?i)" + condition.GetValue()
			}
			b.Arg(strings.ToLower(value))
			break

		case dialect.Gremlin:
			break
		}
	})
	return p
}

// Search 全文搜索
// SQL:
func (sf StructuredFilter) Search(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	if strings.TrimSpace(condition.GetValue()) == "" {
		return p
	}

	p.Append(func(b *sql.Builder) {
		switch s.Builder.Dialect() {
		case dialect.Postgres:
			// 使用全文搜索： to_tsvector(column) @@ plainto_tsquery(?)
			b.WriteString("to_tsvector(")
			b.Ident(s.C(condition.GetField()))
			b.WriteString(") @@ plainto_tsquery(")
			b.Arg(condition.GetValue())
			b.WriteString(")")

		case dialect.MySQL:
			// MySQL 全文搜索（需建全文索引）： MATCH(col) AGAINST(? IN NATURAL LANGUAGE MODE)
			b.WriteString("MATCH(")
			b.Ident(s.C(condition.GetField()))
			b.WriteString(") AGAINST(")
			b.Arg(condition.GetValue())
			b.WriteString(" IN NATURAL LANGUAGE MODE)")

		case dialect.SQLite:
			// SQLite 没有统一全文函数时使用 LIKE
			b.Ident(s.C(condition.GetField()))
			b.WriteString(" LIKE ")
			b.Arg("%" + condition.GetValue() + "%")

		default:
			// fallback 使用通用的 LIKE 匹配
			b.Ident(s.C(condition.GetField()))
			b.WriteString(" LIKE ")
			b.Arg("%" + condition.GetValue() + "%")
		}
	})

	return p
}

// DatePart 时间戳提取日期
// SQL: select extract(quarter from timestamp '2018-08-15 12:10:10');
func (sf StructuredFilter) DatePart(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	if condition.DatePart == nil {
		// 非法的 datePart，不生成表达式以避免注入
		return p
	}

	datePart := filter.ConverterDatePartToString(condition.DatePart)
	datePart = strings.ToUpper(datePart)

	p.Append(func(b *sql.Builder) {
		switch s.Builder.Dialect() {
		case dialect.Postgres:
			// EXTRACT('PART' FROM column)
			b.WriteString("EXTRACT('")
			b.WriteString(datePart)
			b.WriteString("' FROM ")
			b.Ident(s.C(condition.GetField()))
			b.WriteString(")")

		case dialect.MySQL:
			// PART(column)
			b.WriteString(datePart)
			b.WriteString("(")
			b.Ident(s.C(condition.GetField()))
			b.WriteString(")")

		default:
			// fallback to Postgres style
			b.WriteString("EXTRACT('")
			b.WriteString(datePart)
			b.WriteString("' FROM ")
			b.Ident(s.C(condition.GetField()))
			b.WriteString(")")
		}
	})

	return p
}

// DatePartField 日期
func (sf StructuredFilter) DatePartField(s *sql.Selector, condition *paginationV1.FilterCondition) string {
	if condition.DatePart == nil {
		// 非法的 datePart，不生成表达式以避免注入
		return ""
	}

	datePart := filter.ConverterDatePartToString(condition.DatePart)
	datePart = strings.ToUpper(datePart)

	p := sql.P()

	switch s.Builder.Dialect() {
	case dialect.Postgres:
		// EXTRACT('PART' FROM column)
		p.WriteString("EXTRACT(")
		p.WriteString("'" + datePart + "'")
		p.WriteString(" FROM ")
		p.Ident(s.C(condition.GetField()))
		p.WriteString(")")

	case dialect.MySQL:
		// PART(column)
		p.WriteString(datePart)
		p.WriteString("(")
		p.Ident(s.C(condition.GetField()))
		p.WriteString(")")

	default:
		// fallback to Postgres style
		p.WriteString("EXTRACT(")
		p.WriteString("'" + datePart + "'")
		p.WriteString(" FROM ")
		p.Ident(s.C(condition.GetField()))
		p.WriteString(")")
	}

	return p.String()
}

// Jsonb 提取JSONB字段
// Postgresql: WHERE ("app_profile"."preferences" ->> 'daily_email') = 'true'
// Mysql: WHERE JSON_EXTRACT(`preferences`, '$.daily_email') = 'true'
func (sf StructuredFilter) Jsonb(s *sql.Selector, p *sql.Predicate, condition *paginationV1.FilterCondition) *sql.Predicate {
	if condition.GetJsonPath() == "" {
		return p
	}

	// 校验 key 合法性，防止构造出非法路径或注入
	if !jsonKeyPattern.MatchString(condition.GetJsonPath()) {
		return p
	}

	p.Append(func(b *sql.Builder) {
		switch s.Builder.Dialect() {
		case dialect.Postgres:
			b.Ident(s.C(condition.GetField())).WriteString(" ->> ").
				WriteString("'" + condition.GetJsonPath() + "'")

		case dialect.MySQL:
			path := "'$." + condition.GetJsonPath() + "'"
			b.WriteString("JSON_EXTRACT(")
			b.Ident(s.C(condition.GetField()))
			b.WriteString(", ")
			b.WriteString(path)
			b.WriteString(")")

		default:
			// fallback to Postgres style parameterized literal
			b.Ident(s.C(condition.GetField())).WriteString(" ->> ").
				WriteString("'" + condition.GetJsonPath() + "'")
		}
	})

	return p
}

// JsonbFieldExpr 返回一个带参数化占位的表达式（*sql.Predicate），
// 当需要在 SELECT/ORDER/其它构造表达式时使用，避免返回拼接好的原始字符串。
func (sf StructuredFilter) JsonbFieldExpr(s *sql.Selector, condition *paginationV1.FilterCondition) *sql.Predicate {
	p := sql.P()

	// 校验后再构造 path，最终仍通过 b.Arg 绑定参数，防止注入
	if !jsonKeyPattern.MatchString(condition.GetJsonPath()) {
		return p
	}

	p.Append(func(b *sql.Builder) {
		switch s.Builder.Dialect() {
		case dialect.Postgres:
			b.Ident(s.C(condition.GetField())).WriteString(" ->> ").
				WriteString("'" + condition.GetJsonPath() + "'")

		case dialect.MySQL:
			path := "'$." + condition.GetJsonPath() + "'"
			b.WriteString("JSON_EXTRACT(")
			b.Ident(s.C(condition.GetField()))
			b.WriteString(", ")
			b.WriteString(path)
			b.WriteString(")")

		default:
			b.Ident(s.C(condition.GetField())).WriteString(" ->> ").
				WriteString("'" + condition.GetJsonPath() + "'")
		}
	})

	return p
}

// JsonbField JSONB字段
func (sf StructuredFilter) JsonbField(s *sql.Selector, condition *paginationV1.FilterCondition) string {
	p := sql.P()

	// 校验后再构造 path，最终仍通过 b.Arg 绑定参数，防止注入
	if !jsonKeyPattern.MatchString(condition.GetJsonPath()) {
		return ""
	}

	switch s.Builder.Dialect() {
	case dialect.Postgres:
		p.Ident(s.C(condition.GetField())).WriteString(" ->> ").
			WriteString("'" + condition.GetJsonPath() + "'")

	case dialect.MySQL:
		path := "'$." + condition.GetJsonPath() + "'"
		p.WriteString("JSON_EXTRACT(")
		p.Ident(s.C(condition.GetField()))
		p.WriteString(", ")
		p.WriteString(path)
		p.WriteString(")")

	default:
		p.Ident(s.C(condition.GetField())).WriteString(" ->> ").
			WriteString("'" + condition.GetJsonPath() + "'")
	}

	return p.String()
}
