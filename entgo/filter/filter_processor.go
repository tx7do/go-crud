package filter

import (
	"regexp"
	"strings"

	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/sql"
	"github.com/tx7do/go-crud/pagination"

	"github.com/go-kratos/kratos/v2/encoding"
	_ "github.com/go-kratos/kratos/v2/encoding/json"

	"github.com/tx7do/go-utils/stringcase"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

// escapeSQLString 对 SQL 字面量做最小转义，双写单引号并转义反斜杠，降低注入风险。
func escapeSQLString(s string) string {
	// 先转义反斜杠，再双写单引号
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `''`)
	return s
}

var jsonKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_\.]+$`)

// Processor 过滤处理器接口
type Processor struct {
	codec encoding.Codec
}

func NewProcessor() *Processor {
	return &Processor{
		codec: encoding.GetCodec("json"),
	}
}

// Process 处理过滤条件
func (poc Processor) Process(s *sql.Selector, p *sql.Predicate, op paginationV1.Operator, field, value string, values []string) *sql.Predicate {
	switch op {
	case paginationV1.Operator_EQ:
		return poc.Equal(s, p, field, value)
	case paginationV1.Operator_NEQ:
		return poc.NotEqual(s, p, field, value)
	case paginationV1.Operator_IN:
		return poc.In(s, p, field, value, values)
	case paginationV1.Operator_NIN:
		return poc.NotIn(s, p, field, value, values)
	case paginationV1.Operator_GTE:
		return poc.GTE(s, p, field, value)
	case paginationV1.Operator_GT:
		return poc.GT(s, p, field, value)
	case paginationV1.Operator_LTE:
		return poc.LTE(s, p, field, value)
	case paginationV1.Operator_LT:
		return poc.LT(s, p, field, value)
	case paginationV1.Operator_BETWEEN:
		return poc.Range(s, p, field, value, values)
	case paginationV1.Operator_IS_NULL:
		return poc.IsNull(s, p, field, value)
	case paginationV1.Operator_IS_NOT_NULL:
		return poc.IsNotNull(s, p, field, value)
	case paginationV1.Operator_CONTAINS:
		return poc.Contains(s, p, field, value)
	case paginationV1.Operator_ICONTAINS:
		return poc.InsensitiveContains(s, p, field, value)
	case paginationV1.Operator_STARTS_WITH:
		return poc.StartsWith(s, p, field, value)
	case paginationV1.Operator_ISTARTS_WITH:
		return poc.InsensitiveStartsWith(s, p, field, value)
	case paginationV1.Operator_ENDS_WITH:
		return poc.EndsWith(s, p, field, value)
	case paginationV1.Operator_IENDS_WITH:
		return poc.InsensitiveEndsWith(s, p, field, value)
	case paginationV1.Operator_EXACT:
		return poc.Exact(s, p, field, value)
	case paginationV1.Operator_IEXACT:
		return poc.InsensitiveExact(s, p, field, value)
	case paginationV1.Operator_REGEXP:
		return poc.Regex(s, p, field, value)
	case paginationV1.Operator_IREGEXP:
		return poc.InsensitiveRegex(s, p, field, value)
	case paginationV1.Operator_SEARCH:
		return poc.Search(s, p, field, value)
	default:
		return nil
	}
}

// Equal = 相等操作
// SQL: WHERE "name" = "tom"
func (poc Processor) Equal(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.EQ(s.C(field), value)
}

// NotEqual NOT 不相等操作
// SQL: WHERE NOT ("name" = "tom")
// 或者： WHERE "name" <> "tom"
// 用NOT可以过滤出NULL，而用<>、!=则不能。
func (poc Processor) NotEqual(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.Not().EQ(s.C(field), value)
}

// In IN操作
// SQL: WHERE name IN ("tom", "jimmy")
func (poc Processor) In(s *sql.Selector, p *sql.Predicate, field, value string, values []string) *sql.Predicate {
	if len(value) > 0 {
		var jsonValues []any
		if err := poc.codec.Unmarshal([]byte(value), &jsonValues); err == nil {
			return p.In(s.C(field), jsonValues...)
		}
	} else if len(values) > 0 {
		var anyValues []any
		for _, v := range values {
			anyValues = append(anyValues, v)
		}
		return p.In(s.C(field), anyValues...)
	}

	return nil
}

// NotIn NOT IN操作
// SQL: WHERE name NOT IN ("tom", "jimmy")`
func (poc Processor) NotIn(s *sql.Selector, p *sql.Predicate, field, value string, values []string) *sql.Predicate {
	if len(value) > 0 {
		var jsonValues []any
		if err := poc.codec.Unmarshal([]byte(value), &jsonValues); err == nil {
			return p.NotIn(s.C(field), jsonValues...)
		}
	} else if len(values) > 0 {
		var anyValues []any
		for _, v := range values {
			anyValues = append(anyValues, v)
		}
		return p.NotIn(s.C(field), anyValues...)
	}

	return nil
}

// GTE (Greater Than or Equal) 大于等于 >= 操作
// SQL: WHERE "create_time" >= "2023-10-25"
func (poc Processor) GTE(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.GTE(s.C(field), value)
}

// GT (Greater than) 大于 > 操作
// SQL: WHERE "create_time" > "2023-10-25"
func (poc Processor) GT(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.GT(s.C(field), value)
}

// LTE LTE (Less Than or Equal) 小于等于 <=操作
// SQL: WHERE "create_time" <= "2023-10-25"
func (poc Processor) LTE(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.LTE(s.C(field), value)
}

// LT (Less than) 小于 <操作
// SQL: WHERE "create_time" < "2023-10-25"
func (poc Processor) LT(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.LT(s.C(field), value)
}

// Range 在值域之中 BETWEEN操作
// SQL: WHERE "create_time" BETWEEN "2023-10-25" AND "2024-10-25"
// 或者： WHERE "create_time" >= "2023-10-25" AND "create_time" <= "2024-10-25"
func (poc Processor) Range(s *sql.Selector, _ *sql.Predicate, field, value string, values []string) *sql.Predicate {
	if len(value) > 0 {
		var jsonValues []any
		if err := poc.codec.Unmarshal([]byte(value), &jsonValues); err == nil {
			if len(jsonValues) != 2 {
				return nil
			}

			return sql.And(
				sql.GTE(s.C(field), jsonValues[0]),
				sql.LTE(s.C(field), jsonValues[1]),
			)
		}
	} else if len(values) == 2 {
		return sql.And(
			sql.GTE(s.C(field), values[0]),
			sql.LTE(s.C(field), values[1]),
		)
	}

	return nil
}

// IsNull 为空 IS NULL操作
// SQL: WHERE name IS NULL
func (poc Processor) IsNull(s *sql.Selector, p *sql.Predicate, field, _ string) *sql.Predicate {
	return p.IsNull(s.C(field))
}

// IsNotNull 不为空 IS NOT NULL操作
// SQL: WHERE name IS NOT NULL
func (poc Processor) IsNotNull(s *sql.Selector, p *sql.Predicate, field, _ string) *sql.Predicate {
	return p.Not().IsNull(s.C(field))
}

// Contains LIKE 前后模糊查询
// SQL: WHERE name LIKE '%L%';
func (poc Processor) Contains(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.Contains(s.C(field), value)
}

// InsensitiveContains ILIKE 前后模糊查询
// SQL: WHERE name ILIKE '%L%';
func (poc Processor) InsensitiveContains(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.ContainsFold(s.C(field), value)
}

// StartsWith LIKE 前缀+模糊查询
// SQL: WHERE name LIKE 'La%';
func (poc Processor) StartsWith(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.HasPrefix(s.C(field), value)
}

// InsensitiveStartsWith ILIKE 前缀+模糊查询
// SQL: WHERE name ILIKE 'La%';
func (poc Processor) InsensitiveStartsWith(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.EqualFold(s.C(field), value+"%")
}

// EndsWith LIKE 后缀+模糊查询
// SQL: WHERE name LIKE '%a';
func (poc Processor) EndsWith(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.HasSuffix(s.C(field), value)
}

// InsensitiveEndsWith ILIKE 后缀+模糊查询
// SQL: WHERE name ILIKE '%a';
func (poc Processor) InsensitiveEndsWith(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.EqualFold(s.C(field), "%"+value)
}

// Exact LIKE 操作 精确比对
// SQL: WHERE name LIKE 'a';
func (poc Processor) Exact(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.Like(s.C(field), value)
}

// InsensitiveExact ILIKE 操作 不区分大小写，精确比对
// SQL: WHERE name ILIKE 'a';
func (poc Processor) InsensitiveExact(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	return p.EqualFold(s.C(field), value)
}

// Regex 正则查找
// MySQL: WHERE title REGEXP BINARY '^(An?|The) +'
// Oracle: WHERE REGEXP_LIKE(title, '^(An?|The) +', 'c');
// PostgreSQL: WHERE title ~ '^(An?|The) +';
// SQLite: WHERE title REGEXP '^(An?|The) +';
func (poc Processor) Regex(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	p.Append(func(b *sql.Builder) {
		switch s.Builder.Dialect() {
		case dialect.Postgres:
			b.Ident(s.C(field)).WriteString(" ~ ")
			b.Arg(value)
			break

		case dialect.MySQL:
			b.Ident(s.C(field)).WriteString(" REGEXP BINARY ")
			b.Arg(value)
			break

		case dialect.SQLite:
			b.Ident(s.C(field)).WriteString(" REGEXP ")
			b.Arg(value)
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
func (poc Processor) InsensitiveRegex(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	p.Append(func(b *sql.Builder) {
		switch s.Builder.Dialect() {
		case dialect.Postgres:
			b.Ident(s.C(field)).WriteString(" ~* ")
			b.Arg(strings.ToLower(value))
			break

		case dialect.MySQL:
			b.Ident(s.C(field)).WriteString(" REGEXP ")
			b.Arg(strings.ToLower(value))
			break

		case dialect.SQLite:
			b.Ident(s.C(field)).WriteString(" REGEXP ")
			if !strings.HasPrefix(value, "(?i)") {
				value = "(?i)" + value
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
func (poc Processor) Search(s *sql.Selector, p *sql.Predicate, field, value string) *sql.Predicate {
	if strings.TrimSpace(value) == "" {
		return p
	}

	p.Append(func(b *sql.Builder) {
		switch s.Builder.Dialect() {
		case dialect.Postgres:
			// 使用全文搜索： to_tsvector(column) @@ plainto_tsquery(?)
			b.WriteString("to_tsvector(")
			b.Ident(s.C(field))
			b.WriteString(") @@ plainto_tsquery(")
			b.Arg(value)
			b.WriteString(")")

		case dialect.MySQL:
			// MySQL 全文搜索（需建全文索引）： MATCH(col) AGAINST(? IN NATURAL LANGUAGE MODE)
			b.WriteString("MATCH(")
			b.Ident(s.C(field))
			b.WriteString(") AGAINST(")
			b.Arg(value)
			b.WriteString(" IN NATURAL LANGUAGE MODE)")

		case dialect.SQLite:
			// SQLite 没有统一全文函数时使用 LIKE
			b.Ident(s.C(field))
			b.WriteString(" LIKE ")
			b.Arg("%" + value + "%")

		default:
			// fallback 使用通用的 LIKE 匹配
			b.Ident(s.C(field))
			b.WriteString(" LIKE ")
			b.Arg("%" + value + "%")
		}
	})

	return p
}

// DatePart 时间戳提取日期
// SQL: select extract(quarter from timestamp '2018-08-15 12:10:10');
func (poc Processor) DatePart(s *sql.Selector, p *sql.Predicate, datePart, field string) *sql.Predicate {
	if !pagination.IsValidDatePartString(datePart) {
		// 非法的 datePart，不生成表达式以避免注入
		return p
	}

	datePartUpper := strings.ToUpper(datePart)

	p.Append(func(b *sql.Builder) {
		switch s.Builder.Dialect() {
		case dialect.Postgres:
			// EXTRACT('PART' FROM column)
			b.WriteString("EXTRACT('")
			b.WriteString(datePartUpper)
			b.WriteString("' FROM ")
			b.Ident(s.C(field))
			b.WriteString(")")

		case dialect.MySQL:
			// PART(column)
			b.WriteString(datePartUpper)
			b.WriteString("(")
			b.Ident(s.C(field))
			b.WriteString(")")

		default:
			// fallback to Postgres style
			b.WriteString("EXTRACT('")
			b.WriteString(datePartUpper)
			b.WriteString("' FROM ")
			b.Ident(s.C(field))
			b.WriteString(")")
		}
	})

	return p
}

// DatePartField 日期
func (poc Processor) DatePartField(s *sql.Selector, datePart, field string) string {
	if !pagination.IsValidDatePartString(datePart) {
		// 非法的 datePart，不生成表达式以避免注入
		return ""
	}

	datePart = strings.ToUpper(datePart)

	p := sql.P()

	switch s.Builder.Dialect() {
	case dialect.Postgres:
		// EXTRACT('PART' FROM column)
		p.WriteString("EXTRACT(")
		p.WriteString("'" + datePart + "'")
		p.WriteString(" FROM ")
		p.Ident(s.C(field))
		p.WriteString(")")

	case dialect.MySQL:
		// PART(column)
		p.WriteString(datePart)
		p.WriteString("(")
		p.Ident(s.C(field))
		p.WriteString(")")

	default:
		// fallback to Postgres style
		p.WriteString("EXTRACT(")
		p.WriteString("'" + datePart + "'")
		p.WriteString(" FROM ")
		p.Ident(s.C(field))
		p.WriteString(")")
	}

	return p.String()
}

// Jsonb 提取JSONB字段
// Postgresql: WHERE ("app_profile"."preferences" ->> 'daily_email') = 'true'
// Mysql: WHERE JSON_EXTRACT(`preferences`, '$.daily_email') = 'true'
func (poc Processor) Jsonb(s *sql.Selector, p *sql.Predicate, jsonbField, field string) *sql.Predicate {
	field = stringcase.ToSnakeCase(field)
	jsonbField = strings.TrimSpace(jsonbField)
	if jsonbField == "" {
		return p
	}

	// 校验 key 合法性，防止构造出非法路径或注入
	if !jsonKeyPattern.MatchString(jsonbField) {
		return p
	}

	p.Append(func(b *sql.Builder) {
		switch s.Builder.Dialect() {
		case dialect.Postgres:
			b.Ident(s.C(field)).WriteString(" ->> ").
				WriteString("'" + jsonbField + "'")

		case dialect.MySQL:
			path := "'$." + jsonbField + "'"
			b.WriteString("JSON_EXTRACT(")
			b.Ident(s.C(field))
			b.WriteString(", ")
			b.WriteString(path)
			b.WriteString(")")

		default:
			// fallback to Postgres style parameterized literal
			b.Ident(s.C(field)).WriteString(" ->> ").
				WriteString("'" + jsonbField + "'")
		}
	})

	return p
}

// JsonbFieldExpr 返回一个带参数化占位的表达式（*sql.Predicate），
// 当需要在 SELECT/ORDER/其它构造表达式时使用，避免返回拼接好的原始字符串。
func (poc Processor) JsonbFieldExpr(s *sql.Selector, jsonbField, field string) *sql.Predicate {
	field = stringcase.ToSnakeCase(field)

	p := sql.P()

	// 校验后再构造 path，最终仍通过 b.Arg 绑定参数，防止注入
	if !jsonKeyPattern.MatchString(jsonbField) {
		return p
	}

	p.Append(func(b *sql.Builder) {
		switch s.Builder.Dialect() {
		case dialect.Postgres:
			b.Ident(s.C(field)).WriteString(" ->> ").
				WriteString("'" + jsonbField + "'")

		case dialect.MySQL:
			path := "'$." + jsonbField + "'"
			b.WriteString("JSON_EXTRACT(")
			b.Ident(s.C(field))
			b.WriteString(", ")
			b.WriteString(path)
			b.WriteString(")")

		default:
			b.Ident(s.C(field)).WriteString(" ->> ").
				WriteString("'" + jsonbField + "'")
		}
	})

	return p
}

// JsonbField JSONB字段
func (poc Processor) JsonbField(s *sql.Selector, jsonbField, field string) string {
	field = stringcase.ToSnakeCase(field)

	p := sql.P()

	// 校验后再构造 path，最终仍通过 b.Arg 绑定参数，防止注入
	if !jsonKeyPattern.MatchString(jsonbField) {
		return ""
	}

	switch s.Builder.Dialect() {
	case dialect.Postgres:
		p.Ident(s.C(field)).WriteString(" ->> ").
			WriteString("'" + jsonbField + "'")

	case dialect.MySQL:
		path := "'$." + jsonbField + "'"
		p.WriteString("JSON_EXTRACT(")
		p.Ident(s.C(field))
		p.WriteString(", ")
		p.WriteString(path)
		p.WriteString(")")

	default:
		p.Ident(s.C(field)).WriteString(" ->> ").
			WriteString("'" + jsonbField + "'")
	}

	return p.String()
}
