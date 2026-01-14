package pagination

// PaginateMode 表示分页方式
type PaginateMode int

const (
	ModePage PaginateMode = iota
	ModeOffset
	ModeToken
)

// Paginator 分页器接口
type Paginator interface {
	Mode() PaginateMode

	// page/size 风格

	Page() int
	Size() int

	// offset/limit 风格

	Offset() int
	Limit() int

	// token 风格

	Token() string
	NextToken() string
	PrevToken() string
	SetToken(token string)
	SetNextToken(token string)
	SetPrevToken(token string)

	// 统计相关

	Total() int64
	SetTotal(total int64)
	TotalPages() int

	HasNext() bool
	HasPrev() bool

	// 链式设置

	WithPage(page int) Paginator
	WithSize(size int) Paginator
	WithOffset(offset int) Paginator
	WithLimit(limit int) Paginator
	WithToken(token string) Paginator
}
