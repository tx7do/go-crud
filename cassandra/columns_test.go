package cassandra

import (
	"reflect"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/tx7do/go-crud/cassandra/mixin"
)

// ─────────────────────────────────────────────────────────────────────────────
// 列映射编解码测试（纯函数，可离线断言）。
//
// 通道约定：匿名嵌入 mixin 拍平（tenant_id 落入列清单）；cql 标签 name
// 优先于字段名、"-" 跳过；主键通道字段（id/uuid）不进普通列清单；编码侧
// 有符号整数按自然类型传递、无符号与结构体报错；解码侧按 gocql 的列类型
// → Go 类型映射精确回填（int 列 → int、uuid 列 → gocql.UUID 特判回填
// string 字段）。
// ─────────────────────────────────────────────────────────────────────────────

// convEntity 列映射矩阵实体。
type convEntity struct {
	mixin.TenantID
	Skip   string `cql:"-"`
	Rename string `cql:"name:renamed"`
	Title  string
	Flag   bool
	Small  int8
	Mid    int16
	Num    int
	Big    int64
	Score  float32
	Ratio  float64
	Seen   time.Time
	Tags   []string
	Meta   map[string]int64
	ID     int64
}

// TestColumnsAndArgsFromEntity 编码矩阵：拍平 / 标签跳过与重命名 / 主键
// 通道剔除 / 各标量自然类型传递 / 列表与映射 / 结构体与无符号报错。
func TestColumnsAndArgsFromEntity(t *testing.T) {
	now := time.Unix(1700000000, 0)
	e := &convEntity{
		Rename: "q",
		Title:  "n",
		Flag:   true,
		Small:  1,
		Mid:    2,
		Num:    3,
		Big:    4,
		Score:  1.5,
		Ratio:  2.5,
		Seen:   now,
		Tags:   []string{"x"},
		Meta:   map[string]int64{"a": 1},
		ID:     101,
	}
	e.SetTenantID(7)

	cols, args, err := columnsAndArgsFromEntity(e)
	require.NoError(t, err)

	// 列顺序按实体字段声明序（mixin 匿名嵌入声明在最前）。
	assert.Equal(t, []string{"tenant_id", "renamed", "Title", "Flag", "Small", "Mid", "Num", "Big", "Score", "Ratio", "Seen", "Tags", "Meta"}, cols)
	require.Len(t, args, len(cols))
	assert.Equal(t, int64(7), args[0], "mixin tenant must be flattened into columns")
	assert.Equal(t, "q", args[1])
	assert.Equal(t, "n", args[2])
	assert.Equal(t, true, args[3])
	assert.Equal(t, int8(1), args[4])
	assert.Equal(t, int16(2), args[5])
	assert.Equal(t, int64(3), args[6], "plain int encodes as int64 (gocql binds by column type)")
	assert.Equal(t, int64(4), args[7])
	assert.InDelta(t, 1.5, args[8], 1e-6)
	assert.Equal(t, 2.5, args[9])
	assert.Equal(t, now, args[10])
	assert.Equal(t, []any{"x"}, args[11])
	assert.Equal(t, map[string]any{"a": int64(1)}, args[12])

	// 主键通道不进普通列清单。
	for _, c := range cols {
		assert.NotEqual(t, "ID", c)
	}
}

// TestColumnsAndArgsFromEntity_UnsignedRejected 无符号整数响亮报错
// （静默丢列比报错更糟）。
func TestColumnsAndArgsFromEntity_UnsignedRejected(t *testing.T) {
	type unsignedEntity struct {
		ID    int64
		Count uint32
	}
	_, _, err := columnsAndArgsFromEntity(&unsignedEntity{ID: 1, Count: 2})
	assert.ErrorIs(t, err, ErrPayloadConversion)
}

// TestColumnsAndArgsFromEntity_StructRejected 结构体字段报错。
func TestColumnsAndArgsFromEntity_StructRejected(t *testing.T) {
	type structEntity struct {
		ID    int64
		Inner struct{ A string }
	}
	_, _, err := columnsAndArgsFromEntity(&structEntity{ID: 1})
	assert.ErrorIs(t, err, ErrPayloadConversion)
}

// TestEntityFromRow 解码矩阵：按 gocql 列类型映射精确回填（int 列 → int、
// uuid 列 → gocql.UUID 特判回填 string）；类型不符/缺失列跳过。
func TestEntityFromRow(t *testing.T) {
	now := time.Unix(1700000000, 0)
	var e convEntity
	row := map[string]any{
		"tenant_id": int64(7),
		"renamed":   "r",
		"Title":     "n",
		"Flag":      true,
		"Small":     int8(1),
		"Mid":       int16(2),
		"Num":       int(3),
		"Big":       int64(4),
		"Score":     float32(1.5),
		"Ratio":     float64(2.5),
		"Seen":      now,
		"Tags":      []any{"a", "b"},
		"Meta":      map[string]any{"k": int64(3)},
	}
	require.NoError(t, entityFromRow(row, &e))

	assert.Equal(t, int64(7), e.TenantID.TenantID)
	assert.Equal(t, "r", e.Rename)
	assert.Equal(t, "n", e.Title)
	assert.True(t, e.Flag)
	assert.Equal(t, int8(1), e.Small)
	assert.Equal(t, int16(2), e.Mid)
	assert.Equal(t, 3, e.Num, "CQL int columns decode as Go int")
	assert.Equal(t, int64(4), e.Big)
	assert.InDelta(t, 1.5, e.Score, 1e-6)
	assert.Equal(t, 2.5, e.Ratio)
	assert.Equal(t, now, e.Seen)
	assert.Equal(t, []string{"a", "b"}, e.Tags)
	assert.Equal(t, map[string]int64{"k": 3}, e.Meta)
	assert.Empty(t, e.Skip, "missing column keeps zero value")
}

// TestEntityFromRow_UUIDChannel uuid 列的驱动原生类型回填 string 字段。
func TestEntityFromRow_UUIDChannel(t *testing.T) {
	type uuidEntity struct {
		ID    gocql.UUID
		UUID  string
		Title string
	}
	u, uerr := gocql.ParseUUID("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, uerr)
	var e uuidEntity
	require.NoError(t, entityFromRow(map[string]any{"ID": u, "Title": "x"}, &e))
	assert.Equal(t, u, e.ID)
	assert.Empty(t, e.UUID, "column named ID is not the gocql.UUID column here")

	var e2 uuidEntity
	require.NoError(t, entityFromRow(map[string]any{"UUID": u, "Title": "x"}, &e2))
	assert.Equal(t, u.String(), e2.UUID, "gocql.UUID must hydrate string uuid-channel fields")
}

// TestEntityFromRow_NilDestination nil 目标报错。
func TestEntityFromRow_NilDestination(t *testing.T) {
	var e *convEntity
	err := entityFromRow(map[string]any{}, e)
	assert.ErrorIs(t, err, ErrPayloadConversion)
}

// TestCollectColumnSpecs_Guards 非法列名 / 缺主键 / 非结构体。
func TestCollectColumnSpecs_Guards(t *testing.T) {
	type badColEntity struct {
		ID   int64
		Name string `cql:"name:bad)col"`
	}
	_, err := collectColumnSpecs(reflect.TypeOf(&badColEntity{}).Elem())
	assert.ErrorIs(t, err, ErrInvalidRequest)

	type noPKEntity struct {
		Title string
	}
	_, err = collectColumnSpecs(reflect.TypeOf(&noPKEntity{}).Elem())
	assert.ErrorIs(t, err, ErrInvalidPointID)

	_, err = collectSpecsOf("not-a-struct")
	assert.ErrorIs(t, err, ErrPayloadConversion)

	_, err = collectSpecsOf(nil)
	assert.ErrorIs(t, err, ErrInvalidRequest)
}

// TestParseTagSetting 标签设置解析（分段、k:v、空白修剪）。
func TestParseTagSetting(t *testing.T) {
	assert.Equal(t, map[string]string{}, parseTagSetting(""))
	assert.Equal(t, map[string]string{"name": "a", "other": "b"}, parseTagSetting("name: a , other: b"))
	assert.Equal(t, map[string]string{}, parseTagSetting("noseparator"))
}

// TestEntityFromRow_TypedCollections gocql 对列表/映射列产出原生类型
// （list<text> → []string、map<text,bigint> → map[string]int64），
// 反射通道同样精确回填（[]any / map[string]any 替身形态见 TestEntityFromRow）。
func TestEntityFromRow_TypedCollections(t *testing.T) {
	var e convEntity
	row := map[string]any{
		"Tags": []string{"a", "b"},
		"Meta": map[string]int64{"k": 3},
	}
	require.NoError(t, entityFromRow(row, &e))
	assert.Equal(t, []string{"a", "b"}, e.Tags)
	assert.Equal(t, map[string]int64{"k": 3}, e.Meta)
}

// TestRowColumnValue 行列取值按折叠小写匹配（CQL 标识符大小写不敏感）。
func TestRowColumnValue(t *testing.T) {
	row := map[string]any{"id": int64(1)}
	v, ok := rowColumnValue(row, "ID")
	assert.True(t, ok)
	assert.Equal(t, int64(1), v)
	v, ok = rowColumnValue(row, "id")
	assert.True(t, ok)
	assert.Equal(t, int64(1), v)
	_, ok = rowColumnValue(row, "missing")
	assert.False(t, ok)
}
