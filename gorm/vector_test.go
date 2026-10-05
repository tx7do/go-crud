package gorm

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tx7do/go-utils/mapper"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"github.com/tx7do/go-crud/vector"
)

// 向量检索（pgvector）测试：SQLite 内存库 + DryRun 会话预览生成 SQL，
// 真实 pgvector 执行需 PostgreSQL 集成环境（见 module README）。

// sqlCaptureLogger 捕获 gorm 生成的 SQL（含插值参数）供断言。
type sqlCaptureLogger struct {
	logger.Interface
	buf *strings.Builder
}

func (l *sqlCaptureLogger) Trace(_ context.Context, _ time.Time, fc func() (string, int64), _ error) {
	sql, _ := fc()
	l.buf.WriteString(sql)
	l.buf.WriteString("\n")
}

// openVectorTestDB 打开带 SQL 捕获 logger 的 SQLite 内存库。
func openVectorTestDB(t *testing.T) (*gorm.DB, *strings.Builder) {
	t.Helper()
	buf := &strings.Builder{}
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: &sqlCaptureLogger{Interface: logger.Default.LogMode(logger.Silent), buf: buf},
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err := db.AutoMigrate(&testUserEntity{}); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}
	return db, buf
}

func TestPgvectorOperator(t *testing.T) {
	op, err := pgvectorOperator(vector.MetricCosine)
	require.NoError(t, err)
	assert.Equal(t, "<=>", op)

	op, err = pgvectorOperator(vector.MetricEuclidean)
	require.NoError(t, err)
	assert.Equal(t, "<->", op)

	op, err = pgvectorOperator(vector.MetricDotProduct)
	require.NoError(t, err)
	assert.Equal(t, "<#>", op)

	_, err = pgvectorOperator("unknown")
	assert.Error(t, err)
}

func TestSearchByVector_SQLPreview(t *testing.T) {
	db, sqlBuf := openVectorTestDB(t)
	repo := NewRepository[CacheTestUser, testUserEntity](mapper.NewCopierMapper[CacheTestUser, testUserEntity]())

	ctx := context.Background()

	cases := []struct {
		name   string
		metric vector.DistanceMetric
		op     string
	}{
		{"默认度量按余弦处理", "", "<=>"},
		{"cosine", vector.MetricCosine, "<=>"},
		{"euclidean", vector.MetricEuclidean, "<->"},
		{"dot", vector.MetricDotProduct, "<#>"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sqlBuf.Reset()

			// DryRun 会话绕过方言守卫，仅生成 SQL 不执行
			dry := db.Session(&gorm.Session{DryRun: true})
			q := &vector.Query{
				Field:  "embedding",
				Vector: []float32{0.1, 0.2, 0.3},
				TopK:   10,
				Metric: vector.DistanceMetric(tc.metric),
			}

			res, err := repo.SearchByVector(ctx, dry, q, []func(*gorm.DB) *gorm.DB{
				func(db *gorm.DB) *gorm.DB { return db.Where("tenant_id = ?", "t1") },
			})
			require.NoError(t, err)
			require.NotNil(t, res)
			// DryRun 不执行，命中为空
			assert.Empty(t, res.Hits)
			assert.Equal(t, int64(0), res.Total)

			sql := sqlBuf.String()
			assert.Contains(t, sql, `"embedding" `+tc.op)
			assert.Contains(t, sql, `AS __vs_distance`)
			assert.Contains(t, sql, `ORDER BY __vs_distance`)
			assert.Contains(t, sql, `LIMIT 10`)
			assert.Contains(t, sql, `tenant_id`)
		})
	}
}

func TestSearchByVector_ValidationAndGuards(t *testing.T) {
	db := openTestDBForRepository(t)
	repo := NewRepository[CacheTestUser, testUserEntity](mapper.NewCopierMapper[CacheTestUser, testUserEntity]())

	ctx := context.Background()

	// db 为 nil
	_, err := repo.SearchByVector(ctx, nil, &vector.Query{
		Field: "embedding", Vector: []float32{1}, TopK: 1,
	}, nil)
	assert.ErrorContains(t, err, "db is nil")

	// 非法查询
	dry := db.Session(&gorm.Session{DryRun: true})
	_, err = repo.SearchByVector(ctx, dry, &vector.Query{
		Field: "", Vector: []float32{1}, TopK: 1,
	}, nil)
	assert.ErrorContains(t, err, "invalid vector query")
	_, err = repo.SearchByVector(ctx, dry, &vector.Query{
		Field: "embedding", Vector: nil, TopK: 1,
	}, nil)
	assert.ErrorContains(t, err, "invalid vector query")
	_, err = repo.SearchByVector(ctx, dry, &vector.Query{
		Field: "embedding", Vector: []float32{1}, TopK: 0,
	}, nil)
	assert.ErrorContains(t, err, "invalid vector query")

	// 不支持的度量
	_, err = repo.SearchByVector(ctx, dry, &vector.Query{
		Field: "embedding", Vector: []float32{1}, TopK: 1, Metric: "unknown",
	}, nil)
	assert.ErrorContains(t, err, "unsupported vector metric")

	// 非 postgres 且非 DryRun 的会话应被方言守卫拦截
	_, err = repo.SearchByVector(ctx, db, &vector.Query{
		Field: "embedding", Vector: []float32{1}, TopK: 1,
	}, nil)
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "requires postgres"), err.Error())
}

func TestCreateVectorIndex_Guards(t *testing.T) {
	db := openTestDBForRepository(t)
	c := &Client{DB: db}

	// 非 postgres 方言应被拦截
	err := c.CreateVectorIndex("test_user_entities", "embedding", vector.MetricCosine)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires postgres")

	// 索引名与操作符类映射（纯函数部分在 SQL 拼装中覆盖）
	assert.Equal(t, `idx_users_embedding_hnsw`, strings.Trim(quotePgIdentifier(`idx_users_embedding_hnsw`), `"`))
	assert.Equal(t, `"a""b"`, quotePgIdentifier(`a"b`))

	// 不支持的度量
	err = c.CreateVectorIndex("t", "v", "unknown")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported vector metric")
}
