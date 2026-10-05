package qdrant

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	qdrant "github.com/qdrant/go-client/qdrant"

	"github.com/tx7do/go-crud/vector"
)

// Client 包装 qdrant 官方 gRPC 客户端。
type Client struct {
	cli *qdrant.Client

	host      string
	port      int
	apiKey    string
	useTLS    bool
	tlsConfig *tls.Config
}

// NewClient 创建 Qdrant 客户端。
func NewClient(opts ...Option) (*Client, error) {
	c := &Client{
		host: "localhost",
		port: 6334,
	}
	for _, o := range opts {
		if o != nil {
			o(c)
		}
	}
	if c.cli != nil {
		return c, nil
	}

	cli, err := qdrant.NewClient(&qdrant.Config{
		Host:      c.host,
		Port:      c.port,
		APIKey:    c.apiKey,
		UseTLS:    c.useTLS,
		TLSConfig: c.tlsConfig,
	})
	if err != nil {
		return nil, err
	}
	c.cli = cli
	return c, nil
}

// Close 关闭客户端连接。
func (c *Client) Close() error {
	if c.cli == nil {
		return nil
	}
	return c.cli.Close()
}

// CheckConnect 探测连接可用性（列出集合作为探针）。
func (c *Client) CheckConnect() bool {
	if c.cli == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := c.cli.ListCollections(ctx)
	return err == nil
}

// HasCollection 判断集合是否存在。
func (c *Client) HasCollection(ctx context.Context, collection string) (bool, error) {
	if c.cli == nil {
		return false, ErrClientNotInitialized
	}
	return c.cli.CollectionExists(ctx, collection)
}

// DropCollection 删除集合。
func (c *Client) DropCollection(ctx context.Context, collection string) error {
	if c.cli == nil {
		return ErrClientNotInitialized
	}
	return c.cli.DeleteCollection(ctx, collection)
}

// qdrantDistances 把统一度量枚举映射到 Qdrant 的 Distance 取值。
var qdrantDistances = map[vector.DistanceMetric]qdrant.Distance{
	vector.MetricCosine:     qdrant.Distance_Cosine,
	vector.MetricEuclidean:  qdrant.Distance_Euclid,
	vector.MetricDotProduct: qdrant.Distance_Dot,
}

// CreateVectorCollection 创建带向量字段的集合。
//
//	@param ctx 上下文
//	@param collection 集合名
//	@param dims 向量维度
//	@param metric 距离度量（cosine / euclidean / dot），集合创建时固定
//
// 注意：Qdrant 的距离度量在集合创建时固定，查询期不可换。
func (c *Client) CreateVectorCollection(
	ctx context.Context,
	collection string,
	dims int,
	metric vector.DistanceMetric,
) error {
	if c.cli == nil {
		return ErrClientNotInitialized
	}
	if collection == "" {
		return ErrInvalidRequest
	}
	if dims <= 0 {
		return fmt.Errorf("%w: dims must be positive, got %d", ErrInvalidRequest, dims)
	}
	// 与其余引擎约定一致：未指定度量时按 cosine 处理。
	if metric == "" {
		metric = vector.MetricCosine
	}
	distance, ok := qdrantDistances[metric]
	if !ok {
		return fmt.Errorf("%w: unsupported vector metric %q", ErrInvalidRequest, metric)
	}

	return c.cli.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: collection,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     uint64(dims),
			Distance: distance,
		}),
	})
}

// DropVectorCollection 删除带向量字段的集合（DropCollection 的别名）。
func (c *Client) DropVectorCollection(ctx context.Context, collection string) error {
	return c.DropCollection(ctx, collection)
}

// PayloadIndexKind 载荷索引种类。
type PayloadIndexKind int

const (
	// PayloadIndexKeyword 字符串载荷的精确匹配索引。
	PayloadIndexKeyword PayloadIndexKind = iota + 1
	// PayloadIndexInteger 整数载荷的精确匹配索引（如 tenant_id）。
	PayloadIndexInteger
)

// CreatePayloadIndex 为载荷字段建立精确匹配索引。
// 多租户场景下对 tenant_id 建立整数索引可显著提升过滤检索性能
// （Qdrant 官方推荐的多租户实践之一）。
func (c *Client) CreatePayloadIndex(ctx context.Context, collection, field string, kind PayloadIndexKind) error {
	if c.cli == nil {
		return ErrClientNotInitialized
	}
	if collection == "" || field == "" {
		return ErrInvalidRequest
	}

	// 服务端要求 field_type 与 field_index_params 成对给出：
	// 缺 field_type 时报 cannot convert field_type；内层参数结构也必须
	// 实例化（经官方工厂构造）。
	var (
		params *qdrant.PayloadIndexParams
		ft     qdrant.FieldType
	)
	switch kind {
	case PayloadIndexKeyword:
		params = qdrant.NewPayloadIndexParamsKeyword(&qdrant.KeywordIndexParams{})
		ft = qdrant.FieldType_FieldTypeKeyword
	case PayloadIndexInteger:
		params = qdrant.NewPayloadIndexParamsInt(&qdrant.IntegerIndexParams{})
		ft = qdrant.FieldType_FieldTypeInteger
	default:
		return fmt.Errorf("%w: unsupported payload index kind %d", ErrInvalidRequest, kind)
	}

	if _, err := c.cli.CreateFieldIndex(ctx, &qdrant.CreateFieldIndexCollection{
		CollectionName:   collection,
		FieldName:        field,
		FieldType:        &ft,
		FieldIndexParams: params,
	}); err != nil {
		return fmt.Errorf("%w: %v", ErrQueryFailed, err)
	}
	return nil
}
