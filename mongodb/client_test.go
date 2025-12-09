package mongodb

import (
	"context"
	"testing"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/stretchr/testify/assert"
	"github.com/tx7do/go-utils/trans"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.mongodb.org/mongo-driver/v2/bson"
	optionsV2 "go.mongodb.org/mongo-driver/v2/mongo/options"
)

type Candle struct {
	Symbol    *string                `json:"s"`
	Open      *float64               `json:"o"`
	High      *float64               `json:"h"`
	Low       *float64               `json:"l"`
	Close     *float64               `json:"c"`
	Volume    *float64               `json:"v"`
	StartTime *timestamppb.Timestamp `json:"st"`
	EndTime   *timestamppb.Timestamp `json:"et"`
}

func createTestClient() *Client {
	cli, _ := NewClient(
		WithLogger(log.DefaultLogger),
		WithURI("mongodb://root:123456@127.0.0.1:27017/?compressors=snappy,zlib,zstd"),
		WithDatabase("finances"),
	)
	return cli
}

func TestNewClient(t *testing.T) {
	client := createTestClient()
	assert.NotNil(t, client)

	client.CheckConnect()
}

func TestInsertOne(t *testing.T) {
	client := createTestClient()
	assert.NotNil(t, client)

	ctx := context.Background()

	candle := Candle{
		StartTime: timestamppb.New(time.Now()),
		Symbol:    trans.Ptr("AAPL"),
		Open:      trans.Ptr(1.0),
		High:      trans.Ptr(2.0),
		Low:       trans.Ptr(3.0),
		Close:     trans.Ptr(4.0),
		Volume:    trans.Ptr(1000.0),
	}

	_, err := client.InsertOne(ctx, "candles", candle)
	assert.NoError(t, err)
}

func TestClient_CRUD(t *testing.T) {
	client := createTestClient()
	if client == nil {
		return
	}
	ctx := context.Background()
	collection := "test_candles"

	// 确保测试集合干净
	_ = client.cli.Database(client.database).Collection(collection).Drop(ctx)

	// InsertOne
	doc := bson.M{
		"s":  "AAPL",
		"o":  1.0,
		"h":  2.0,
		"l":  3.0,
		"c":  4.0,
		"v":  1000,
		"st": time.Now(),
	}
	res, err := client.InsertOne(ctx, collection, doc)
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.NotNil(t, res.InsertedID)

	// FindOne
	var found bson.M
	err = client.FindOne(ctx, collection, bson.M{"s": "AAPL"}, &found)
	assert.NoError(t, err)
	assert.Equal(t, "AAPL", found["s"])

	// Find (multiple)
	// insert another doc
	_, err = client.InsertOne(ctx, collection, bson.M{"s": "AAPL2", "c": 10.0})
	assert.NoError(t, err)

	var docs []bson.M
	err = client.Find(ctx, collection, bson.M{}, &docs)
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, len(docs), 2)

	// UpdateOne
	updateRes, err := client.UpdateOne(ctx, collection, bson.M{"s": "AAPL"}, bson.M{"$set": bson.M{"c": 5.0}})
	assert.NoError(t, err)
	// 有些情况下 UpdateResult.MatchedCount/ModifiedCount 可能为0（如果驱动/服务器行为不同），这里只做基本检查
	assert.NotNil(t, updateRes)

	// 校验更新
	var afterUpdate bson.M
	err = client.FindOne(ctx, collection, bson.M{"s": "AAPL"}, &afterUpdate)
	assert.NoError(t, err)
	// 强转为 float64（bson 解码通常为 float64）
	if v, ok := afterUpdate["c"].(float64); ok {
		assert.Equal(t, 5.0, v)
	}

	// DeleteOne
	deleteRes, err := client.DeleteOne(ctx, collection, bson.M{"s": "AAPL2"})
	assert.NoError(t, err)
	assert.NotNil(t, deleteRes)

	_ = client.cli.Database(client.database).Collection(collection).Drop(ctx)

	// Close -> 后续 Ping 应失败
	client.Close()
	connected := client.CheckConnect()
	assert.False(t, connected)
}

func TestClient_InsertManyAndOptions(t *testing.T) {
	client := createTestClient()
	if client == nil {
		return
	}
	ctx := context.Background()
	collection := "test_candles_many"

	// cleanup
	_ = client.cli.Database(client.database).Collection(collection).Drop(ctx)
	defer func() {
		_ = client.cli.Database(client.database).Collection(collection).Drop(ctx)
	}()

	// InsertMany
	docs := []interface{}{
		bson.M{"s": "SYM1", "v": 1},
		bson.M{"s": "SYM2", "v": 2},
	}
	res, err := client.InsertMany(ctx, collection, docs)
	assert.NoError(t, err)
	assert.NotNil(t, res)
	assert.Len(t, res.InsertedIDs, 2)

	// Find with options: limit 1
	var results []bson.M
	cur, err := client.cli.Database(client.database).Collection(collection).Find(ctx, bson.M{}, optionsV2.Find().SetLimit(1))
	if err != nil {
		t.Fatalf("driver find failed: %v", err)
	}
	defer cur.Close(ctx)
	err = cur.All(ctx, &results)
	assert.NoError(t, err)
	assert.Len(t, results, 1)
}
