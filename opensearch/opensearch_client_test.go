package opensearch

import (
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/tx7do/go-crud/log"
	"github.com/stretchr/testify/assert"
	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	"github.com/tx7do/go-utils/trans"
)

const (
	userIndex   = "user"
	tweetIndex  = "tweet"
	sensorIndex = "sensor"
)

type User struct {
	Name   string    `json:"name"`
	Age    int       `json:"age"`
	Phone  string    `json:"phone"`
	Birth  time.Time `json:"birth"`
	Height float32   `json:"height"`
	Smoke  bool      `json:"smoke"`
	Home   string    `json:"home"`
}

// UserMapping 定义用户mapping
const UserMapping = `
{
    "mappings": {
        "properties": {
            "name": {"type": "text"},
            "age": {"type": "byte"},
            "phone": {"type": "text"},
            "birth": {"type": "date"},
            "height": {"type": "float"},
            "smoke": {"type": "boolean"},
            "home": {"type": "geo_point"}
        }
    }
}`

type Tweet struct {
	User     string    `json:"user"`               // 用户
	Message  string    `json:"message"`            // 微博内容
	Retweets int       `json:"retweets"`           // 转发数
	Image    string    `json:"image,omitempty"`    // 图片
	Created  time.Time `json:"created,omitempty"`  // 创建时间
	Tags     []string  `json:"tags,omitempty"`     // 标签
	Location string    `json:"location,omitempty"` //位置
	//Suggest  *elasticsearchV9.SuggestField `json:"suggest_field,omitempty"`
}

const TweetMapping = `
{
  "mappings": {
    "properties": {
      "user": {"type": "keyword"},
      "message": {"type": "text"},
      "image": {"type": "keyword"},
      "created": {"type": "date"},
      "tags": {"type": "keyword"},
      "location": {"type": "geo_point"},
      "suggest_field": {"type": "completion"}
    }
  }
}`

type Sensor struct {
	Id       int    `json:"id" bson:"_id,omitempty"`
	Type     string `json:"type" bson:"type,omitempty"`
	Location string `json:"location,omitempty" bson:"location,omitempty"`
}

type SensorData struct {
	Id          string    `json:"id" bson:"_id,omitempty"`
	Time        time.Time `json:"time" bson:"created,omitempty"`
	SensorId    int       `json:"sensor_id" bson:"sensor_id,omitempty"`
	Temperature float64   `json:"temperature" bson:"temperature,omitempty"`
	CPU         float64   `json:"cpu" bson:"cpu,omitempty"`
}

const SensorMapping = `
{
  "mappings": {
    "properties": {
      "sensor_id": {"type": "integer"},
      "temperature": {"type": "double"},
      "cpu": {"type": "double"},
      "location": {"type": "geo_point"}
    }
  }
}`

func createTestClient() *Client {
	cli, _ := NewOpenSearchClient(
		WithAddresses("http://localhost:9200"),
		WithUsername("admin"),
		WithPassword("@Abcd#123456"),
		WithEnableDebugLogger(true),
		WithLogger(log.DefaultLogger),
	)
	return cli
}

func TestNewClient(t *testing.T) {
	client := createTestClient()
	assert.NotNil(t, client)

	client.CheckConnectStatus(t.Context())
}

func TestCreateIndex(t *testing.T) {
	client := createTestClient()
	assert.NotNil(t, client)

	{
		_ = client.DeleteIndex(t.Context(), userIndex)
		err := client.CreateIndex(t.Context(), userIndex, UserMapping, "")
		assert.Nil(t, err)
	}

	{
		_ = client.DeleteIndex(t.Context(), tweetIndex)
		err := client.CreateIndex(t.Context(), tweetIndex, TweetMapping, "")
		assert.Nil(t, err)
	}

	{
		_ = client.DeleteIndex(t.Context(), sensorIndex)
		err := client.CreateIndex(t.Context(), sensorIndex, SensorMapping, "")
		assert.Nil(t, err)
	}
}

func TestDeleteIndex(t *testing.T) {
	client := createTestClient()
	assert.NotNil(t, client)

	err := client.DeleteIndex(t.Context(), userIndex)
	assert.Nil(t, err)

	err = client.DeleteIndex(t.Context(), tweetIndex)
	assert.Nil(t, err)

	err = client.DeleteIndex(t.Context(), sensorIndex)
	assert.Nil(t, err)
}

func TestInsertDocument(t *testing.T) {
	client := createTestClient()
	assert.NotNil(t, client)

	{
		// http://localhost:9200/user/_search?q=*&pretty
		loc, _ := time.LoadLocation("Local")
		birth, _ := time.ParseInLocation("2006-01-02", "1991-04-25", loc)
		userOne := User{
			Name:   "张三",
			Age:    23,
			Phone:  "17600000000",
			Birth:  birth,
			Height: 170.5,
			Home:   "41.40338,2.17403",
		}

		err := client.InsertDocument(t.Context(), userIndex, "N_1fm5cBE8GqVkmNBLNY", userOne)
		assert.Nil(t, err)
	}

	{
		tweetOne := Tweet{User: "olive", Message: "打酱油的一天", Retweets: 0}

		err := client.InsertDocument(t.Context(), tweetIndex, "", tweetOne)
		assert.Nil(t, err)
	}
}

func TestBatchInsertDocument(t *testing.T) {
	client := createTestClient()
	assert.NotNil(t, client)

	{
		loc, _ := time.LoadLocation("Local")
		// 生日
		birthSlice := []string{"1991-04-25", "1990-01-15", "1989-11-05", "1988-01-25", "1994-10-12"}
		// 姓名
		nameSlice := []string{"李四", "张飞", "赵云", "关羽", "刘备"}

		var users []any
		for i := 1; i < 20; i++ {
			birth, _ := time.ParseInLocation("2006-01-02", birthSlice[rand.Intn(len(birthSlice))], loc)
			height, _ := strconv.ParseFloat(fmt.Sprintf("%.2f", rand.Float32()+175.0), 32)
			user := User{
				Name:   nameSlice[rand.Intn(len(nameSlice))],
				Age:    rand.Intn(10) + 18,
				Phone:  "1760000000" + strconv.Itoa(i),
				Birth:  birth,
				Height: float32(height),
				Home:   "41.40338,2.17403",
			}
			users = append(users, user)
		}

		// 新增：先删除索引再创建，保证批量插入前索引存在且为空
		_ = client.DeleteIndex(t.Context(), userIndex)
		err := client.CreateIndex(t.Context(), userIndex, UserMapping, "")
		assert.Nil(t, err)

		err = client.BatchInsertDocument(t.Context(), userIndex, users, nil)
		assert.Nil(t, err)

		// 新增：校验插入数量
		//searchResult, err := client.search(t.Context(), userIndex, "*", nil, nil, 0, 100)
		//assert.Nil(t, err)
		//assert.NotNil(t, searchResult)
		//assert.GreaterOrEqual(t, len(searchResult.Hits.Hits), len(users))
	}
}

func TestGetDocument(t *testing.T) {
	client := createTestClient()
	assert.NotNil(t, client)

	var user User
	const id = "N_1fm5cBE8GqVkmNBLNC"
	err := client.GetDocument(t.Context(), userIndex, id, nil, &user)
	assert.Equal(t, err, ErrDocumentNotFound)
	assert.NotNil(t, user)
}

func TestMergeOptions(t *testing.T) {
	mapping := `{
		"properties": {
			"name": {
				"type": "text"
			},
			"age": {
				"type": "integer"
			}
		}
	}`

	settings := `{
		"index": {
			"number_of_shards": 1,
			"number_of_replicas": 0
		}
	}`

	//expected := `{"mappings":{"properties":{"name":{"type":"text"},"age":{"type":"integer"}}},"settings":{"index":{"number_of_shards":1,"number_of_replicas":0}}}`

	result, err := MergeOptions(mapping, settings)
	assert.Nil(t, err)
	//assert.Equal(t, expected, result)
	t.Log(result)
}

func TestParseQueryString(t *testing.T) {
	// 测试单个键值对的查询字符串
	query := `{"name":"张三"}`
	result := ParseQueryString(query)
	assert.NotNil(t, result)
	assert.Equal(t, []string{"name:张三"}, result)

	// 测试多个键值对的查询字符串
	query = `[{"name":"张三"},{"age":"23"}]`
	result = ParseQueryString(query)
	assert.NotNil(t, result)
	assert.Equal(t, []string{"name:张三", "age:23"}, result)

	t.Log(strings.Join(result, " AND "))

	// 测试无效的查询字符串
	query = `invalid`
	result = ParseQueryString(query)
	assert.Nil(t, result)
}

func TestMakeQueryString(t *testing.T) {
	// 测试 AND 查询
	andQuery := `{"name":"张三","age":"23"}`
	orQuery := ``
	result := MakeQueryString(andQuery, orQuery)
	assert.Equal(t, "name:张三 AND age:23", result)

	// 测试 OR 查询
	andQuery = ``
	orQuery = `[{"city":"北京"},{"country":"中国"}]`
	result = MakeQueryString(andQuery, orQuery)
	assert.Equal(t, "city:北京 OR country:中国", result)

	// 测试 AND 和 OR 查询同时存在
	andQuery = `{"name":"张三"}`
	orQuery = `[{"city":"北京"},{"country":"中国"}]`
	result = MakeQueryString(andQuery, orQuery)
	assert.Equal(t, "name:张三 AND (city:北京 OR country:中国)", result)

	// 测试空查询
	andQuery = ``
	orQuery = ``
	result = MakeQueryString(andQuery, orQuery)
	assert.Equal(t, "", result)
}

func TestSearchBySQLTo(t *testing.T) {
	client := createTestClient()
	assert.NotNil(t, client)

	// 确保索引存在并插入测试数据
	_ = client.DeleteIndex(t.Context(), userIndex)
	_ = client.CreateIndex(t.Context(), userIndex, UserMapping, "")
	users := []User{
		{Name: "Alice", Age: 25, Phone: "123", Birth: time.Now(), Height: 1.65, Smoke: false, Home: "40.7128,-74.0060"},
		{Name: "Bob", Age: 30, Phone: "456", Birth: time.Now(), Height: 1.80, Smoke: true, Home: "34.0522,-118.2437"},
	}
	for i, u := range users {
		_ = client.InsertDocument(t.Context(), userIndex, strconv.Itoa(i+1), u)
	}
	// 刷新索引，确保可查
	err := client.RefreshIndex(t.Context(), userIndex)
	assert.Nil(t, err)

	// SQL 查询
	sql := "SELECT name, age FROM user WHERE age > 20 ORDER BY age DESC"
	var result []struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	err = client.SearchBySQLTo(t.Context(), sql, &result)
	assert.Nil(t, err)
	assert.True(t, len(result) >= 2)
	assert.Equal(t, "Bob", result[0].Name)
	assert.Equal(t, 30, result[0].Age)
	assert.Equal(t, "Alice", result[1].Name)
	assert.Equal(t, 25, result[1].Age)
}

func TestQueryWithSQLPagination(t *testing.T) {
	client := createTestClient()
	assert.NotNil(t, client)

	_ = client.DeleteIndex(t.Context(), userIndex)
	_ = client.CreateIndex(t.Context(), userIndex, UserMapping, "")
	users := []User{
		{Name: "Alice", Age: 25, Phone: "123", Birth: time.Now(), Height: 1.65, Smoke: false, Home: "40.7128,-74.0060"},
		{Name: "Bob", Age: 30, Phone: "456", Birth: time.Now(), Height: 1.80, Smoke: true, Home: "34.0522,-118.2437"},
		{Name: "Carol", Age: 22, Phone: "789", Birth: time.Now(), Height: 1.70, Smoke: false, Home: "51.5074,-0.1278"},
	}
	for i, u := range users {
		_ = client.InsertDocument(t.Context(), userIndex, strconv.Itoa(i+1), u)
	}
	err := client.RefreshIndex(t.Context(), userIndex)
	assert.Nil(t, err)

	// 构造分页请求
	req := &paginationV1.PagingRequest{
		Page:     trans.Ptr(uint32(1)),
		PageSize: trans.Ptr(uint32(2)),
		Sorting:  []*paginationV1.Sorting{{Field: "age", Direction: paginationV1.Sorting_DESC}},
		FilteringType: &paginationV1.PagingRequest_FilterExpr{FilterExpr: &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{{
				Field:      "age",
				Op:         paginationV1.Operator_GTE,
				ValueOneof: &paginationV1.FilterCondition_Value{Value: "22"},
			}},
		},
		},
	}

	result, err := client.QueryWithSQLPagination(t.Context(), userIndex, req)
	assert.Nil(t, err)
	assert.NotNil(t, result)
	assert.True(t, len(result.Datarows) > 0)
	// 检查排序和过滤
	prevAge := 1000
	for _, row := range result.Datarows {
		t.Logf("Row: %v", row)
		age, _ := row[4].(float64)
		assert.True(t, int(age) <= prevAge)
		prevAge = int(age)
		assert.True(t, int(age) >= 22)
	}
}

func TestQueryWithSQLPaginationTo(t *testing.T) {
	client := createTestClient()
	assert.NotNil(t, client)

	_ = client.DeleteIndex(t.Context(), userIndex)
	_ = client.CreateIndex(t.Context(), userIndex, UserMapping, "")
	users := []User{
		{Name: "Alice", Age: 25, Phone: "123", Birth: time.Now(), Height: 1.65, Smoke: false, Home: "40.7128,-74.0060"},
		{Name: "Bob", Age: 30, Phone: "456", Birth: time.Now(), Height: 1.80, Smoke: true, Home: "34.0522,-118.2437"},
		{Name: "Carol", Age: 22, Phone: "789", Birth: time.Now(), Height: 1.70, Smoke: false, Home: "51.5074,-0.1278"},
	}
	for i, u := range users {
		_ = client.InsertDocument(t.Context(), userIndex, strconv.Itoa(i+1), u)
	}
	err := client.RefreshIndex(t.Context(), userIndex)
	assert.Nil(t, err)

	// 构造分页请求
	req := &paginationV1.PagingRequest{
		Page:     trans.Ptr(uint32(1)),
		PageSize: trans.Ptr(uint32(2)),
		Sorting:  []*paginationV1.Sorting{{Field: "age", Direction: paginationV1.Sorting_DESC}},
		FilteringType: &paginationV1.PagingRequest_FilterExpr{FilterExpr: &paginationV1.FilterExpr{
			Type: paginationV1.ExprType_AND,
			Conditions: []*paginationV1.FilterCondition{{
				Field:      "age",
				Op:         paginationV1.Operator_GTE,
				ValueOneof: &paginationV1.FilterCondition_Value{Value: "22"},
			}},
		},
		},
	}

	var out []*User

	err = client.QueryWithSQLPaginationTo(t.Context(), userIndex, req, &out)
	assert.Nil(t, err)
	assert.NotNil(t, out)
	assert.True(t, len(out) > 0)
	// 检查排序和过滤
	prevAge := 1000
	for _, row := range out {
		assert.True(t, row.Age <= prevAge)
		prevAge = row.Age
		assert.True(t, int(row.Age) >= 22)
		t.Logf("User: %v", row)
	}
}
