package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/go-kratos/kratos/v2/log"

	elasticsearchV9 "github.com/elastic/go-elasticsearch/v9"
	esapiV9 "github.com/elastic/go-elasticsearch/v9/esapi"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

type Client struct {
	*elasticsearchV9.Client
	options *elasticsearchV9.Config

	log *log.Helper
}

func NewElasticsearchClient(opts ...Option) (*Client, error) {
	c := &Client{
		options: &elasticsearchV9.Config{},
		log:     log.NewHelper(log.DefaultLogger),
	}

	for _, o := range opts {
		o(c)
	}

	if err := c.createESClient(c.options); err != nil {
		return nil, err
	}

	return c, nil
}

// createESClient 创建Elasticsearch客户端
func (c *Client) createESClient(options *elasticsearchV9.Config) error {
	cli, err := elasticsearchV9.NewClient(*options)
	if err != nil {
		c.log.Errorf("failed to create elasticsearch client: %v", err)
		return err
	}

	c.Client = cli

	return nil
}

// CheckConnectStatus 检查Elasticsearch连接
func (c *Client) CheckConnectStatus() bool {
	if c.Client == nil {
		return false
	}

	resp, err := c.Client.Info()
	if err != nil {
		c.log.Errorf("failed to connect to elasticsearch: %v", err)
		return false
	}
	defer func(Body io.ReadCloser) {
		if err = Body.Close(); err != nil {
			c.log.Errorf("failed to close response body: %v", err)
		}
	}(resp.Body)

	if resp.IsError() {
		c.log.Errorf("Error: %s", resp.String())
		return false
	}

	var r map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
		return false
	}

	c.log.Infof("Client Version: %s", elasticsearchV9.Version)
	c.log.Infof("Server Version: %s", r["version"].(map[string]any)["number"])

	return true
}

// IndexExists 检查索引是否存在
func (c *Client) IndexExists(ctx context.Context, indexName string) (bool, error) {
	resp, err := c.Client.Indices.Exists(
		[]string{indexName},
		c.Client.Indices.Exists.WithContext(ctx),
	)
	if err != nil {
		c.log.Errorf("failed to check if index exists: %v", err)
		return false, err
	}

	return !resp.IsError(), nil
}

// CreateIndex 创建一条索引
//
//	如果mapping为空("")则表示不创建模型
func (c *Client) CreateIndex(ctx context.Context, indexName string, mapping, settings string) error {
	exist, err := c.IndexExists(ctx, indexName)
	if err != nil {
		return err
	}
	if exist {
		return ErrIndexAlreadyExists
	}

	body, err := MergeOptions(mapping, settings)
	if err != nil {
		c.log.Errorf("failed to merge options: %v", err)
		return err
	}

	resp, err := c.Client.Indices.Create(
		indexName,
		c.Client.Indices.Create.WithContext(ctx),
		c.Client.Indices.Create.WithBody(bytes.NewReader([]byte(body))),
	)
	if err != nil {
		c.log.Errorf("failed to create index: %v", err)
		return err
	}

	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			c.log.Errorf("failed to parse error message: %v", err)
			return err
		}

		c.log.Errorf("create index failed: %s", errResp.Error)

		return ErrCreateIndex
	}

	return nil
}

// DeleteIndex 删除一条索引
func (c *Client) DeleteIndex(ctx context.Context, indexName string) error {
	exist, err := c.IndexExists(ctx, indexName)
	if err != nil {
		c.log.Errorf("failed to check if index exists: %v", err)
		return err
	}
	if !exist {
		return ErrIndexNotFound
	}

	resp, err := c.Client.Indices.Delete(
		[]string{indexName},
		c.Client.Indices.Delete.WithContext(ctx),
	)
	if err != nil {
		c.log.Errorf("failed to delete index: %v", err)
		return err
	}

	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			c.log.Errorf("failed to parse error message: %v", err)
			return err
		}

		c.log.Errorf("delete index failed: %s", errResp.Error.Reason)

		return ErrDeleteIndex
	}

	return nil
}

// DeleteDocument 删除一条数据
func (c *Client) DeleteDocument(ctx context.Context, indexName, id string) error {
	_, err := c.Client.Delete(
		indexName, id,
		c.Client.Delete.WithContext(ctx),
	)
	if err != nil {
		c.log.Errorf("failed to delete document: %v", err)
		return err
	}
	return nil
}

// InsertDocument 插入一条数据
func (c *Client) InsertDocument(ctx context.Context, indexName, id string, data any) error {
	var err error

	var dataBytes []byte
	dataBytes, err = json.Marshal(data)
	if err != nil {
		c.log.Errorf("failed to marshal data: %v", err)
		return err
	}

	var resp *esapiV9.Response

	opts := []func(*esapiV9.IndexRequest){
		c.Client.Index.WithContext(ctx),
	}

	if id != "" {
		opts = append(opts, c.Client.Index.WithDocumentID(id))
	}

	resp, err = c.Client.Index(indexName, bytes.NewReader(dataBytes), opts...)
	if err != nil {
		c.log.Errorf("failed to call Index API: %v", err)
		return err
	}
	defer resp.Body.Close()

	if resp.IsError() {
		// 读取并解析错误（避免重复读取 Body）
		bodyBytes, _ := io.ReadAll(resp.Body)
		c.log.Errorf("insert document failed [%d]: %s", resp.StatusCode, string(bodyBytes))

		// 可选：区分 400/409 等不同错误类型
		if resp.StatusCode == 409 {
			return ErrDocumentConflict
		}
		return ErrInsertDocument
	}

	return nil
}

// BatchInsertDocument 批量插入数据
func (c *Client) BatchInsertDocument(ctx context.Context, indexName string, dataSet []any, ids []string) error {
	if len(dataSet) == 0 {
		return nil
	}
	if len(ids) > 0 && len(ids) != len(dataSet) {
		return fmt.Errorf("ids length (%d) must match dataSet length (%d) or be empty", len(ids), len(dataSet))
	}

	// 批次大小限制：避免 OOM（建议 500~2000 条/批）
	const maxBatchSize = 1000
	if len(dataSet) > maxBatchSize {
		c.log.Warnf("batch size %d exceeds limit %d, splitting into chunks", len(dataSet), maxBatchSize)
		for i := 0; i < len(dataSet); i += maxBatchSize {
			end := i + maxBatchSize
			if end > len(dataSet) {
				end = len(dataSet)
			}
			var subIDs []string
			if len(ids) > 0 {
				subIDs = ids[i:end]
			}
			if err := c.BatchInsertDocument(ctx, indexName, dataSet[i:end], subIDs); err != nil {
				return err
			}
		}
		return nil
	}

	var buf bytes.Buffer
	failedCount := 0

	for i, data := range dataSet {
		// 构建 metadata：指定 _id 实现幂等
		var meta map[string]interface{}
		if len(ids) > 0 && i < len(ids) && ids[i] != "" {
			meta = map[string]interface{}{
				"index": map[string]interface{}{
					"_id": ids[i],
				},
			}
		} else {
			meta = map[string]interface{}{
				"index": map[string]interface{}{}, // 无 ID 则让 ES 自动生成（不推荐）
			}
		}

		metaBytes, err := json.Marshal(meta)
		if err != nil {
			c.log.Errorf("failed to marshal meta for item %d: %v", i, err)
			failedCount++
			continue
		}
		metaBytes = append(metaBytes, '\n')

		dataBytes, err := json.Marshal(data)
		if err != nil {
			c.log.Errorf("failed to marshal data for item %d: %v", i, err)
			failedCount++
			continue
		}
		dataBytes = append(dataBytes, '\n') // NDJSON 要求每行以 \n 结尾

		buf.Grow(len(metaBytes) + len(dataBytes))
		buf.Write(metaBytes)
		buf.Write(dataBytes)
	}

	if buf.Len() == 0 {
		return fmt.Errorf("no valid documents to insert, %d items failed to marshal", failedCount)
	}

	// 执行 Bulk 请求
	resp, err := c.Client.Bulk(
		bytes.NewReader(buf.Bytes()),
		c.Client.Bulk.WithContext(ctx),
		c.Client.Bulk.WithIndex(indexName),
		c.Client.Bulk.WithRefresh("wait_for"), // 可选：确保写入后可立即搜索
	)
	if err != nil {
		c.log.Errorf("failed to call Bulk API: %v", err)
		return err
	}

	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			c.log.Warnf("failed to close bulk response body: %v", closeErr)
		}
	}()

	if resp.IsError() {
		// 读取 Body（只能读一次）
		bodyBytes, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			c.log.Errorf("bulk API error [%d], failed to read body: %v", resp.StatusCode, readErr)
		} else {
			c.log.Errorf("bulk API error [%d]: %s", resp.StatusCode, string(bodyBytes))
		}
		return ErrBatchInsertDocument
	}

	var bulkResp struct {
		Took   int  `json:"took"`
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			ID     string `json:"_id"`
			Status int    `json:"status"`
			Error  struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error,omitempty"`
		} `json:"items"`
	}

	// 重新读取 Body（上面已读完，需重置或使用副本）
	// 实际生产中建议在 resp.IsError() 判断前先用 io.TeeReader 缓存
	if err := json.NewDecoder(resp.Body).Decode(&bulkResp); err != nil {
		c.log.Errorf("failed to decode bulk response: %v", err)
		return err
	}

	// 处理部分失败
	if bulkResp.Errors {
		var failedIDs []string
		for i, item := range bulkResp.Items {
			for _, result := range item {
				if result.Status >= 400 {
					failedIDs = append(failedIDs, result.ID)
					c.log.Warnf("bulk item %d (id=%s) failed [%d]: %s - %s",
						i, result.ID, result.Status, result.Error.Type, result.Error.Reason)
				}
			}
		}
		c.log.Warnf("bulk insert completed with %d/%d failures", len(failedIDs), len(dataSet))
		return &PartialFailureError{
			Total:     len(dataSet),
			Failed:    len(failedIDs),
			FailedIDs: failedIDs,
		}
	}

	c.log.Debugf("bulk insert succeeded: %d documents to %s", len(dataSet), indexName)
	return nil
}

func (c *Client) UpdateDocument(ctx context.Context, indexName string, pk string, doc any) error {
	data, err := json.Marshal(doc)
	if err != nil {
		c.log.Errorf("failed to marshal data: %v", err)
		return err
	}

	_, err = c.Client.Update(
		indexName, pk,
		bytes.NewReader(data),
		c.Client.Update.WithContext(ctx),
	)
	if err != nil {
		c.log.Errorf("failed to update document: %v", err)
		return err
	}

	return nil
}

// GetDocument 查询数据
func (c *Client) GetDocument(
	ctx context.Context,
	indexName string,
	id string,
	sourceFields []string,
	out any,
) error {
	resp, err := c.Client.Get(
		indexName, id,
		c.Client.Get.WithContext(ctx),
		c.Client.Get.WithSource(sourceFields...), // 指定返回的字段
	)
	if err != nil {
		c.log.Errorf("failed to get document: %v", err)
		return err
	}

	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			c.log.Errorf("failed to parse error message: %v", err)
			return err
		}

		if resp.StatusCode == 404 {
			c.log.Warnf("document not found: %s", errResp.Error.Reason)
			return ErrDocumentNotFound
		}

		c.log.Errorf("get document failed: %s", errResp.Error.Reason)

		return ErrGetDocument
	}

	if err = json.NewDecoder(resp.Body).Decode(&out); err != nil {
		c.log.Errorf("failed to decode document: %v", err)
		return err
	}

	return nil
}

func (c *Client) Search(
	ctx context.Context,
	indexName string,
	req *paginationV1.PagingRequest,
) (*SearchResult, error) {
	var query string
	ParseQueryString(req.GetQuery())

	sortBy := make(map[string]bool)

	pageSize := req.GetPageSize()
	if pageSize <= 0 {
		pageSize = 20 // Default page size
	}

	return c.search(ctx, indexName, query, nil, sortBy, int(req.GetPage()), int(pageSize))
}

// search 查询数据
//
// @param ctx 上下文
// @param indexName 索引名
// @param query 查询条件，例如：field1:value1 AND field2:value2
// @param sourceFields 指定返回的字段，传入nil表示返回所有字段
// @param sortBy 排序
// @param from 分页的页码
// @param pageSize 分页每页的行数
func (c *Client) search(
	ctx context.Context,
	indexName string,
	query string,
	sourceFields []string,
	sortBy map[string]bool,
	from, pageSize int,
) (*SearchResult, error) {
	var sorts []string
	for k, v := range sortBy {
		if v {
			sorts = append(sorts, k+":asc")
		} else {
			sorts = append(sorts, k+":desc")
		}
	}

	resp, err := c.Client.Search(
		c.Client.Search.WithContext(ctx),
		c.Client.Search.WithIndex(indexName),
		c.Client.Search.WithFrom(from),
		c.Client.Search.WithSize(pageSize),
		c.Client.Search.WithSort(sorts...),
		c.Client.Search.WithQuery(query),
		c.Client.Search.WithSource(sourceFields...), // 指定返回的字段
	)
	if err != nil {
		c.log.Errorf("failed to search documents: %v", err)
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		if err = Body.Close(); err != nil {
			c.log.Errorf("failed to close response body: %v", err)
		}
	}(resp.Body)

	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			return nil, err
		}

		c.log.Errorf("search document failed: %s", errResp.Error.Reason)

		return nil, ErrSearchDocument
	}

	var searchResult SearchResult
	if err = json.NewDecoder(resp.Body).Decode(&searchResult); err != nil {
		c.log.Errorf("failed to decode search result: %v", err)
		return nil, err
	}

	return &searchResult, nil
}

// CreateIndexTemplate 创建或更新索引模板（适用于Elasticsearch 7.x及以上）
func (c *Client) CreateIndexTemplate(ctx context.Context, templateName string, templateBody string) error {
	resp, err := c.Client.Indices.PutIndexTemplate(
		templateName,
		bytes.NewReader([]byte(templateBody)),
		c.Client.Indices.PutIndexTemplate.WithContext(ctx),
	)
	if err != nil {
		c.log.Errorf("failed to create index template: %v", err)
		return err
	}
	defer func(Body io.ReadCloser) {
		if err = Body.Close(); err != nil {
			c.log.Errorf("failed to close response body: %v", err)
		}
	}(resp.Body)

	if resp.IsError() {
		var errResp map[string]any
		if err = json.NewDecoder(resp.Body).Decode(&errResp); err == nil {
			c.log.Errorf("create index template failed: %v", errResp)
		}
		return ErrCreateIndex
	}
	return nil
}

// ExistsIndexTemplate 判断索引模板是否存在（适用于Elasticsearch 7.x及以上）
func (c *Client) ExistsIndexTemplate(ctx context.Context, templateName string) (bool, error) {
	resp, err := c.Client.Indices.ExistsIndexTemplate(
		templateName,
		c.Client.Indices.ExistsIndexTemplate.WithContext(ctx),
	)
	if err != nil {
		c.log.Errorf("failed to check if index template exists: %v", err)
		return false, err
	}
	defer func(Body io.ReadCloser) {
		if err = Body.Close(); err != nil {
			c.log.Errorf("failed to close response body: %v", err)
		}
	}(resp.Body)
	return !resp.IsError(), nil
}

// CreateILMPolicy 创建或更新ILM策略
func (c *Client) CreateILMPolicy(ctx context.Context, policyName string, policyBody string) error {
	resp, err := c.Client.ILM.PutLifecycle(
		bytes.NewReader([]byte(policyBody)),
		policyName,
		c.Client.ILM.PutLifecycle.WithContext(ctx),
	)
	if err != nil {
		c.log.Errorf("failed to create ILM policy: %v", err)
		return err
	}
	defer func(Body io.ReadCloser) {
		closeErr := Body.Close()
		if closeErr != nil {
			c.log.Errorf("failed to close response body: %v", closeErr)
		}
	}(resp.Body)

	if resp.IsError() {
		var errResp map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		c.log.Errorf("create ILM policy failed: status=%d, resp=%v", resp.StatusCode, errResp)
		return ErrCreateILMPolicy
	}
	return nil
}

// DeleteILMPolicy 删除ILM策略
func (c *Client) DeleteILMPolicy(ctx context.Context, policyName string) error {
	resp, err := c.Client.ILM.DeleteLifecycle(
		policyName,
		c.Client.ILM.DeleteLifecycle.WithContext(ctx),
	)
	if err != nil {
		c.log.Errorf("failed to delete ILM policy: %v", err)
		return err
	}
	defer func(Body io.ReadCloser) {
		if err = Body.Close(); err != nil {
			c.log.Errorf("failed to close response body: %v", err)
		}
	}(resp.Body)

	if resp.IsError() {
		var errResp map[string]any
		if err = json.NewDecoder(resp.Body).Decode(&errResp); err == nil {
			c.log.Errorf("delete ILM policy failed: %v", errResp)
		}
		return ErrDeleteIndex
	}
	return nil
}

// SearchWithHighlight 支持高亮参数的搜索
func (c *Client) SearchWithHighlight(
	ctx context.Context,
	indexName string,
	query map[string]any,
	highlight map[string]any,
	sourceFields []string,
	sortBy map[string]bool,
	from, pageSize int,
) (*SearchResult, error) {
	body := make(map[string]any)
	if query != nil {
		body["query"] = query
	}
	if highlight != nil {
		body["highlight"] = highlight
	}
	if len(sourceFields) > 0 {
		body["_source"] = sourceFields
	}
	if sortBy != nil && len(sortBy) > 0 {
		sorts := make([]map[string]any, 0, len(sortBy))
		for k, v := range sortBy {
			order := "desc"
			if v {
				order = "asc"
			}
			sorts = append(sorts, map[string]any{k: map[string]any{"order": order}})
		}
		body["sort"] = sorts
	}
	body["from"] = from
	body["size"] = pageSize

	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(body); err != nil {
		c.log.Errorf("failed to encode search body: %v", err)
		return nil, err
	}

	resp, err := c.Client.Search(
		c.Client.Search.WithContext(ctx),
		c.Client.Search.WithIndex(indexName),
		c.Client.Search.WithBody(buf),
	)
	if err != nil {
		c.log.Errorf("failed to search documents: %v", err)
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		if err := Body.Close(); err != nil {
			c.log.Errorf("failed to close response body: %v", err)
		}
	}(resp.Body)

	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			return nil, err
		}
		c.log.Errorf("search document failed: %s", errResp.Error.Reason)
		return nil, ErrSearchDocument
	}

	var searchResult SearchResult
	if err = json.NewDecoder(resp.Body).Decode(&searchResult); err != nil {
		c.log.Errorf("failed to decode search result: %v", err)
		return nil, err
	}

	return &searchResult, nil
}

// SearchBySQL 通过 SQL 查询数据（适用于 Elasticsearch 7.14+，需启用 SQL 插件）
func (c *Client) SearchBySQL(ctx context.Context, sql string) (*SQLResult, error) {
	// 必须包成 JSON：{"query": "SELECT ..."}
	reqBody := map[string]string{
		"query": sql,
	}
	jsonBody, _ := json.Marshal(reqBody)

	// 执行 SQL API
	resp, err := c.Client.SQL.Query(
		bytes.NewReader(jsonBody),
		c.Client.SQL.Query.WithContext(ctx),
	)
	if err != nil {
		c.log.Errorf("sql query error: %v", err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.IsError() {
		body, _ := io.ReadAll(resp.Body)
		c.log.Errorf("sql query failed: %s", string(body))
		return nil, ErrSearchDocument
	}

	// 解析 SQL 专用结构
	var result SQLResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		c.log.Errorf("decode sql result error: %v", err)
		return nil, err
	}

	return &result, nil
}

// SearchBySQLTo 执行SQL查询，直接映射到结构体切片
// out 必须是切片指针，例如 &[]User{}
func (c *Client) SearchBySQLTo(ctx context.Context, sql string, out any) error {
	reqBody := map[string]any{
		"query":      sql,
		"format":     "json",
		"fetch_size": 1000,
	}
	jsonBody, _ := json.Marshal(reqBody)

	resp, err := c.Client.SQL.Query(
		bytes.NewReader(jsonBody),
		c.Client.SQL.Query.WithContext(ctx),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.IsError() {
		body, _ := io.ReadAll(resp.Body)
		c.log.Errorf("sql error: %s", string(body))
		return ErrSearchDocument
	}

	return json.NewDecoder(resp.Body).Decode(out)
}
