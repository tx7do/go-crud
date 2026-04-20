package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/go-kratos/kratos/v2/log"

	opensearchV4 "github.com/opensearch-project/opensearch-go/v4"
	opensearchapiV4 "github.com/opensearch-project/opensearch-go/v4/opensearchapi"

	"github.com/go-kratos/kratos/v2/encoding"
	_ "github.com/go-kratos/kratos/v2/encoding/json"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

type Client struct {
	*opensearchV4.Client
	options *opensearchV4.Config

	log   *log.Helper
	codec encoding.Codec
}

func NewOpenSearchClient(opts ...Option) (*Client, error) {
	c := &Client{
		options: &opensearchV4.Config{},
		log:     log.NewHelper(log.DefaultLogger),
		codec:   encoding.GetCodec("json"),
	}

	for _, o := range opts {
		o(c)
	}

	if err := c.createOSClient(c.options); err != nil {
		return nil, err
	}

	return c, nil
}

// createOSClient 创建OpenSearch客户端
func (c *Client) createOSClient(options *opensearchV4.Config) error {
	cli, err := opensearchV4.NewClient(*options)
	if err != nil {
		c.log.Errorf("failed to create OpenSearch client: %v", err)
		return err
	}

	c.Client = cli

	return nil
}

// CheckConnectStatus 检查连接
func (c *Client) CheckConnectStatus(ctx context.Context) bool {
	if c.Client == nil {
		return false
	}

	req := opensearchapiV4.InfoReq{}
	infoResp := opensearchapiV4.InfoResp{}

	resp, err := c.Client.Do(ctx, req, &infoResp)
	if err != nil {
		c.log.Errorf("failed to connect to OpenSearch: %v", err)
		return false
	}

	if resp.IsError() {
		c.log.Errorf("Error: %s", resp.String())
		return false
	}

	c.log.Infof("Client Version: %s", opensearchV4.Version)
	c.log.Infof("Server Version: %s", infoResp.Version.Number)

	return true
}

// IndexExists 检查索引是否存在
func (c *Client) IndexExists(ctx context.Context, indexName string) (bool, error) {
	req := opensearchapiV4.IndicesExistsReq{
		Indices: []string{indexName},
	}

	resp, err := c.Client.Do(ctx, req, nil)
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

	req := opensearchapiV4.IndicesCreateReq{
		Index: indexName,
		Body:  bytes.NewReader([]byte(body)),
	}

	createResp := opensearchapiV4.IndicesCreateResp{}

	resp, err := c.Client.Do(ctx, req, &createResp)
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

	req := opensearchapiV4.IndicesDeleteReq{
		Indices: []string{indexName},
	}

	resp, err := c.Client.Do(ctx, req, nil)
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
	req := opensearchapiV4.DocumentDeleteReq{
		Index:      indexName,
		DocumentID: id,
	}

	resp, err := c.Client.Do(ctx, req, nil)
	if err != nil {
		c.log.Errorf("failed to delete document: %v", err)
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

		c.log.Errorf("delete document failed: %s", errResp.Error.Reason)

		return ErrDeleteDocument
	}

	return nil
}

// InsertDocument 插入一条数据
func (c *Client) InsertDocument(ctx context.Context, indexName, documentId string, data any) error {
	var err error

	var dataBytes []byte
	dataBytes, err = c.codec.Marshal(data)
	if err != nil {
		c.log.Errorf("failed to marshal data: %v", err)
		return err
	}

	req := &opensearchapiV4.IndexReq{
		Index:      indexName,
		DocumentID: documentId,
		Body:       bytes.NewReader(dataBytes),
	}

	resp, err := c.Client.Do(ctx, req, nil)
	if err != nil {
		c.log.Errorf("failed to call Index API: %v", err)
		return err
	}

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

	var buf bytes.Buffer
	for i, data := range dataSet {
		meta := map[string]any{"index": map[string]any{"_index": indexName}}
		if ids != nil && i < len(ids) && ids[i] != "" {
			meta["index"].(map[string]any)["_id"] = ids[i]
		}
		metaBytes, err := json.Marshal(meta)
		if err != nil {
			c.log.Errorf("failed to marshal meta: %v", err)
			continue
		}
		buf.Write(metaBytes)
		buf.WriteByte('\n')

		dataBytes, err := c.codec.Marshal(data)
		if err != nil {
			c.log.Errorf("failed to marshal data: %v", err)
			continue
		}
		buf.Write(dataBytes)
		buf.WriteByte('\n')
	}

	req := &opensearchapiV4.BulkReq{
		Body: bytes.NewReader(buf.Bytes()),
	}

	bulkResp := opensearchapiV4.BulkResp{}
	resp, err := c.Client.Do(ctx, req, &bulkResp)
	if err != nil {
		c.log.Errorf("failed to perform bulk insert: %v", err)
		return err
	}
	if resp.IsError() {
		bodyBytes, _ := io.ReadAll(resp.Body)
		c.log.Errorf("bulk insert failed [%d]: %s", resp.StatusCode, string(bodyBytes))
		return ErrBatchInsertDocument
	}
	return nil
}

func (c *Client) UpdateDocument(ctx context.Context, indexName string, pk string, doc any) error {
	if pk == "" {
		return ErrDocumentNotFound
	}
	dataBytes, err := c.codec.Marshal(map[string]any{"doc": doc})
	if err != nil {
		c.log.Errorf("failed to marshal update doc: %v", err)
		return err
	}
	req := &opensearchapiV4.UpdateReq{
		Index:      indexName,
		DocumentID: pk,
		Body:       bytes.NewReader(dataBytes),
	}
	resp, err := c.Client.Do(ctx, req, nil)
	if err != nil {
		c.log.Errorf("failed to call Update API: %v", err)
		return err
	}
	if resp.IsError() {
		bodyBytes, _ := io.ReadAll(resp.Body)
		c.log.Errorf("update document failed [%d]: %s", resp.StatusCode, string(bodyBytes))
		if resp.StatusCode == 404 {
			return ErrDocumentNotFound
		}
		return ErrUpdateDocument
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
	req := opensearchapiV4.DocumentGetReq{
		Index:      indexName,
		DocumentID: id,
		Params: opensearchapiV4.DocumentGetParams{
			SourceIncludes: sourceFields,
		},
	}

	getResp := opensearchapiV4.DocumentGetResp{}

	resp, err := c.Client.Do(ctx, req, &getResp)
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

	if err = c.codec.Unmarshal(getResp.Source, out); err != nil {
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
	body := make(map[string]any)
	if query != "" {
		body["query"] = map[string]any{"query_string": map[string]any{"query": query}}
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

	req := &opensearchapiV4.SearchReq{
		Indices: []string{indexName},
		Body:    buf,
	}
	var searchResult SearchResult
	resp, err := c.Client.Do(ctx, req, &searchResult)
	if err != nil {
		c.log.Errorf("failed to search documents: %v", err)
		return nil, err
	}
	if resp.IsError() {
		bodyBytes, _ := io.ReadAll(resp.Body)
		c.log.Errorf("search document failed [%d]: %s", resp.StatusCode, string(bodyBytes))
		return nil, ErrSearchDocument
	}
	return &searchResult, nil
}

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

	req := &opensearchapiV4.SearchReq{
		Indices: []string{indexName},
		Body:    buf,
	}
	var searchResult SearchResult
	resp, err := c.Client.Do(ctx, req, &searchResult)
	if err != nil {
		c.log.Errorf("failed to search documents: %v", err)
		return nil, err
	}
	if resp.IsError() {
		bodyBytes, _ := io.ReadAll(resp.Body)
		c.log.Errorf("search document failed [%d]: %s", resp.StatusCode, string(bodyBytes))
		return nil, ErrSearchDocument
	}
	return &searchResult, nil
}

// CreateIndexTemplate 创建或更新索引模板（适用于Elasticsearch 7.x及以上）
func (c *Client) CreateIndexTemplate(ctx context.Context, templateName string, templateBody string) error {
	req := opensearchapiV4.IndexTemplateCreateReq{
		IndexTemplate: templateName,
		Body:          bytes.NewReader([]byte(templateBody)),
	}

	resp, err := c.Client.Do(ctx, req, nil)
	if err != nil {
		c.log.Errorf("failed to create index template: %v", err)
		return err
	}

	if resp.IsError() {
		var errResp map[string]any
		if err = json.NewDecoder(resp.Body).Decode(&errResp); err == nil {
			c.log.Errorf("create index template failed: %v", errResp)
		}
		return ErrCreateIndex
	}

	return nil
}

// ExistsIndexTemplate 判断索引模板是否存在
func (c *Client) ExistsIndexTemplate(ctx context.Context, templateName string) (bool, error) {
	req := opensearchapiV4.IndexTemplateExistsReq{
		IndexTemplate: templateName,
	}

	resp, err := c.Client.Do(ctx, req, nil)
	if err != nil {
		c.log.Errorf("failed to check if index template exists: %v", err)
		return false, err
	}

	return !resp.IsError(), nil
}

// CreateILMPolicy 创建或更新ILM策略
func (c *Client) CreateILMPolicy(ctx context.Context, policyName string, policyBody string) error {
	// OpenSearch ILM API: PUT _ilm/policy/{policyName}
	if c.Client == nil {
		return ErrRequestFailed
	}
	endpoint := "/_ilm/policy/" + policyName
	req, err := http.NewRequestWithContext(ctx, "PUT", endpoint, bytes.NewReader([]byte(policyBody)))
	if err != nil {
		c.log.Errorf("failed to create ILM policy request: %v", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Client.Perform(req)
	if err != nil {
		c.log.Errorf("failed to perform ILM policy request: %v", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			c.log.Errorf("failed to parse error message: %v", err)
			return err
		}
		c.log.Errorf("create ILM policy failed: %s", errResp.Error.Reason)
		return ErrCreateILMPolicy
	}

	return nil
}

// DeleteILMPolicy 删除ILM策略
func (c *Client) DeleteILMPolicy(ctx context.Context, policyName string) error {
	// OpenSearch ILM API: DELETE _ilm/policy/{policyName}
	if c.Client == nil {
		return ErrRequestFailed
	}
	endpoint := "/_ilm/policy/" + policyName
	req, err := http.NewRequestWithContext(ctx, "DELETE", endpoint, nil)
	if err != nil {
		c.log.Errorf("failed to create ILM policy delete request: %v", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Client.Perform(req)
	if err != nil {
		c.log.Errorf("failed to perform ILM policy delete request: %v", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			c.log.Errorf("failed to parse error message: %v", err)
			return err
		}
		c.log.Errorf("delete ILM policy failed: %s", errResp.Error.Reason)
		return ErrDeleteILMPolicy
	}

	return nil
}
