package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/tx7do/go-wind/log"

	opensearchV4 "github.com/opensearch-project/opensearch-go/v4"
	opensearchapiV4 "github.com/opensearch-project/opensearch-go/v4/opensearchapi"

	"github.com/tx7do/go-wind-plugins/encoding"
	_ "github.com/tx7do/go-wind-plugins/encoding/json"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	paginationFilter "github.com/tx7do/go-crud/pagination/filter"
	"github.com/tx7do/go-crud/pagination/paginator"
	paginationSorting "github.com/tx7do/go-crud/pagination/sorting"

	"github.com/tx7do/go-crud/opensearch/field"
	"github.com/tx7do/go-crud/opensearch/filter"
	paging "github.com/tx7do/go-crud/opensearch/pagination"
	"github.com/tx7do/go-crud/opensearch/query"
	"github.com/tx7do/go-crud/opensearch/sorting"
)

type Client struct {
	*opensearchV4.Client
	options *opensearchV4.Config
	codec   encoding.Codec

	offsetPaginator *paging.OffsetPaginator
	pagePaginator   *paging.PagePaginator
	tokenPaginator  *paging.TokenPaginator

	structuredFilter *filter.StructuredFilter

	structuredSorting      *sorting.StructuredSorting
	orderByStringConverter *paginationSorting.OrderByStringConverter

	fieldSelector *field.Selector
}

func NewOpenSearchClient(opts ...Option) (*Client, error) {
	c := &Client{
		options: &opensearchV4.Config{},

		codec: encoding.GetCodec("json"),

		structuredSorting: sorting.NewStructuredSorting(),

		offsetPaginator: paging.NewOffsetPaginator(),
		pagePaginator:   paging.NewPagePaginator(),
		tokenPaginator:  paging.NewTokenPaginator(),

		structuredFilter: filter.NewStructuredFilter(),

		orderByStringConverter: paginationSorting.NewOrderByStringConverter(),

		fieldSelector: field.NewFieldSelector(),
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
		log.Error(context.Background(), fmt.Sprintf("failed to create OpenSearch client: %v", err))
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

	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodGet, req, &infoResp)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to connect to OpenSearch: %v", err))
		return false
	}

	if resp.IsError() {
		log.Error(context.Background(), fmt.Sprintf("Error: %s", resp.String()))
		return false
	}

	log.Info(context.Background(), fmt.Sprintf("Client Version: %s", opensearchV4.Version))
	log.Info(context.Background(), fmt.Sprintf("Server Version: %s", infoResp.Version.Number))

	return true
}

// IndexExists 检查索引是否存在
func (c *Client) IndexExists(ctx context.Context, indexName string) (bool, error) {
	req := opensearchapiV4.IndicesExistsReq{
		Indices: []string{indexName},
	}

	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodHead, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to check if index exists: %v", err))
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
		log.Error(context.Background(), fmt.Sprintf("failed to merge options: %v", err))
		return err
	}

	req := opensearchapiV4.IndicesCreateReq{
		Index: indexName,
		Body:  bytes.NewReader([]byte(body)),
	}

	createResp := opensearchapiV4.IndicesCreateResp{}

	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPut, req, &createResp)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to create index: %v", err))
		return err
	}

	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}

		log.Error(context.Background(), fmt.Sprintf("create index failed: %s", errResp.Error))

		return ErrCreateIndex
	}

	return nil
}

// DeleteIndex 删除一条索引
func (c *Client) DeleteIndex(ctx context.Context, indexName string) error {
	exist, err := c.IndexExists(ctx, indexName)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to check if index exists: %v", err))
		return err
	}
	if !exist {
		return ErrIndexNotFound
	}

	req := opensearchapiV4.IndicesDeleteReq{
		Indices: []string{indexName},
	}

	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodDelete, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to delete index: %v", err))
		return err
	}

	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}

		log.Error(context.Background(), fmt.Sprintf("delete index failed: %s", errResp.Error.Reason))

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

	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodDelete, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to delete document: %v", err))
		return err
	}

	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}

		if resp.StatusCode == 404 {
			log.Warn(context.Background(), fmt.Sprintf("document not found: %s", errResp.Error.Reason))
			return ErrDocumentNotFound
		}

		log.Error(context.Background(), fmt.Sprintf("delete document failed: %s", errResp.Error.Reason))

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
		log.Error(context.Background(), fmt.Sprintf("failed to marshal data: %v", err))
		return err
	}

	req := &opensearchapiV4.IndexReq{
		Index:      indexName,
		DocumentID: documentId,
		Body:       bytes.NewReader(dataBytes),
	}

	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPut, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to call Index API: %v", err))
		return err
	}

	if resp.IsError() {
		// 读取并解析错误（避免重复读取 Body）
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Error(context.Background(), fmt.Sprintf("insert document failed [%d]: %s", resp.StatusCode, string(bodyBytes)))

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
		metaBytes, err := c.codec.Marshal(meta)
		if err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to marshal meta: %v", err))
			continue
		}
		buf.Write(metaBytes)
		buf.WriteByte('\n')

		dataBytes, err := c.codec.Marshal(data)
		if err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to marshal data: %v", err))
			continue
		}
		buf.Write(dataBytes)
		buf.WriteByte('\n')
	}

	req := &opensearchapiV4.BulkReq{
		Body: bytes.NewReader(buf.Bytes()),
	}

	bulkResp := opensearchapiV4.BulkResp{}
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPost, req, &bulkResp)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to perform bulk insert: %v", err))
		return err
	}
	if resp.IsError() {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Error(context.Background(), fmt.Sprintf("bulk insert failed [%d]: %s", resp.StatusCode, string(bodyBytes)))
		return ErrBatchInsertDocument
	}
	return nil
}

// MultiGet 批量获取文档。返回每个 id 对应文档的 _source 原始 JSON（json.RawMessage）；
// 若该文档未找到或请求被拒绝，对应位置为 nil。sourceFields 为允许返回的字段白名单（可空）。
func (c *Client) MultiGet(ctx context.Context, index string, ids []string, sourceFields []string) ([]json.RawMessage, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	type mgetDoc struct {
		Index string `json:"_index"`
		ID    string `json:"_id"`
	}
	type mgetBody struct {
		Docs []mgetDoc `json:"docs"`
	}
	body := mgetBody{Docs: make([]mgetDoc, 0, len(ids))}
	for _, id := range ids {
		if id == "" {
			continue
		}
		body.Docs = append(body.Docs, mgetDoc{Index: index, ID: id})
	}
	if len(body.Docs) == 0 {
		return nil, nil
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to marshal mget body: %v", err))
		return nil, err
	}
	// OpenSearch mget 不支持 per-request source field 白名单，sourceFields 在此忽略。
	_ = sourceFields
	req := opensearchapiV4.MGetReq{
		Index: index,
		Body:  bytes.NewReader(bodyBytes),
	}
	var parsed struct {
		Docs []struct {
			Found  bool            `json:"found"`
			Source json.RawMessage `json:"_source"`
		} `json:"docs"`
	}
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPost, req, &parsed)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to call mget: %v", err))
		return nil, err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("mget failed: %s", errResp.Error))
		return nil, ErrGetDocument
	}
	results := make([]json.RawMessage, len(ids))
	di := 0
	for i, id := range ids {
		if id == "" {
			results[i] = nil
			continue
		}
		if di < len(parsed.Docs) {
			if parsed.Docs[di].Found {
				results[i] = parsed.Docs[di].Source
			} else {
				results[i] = nil
			}
		} else {
			results[i] = nil
		}
		di++
	}
	return results, nil
}

// BatchUpdateDocument 批量更新文档。采用 bulk update action（NDJSON）。
// 空 dataSet 短路返回 nil。失败返回 ErrBatchInsertDocument。
func (c *Client) BatchUpdateDocument(ctx context.Context, indexName string, dataSet []any, ids []string) error {
	if len(dataSet) == 0 {
		return nil
	}
	if len(ids) > 0 && len(ids) != len(dataSet) {
		return fmt.Errorf("ids length (%d) must match dataSet length (%d) or be empty", len(ids), len(dataSet))
	}

	var buf bytes.Buffer
	for i, data := range dataSet {
		var id string
		if ids != nil && i < len(ids) && ids[i] != "" {
			id = ids[i]
		}
		if id == "" {
			log.Error(context.Background(), fmt.Sprintf("batch update item %d missing _id, skipped", i))
			continue
		}
		meta := map[string]any{
			"update": map[string]any{
				"_id":    id,
				"_index": indexName,
			},
		}
		metaBytes, err := c.codec.Marshal(meta)
		if err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to marshal update meta: %v", err))
			continue
		}
		buf.Write(metaBytes)
		buf.WriteByte('\n')
		docBytes, err := c.codec.Marshal(map[string]any{"doc": data})
		if err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to marshal update data: %v", err))
			continue
		}
		buf.Write(docBytes)
		buf.WriteByte('\n')
	}
	if buf.Len() == 0 {
		return fmt.Errorf("no valid documents to update")
	}

	req := &opensearchapiV4.BulkReq{
		Index: indexName,
		Body:  bytes.NewReader(buf.Bytes()),
	}
	bulkResp := opensearchapiV4.BulkResp{}
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPost, req, &bulkResp)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to perform bulk update: %v", err))
		return err
	}
	if resp.IsError() {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Error(context.Background(), fmt.Sprintf("bulk update failed [%d]: %s", resp.StatusCode, string(bodyBytes)))
		return ErrBatchInsertDocument
	}
	return nil
}

// BatchDeleteDocument 批量删除文档。空 ids 短路返回 nil。
func (c *Client) BatchDeleteDocument(ctx context.Context, indexName string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, id := range ids {
		if id == "" {
			continue
		}
		meta := map[string]any{
			"delete": map[string]any{
				"_id":    id,
				"_index": indexName,
			},
		}
		metaBytes, err := c.codec.Marshal(meta)
		if err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to marshal delete meta: %v", err))
			continue
		}
		buf.Write(metaBytes)
		buf.WriteByte('\n')
	}
	if buf.Len() == 0 {
		return fmt.Errorf("no valid ids to delete")
	}

	req := &opensearchapiV4.BulkReq{
		Index: indexName,
		Body:  bytes.NewReader(buf.Bytes()),
	}
	bulkResp := opensearchapiV4.BulkResp{}
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPost, req, &bulkResp)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to perform bulk delete: %v", err))
		return err
	}
	if resp.IsError() {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Error(context.Background(), fmt.Sprintf("bulk delete failed [%d]: %s", resp.StatusCode, string(bodyBytes)))
		return ErrBatchInsertDocument
	}
	return nil
}

// UpdateByQuery 按查询更新文档。bodyJSON 为查询体（DSL）。
func (c *Client) UpdateByQuery(ctx context.Context, index, bodyJSON string) error {
	req := opensearchapiV4.UpdateByQueryReq{
		Indices: []string{index},
		Body:    bytes.NewReader([]byte(bodyJSON)),
	}
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPost, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to update by query: %v", err))
		return err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}
		log.Error(context.Background(), fmt.Sprintf("update by query failed: %s", errResp.Error))
		return ErrUpdateDocument
	}
	return nil
}

// DeleteByQuery 按查询删除文档。bodyJSON 为查询体（DSL）。
func (c *Client) DeleteByQuery(ctx context.Context, index, bodyJSON string) error {
	req := opensearchapiV4.DocumentDeleteByQueryReq{
		Indices: []string{index},
		Body:    bytes.NewReader([]byte(bodyJSON)),
	}
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPost, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to delete by query: %v", err))
		return err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}
		log.Error(context.Background(), fmt.Sprintf("delete by query failed: %s", errResp.Error))
		return ErrDeleteDocument
	}
	return nil
}

// UpdateDocument 更新一条数据
func (c *Client) UpdateDocument(ctx context.Context, indexName string, pk string, doc any) error {
	if pk == "" {
		return ErrDocumentNotFound
	}
	dataBytes, err := c.codec.Marshal(map[string]any{"doc": doc})
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to marshal update doc: %v", err))
		return err
	}
	req := &opensearchapiV4.UpdateReq{
		Index:      indexName,
		DocumentID: pk,
		Body:       bytes.NewReader(dataBytes),
	}
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPost, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to call Update API: %v", err))
		return err
	}
	if resp.IsError() {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Error(context.Background(), fmt.Sprintf("update document failed [%d]: %s", resp.StatusCode, string(bodyBytes)))
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
	documentId string,
	sourceFields []string,
	out any,
) error {
	req := opensearchapiV4.DocumentGetReq{
		Index:      indexName,
		DocumentID: documentId,
		Params: opensearchapiV4.DocumentGetParams{
			SourceIncludes: sourceFields,
		},
	}

	getResp := opensearchapiV4.DocumentGetResp{}

	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodGet, req, &getResp)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to get document: %v", err))
		return err
	}

	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}

		if resp.StatusCode == 404 {
			log.Warn(context.Background(), fmt.Sprintf("document not found: %s", errResp.Error.Reason))
			return ErrDocumentNotFound
		}

		log.Error(context.Background(), fmt.Sprintf("get document failed: %s", errResp.Error.Reason))

		return ErrGetDocument
	}

	if err = c.codec.Unmarshal(getResp.Source, out); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to decode document: %v", err))
		return err
	}

	return nil
}

// CreateIndexTemplate 创建或更新索引模板
func (c *Client) CreateIndexTemplate(ctx context.Context, templateName string, templateBody string) error {
	req := opensearchapiV4.IndexTemplateCreateReq{
		IndexTemplate: templateName,
		Body:          bytes.NewReader([]byte(templateBody)),
	}

	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPut, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to create index template: %v", err))
		return err
	}

	if resp.IsError() {
		var errResp map[string]any
		if err = json.NewDecoder(resp.Body).Decode(&errResp); err == nil {
			log.Error(context.Background(), fmt.Sprintf("create index template failed: %v", errResp))
		}
		return ErrCreateTemplate
	}

	return nil
}

// ExistsIndexTemplate 判断索引模板是否存在
func (c *Client) ExistsIndexTemplate(ctx context.Context, templateName string) (bool, error) {
	req := opensearchapiV4.IndexTemplateExistsReq{
		IndexTemplate: templateName,
	}

	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodHead, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to check if index template exists: %v", err))
		return false, err
	}

	return !resp.IsError(), nil
}

// DeleteIndexTemplate 删除索引模板
func (c *Client) DeleteIndexTemplate(ctx context.Context, templateName string) error {
	req := opensearchapiV4.IndexTemplateDeleteReq{
		IndexTemplate: templateName,
	}

	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodDelete, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to delete index template: %v", err))
		return err
	}

	if resp.IsError() {
		var errResp map[string]any
		if err = json.NewDecoder(resp.Body).Decode(&errResp); err == nil {
			log.Error(context.Background(), fmt.Sprintf("delete index template failed: %v", errResp))
		}
		return ErrDeleteTemplate
	}

	return nil
}

// CreateComponentTemplate 创建或更新组件模板
func (c *Client) CreateComponentTemplate(ctx context.Context, templateName string, templateBody string) error {
	req := opensearchapiV4.ComponentTemplateCreateReq{
		ComponentTemplate: templateName,
		Body:              bytes.NewReader([]byte(templateBody)),
	}

	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPut, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to create component template: %v", err))
		return err
	}

	if resp.IsError() {
		var errResp map[string]any
		if err = json.NewDecoder(resp.Body).Decode(&errResp); err == nil {
			log.Error(context.Background(), fmt.Sprintf("create component template failed: %v", errResp))
		}
		return ErrCreateTemplate
	}

	return nil
}

// DeleteComponentTemplate 删除组件模板
func (c *Client) DeleteComponentTemplate(ctx context.Context, templateName string) error {
	req := opensearchapiV4.ComponentTemplateDeleteReq{
		ComponentTemplate: templateName,
	}

	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodDelete, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to delete component template: %v", err))
		return err
	}

	if resp.IsError() {
		var errResp map[string]any
		if err = json.NewDecoder(resp.Body).Decode(&errResp); err == nil {
			log.Error(context.Background(), fmt.Sprintf("delete component template failed: %v", errResp))
		}
		return ErrDeleteTemplate
	}

	return nil
}

// ExistsComponentTemplate 判断组件模板是否存在
func (c *Client) ExistsComponentTemplate(ctx context.Context, templateName string) (bool, error) {
	req := opensearchapiV4.ComponentTemplateExistsReq{
		ComponentTemplate: templateName,
	}

	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodHead, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to check if component template exists: %v", err))
		return false, err
	}

	return !resp.IsError(), nil
}

// CreateISMPolicy 创建或更新ISM策略
func (c *Client) CreateISMPolicy(ctx context.Context, policyName string, policyBody string) error {
	if c.Client == nil {
		return ErrRequestFailed
	}
	endpoint := "/_plugins/_ism/policies/" + policyName
	req, err := http.NewRequestWithContext(ctx, "PUT", endpoint, bytes.NewReader([]byte(policyBody)))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to create ISM policy request: %v", err))
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Client.Stream(req)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to perform ISM policy request: %v", err))
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}
		log.Error(context.Background(), fmt.Sprintf("create ISM policy failed: %s", errResp.Error.Reason))
		return ErrCreateISMPolicy
	}

	return nil
}

// DeleteISMPolicy 删除ISM策略
func (c *Client) DeleteISMPolicy(ctx context.Context, policyName string) error {
	if c.Client == nil {
		return ErrRequestFailed
	}
	endpoint := "/_plugins/_ism/policies/" + policyName
	req, err := http.NewRequestWithContext(ctx, "DELETE", endpoint, nil)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to create ISM policy delete request: %v", err))
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Client.Stream(req)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to perform ISM policy delete request: %v", err))
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}
		log.Error(context.Background(), fmt.Sprintf("delete ISM policy failed: %s", errResp.Error.Reason))
		return ErrDeleteISMPolicy
	}

	return nil
}

// Search 查询数据，适用于简单查询场景，复杂查询建议使用 SearchWithHighlight 或 SearchBySQL
func (c *Client) Search(
	ctx context.Context,
	indexName string,
	req *paginationV1.PagingRequest,
) (*opensearchapiV4.SearchResp, error) {
	if req == nil {
		return nil, ErrInvalidRequest
	}

	qb := query.NewQueryBuilder()

	var err error

	// apply filters
	var filterExpr *paginationV1.FilterExpr
	filterExpr, err = paginationFilter.ConvertFilterByPagingRequest(req)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("convert filter string to filter expr failed: %s", err.Error()))
		return nil, ErrInvalidFilter
	}
	req.FilteringType = &paginationV1.PagingRequest_FilterExpr{FilterExpr: filterExpr}

	if _, err = c.structuredFilter.BuildSelectors(qb, req.GetFilterExpr()); err != nil {
		return nil, err
	}

	// select fields
	if req.FieldMask != nil && len(req.GetFieldMask().Paths) > 0 {
		if _, err = c.fieldSelector.BuildSelector(qb, req.GetFieldMask().GetPaths()); err != nil {
			log.Error(context.Background(), fmt.Sprintf("field selector build error: %v", err))
		}
	}

	// sorting
	if len(req.GetOrderBy()) > 0 {
		var sortings []*paginationV1.Sorting
		sortings, err = c.orderByStringConverter.Convert(req.GetOrderBy())
		if err != nil {
			log.Error(context.Background(), fmt.Sprintf("convert order by string to sorting failed: %s", err.Error()))
			return nil, err
		}
		_ = c.structuredSorting.BuildOrderClause(qb, sortings)
	} else if len(req.GetSorting()) > 0 {
		_ = c.structuredSorting.BuildOrderClause(qb, req.GetSorting())
	}

	// pagination
	if !req.GetNoPaging() {
		if req.Page != nil && req.PageSize != nil {
			_ = c.pagePaginator.BuildClause(qb, int(req.GetPage()), int(req.GetPageSize()))
		} else if req.Offset != nil && req.Limit != nil {
			_ = c.offsetPaginator.BuildClause(qb, int(req.GetOffset()), int(req.GetLimit()))
		} else if req.Token != nil && req.Offset != nil {
			_ = c.tokenPaginator.BuildClause(qb, req.GetToken(), int(req.GetOffset()))
		}
	} else if paginator.NoPagingMaxLimit > 0 {
		// no_paging 为客户端可设置字段，仍施加宽松的行数兜底，防止无界查询构成 DoS。
		_ = c.offsetPaginator.BuildClause(qb, 0, paginator.NoPagingMaxLimit)
	}

	body := qb.Build()
	buf := &bytes.Buffer{}
	if err = json.NewEncoder(buf).Encode(body); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to encode search body: %v", err))
		return nil, err
	}

	searchReq := &opensearchapiV4.SearchReq{
		Indices: []string{indexName},
		Body:    buf,
	}
	var searchResult opensearchapiV4.SearchResp
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPost, searchReq, &searchResult)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to search documents: %v", err))
		return nil, err
	}
	if resp.IsError() {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Error(context.Background(), fmt.Sprintf("search document failed [%d]: %s", resp.StatusCode, string(bodyBytes)))
		return nil, ErrSearchDocument
	}

	return &searchResult, nil
}

// SearchWithHighlight 查询数据，带高亮
func (c *Client) SearchWithHighlight(
	ctx context.Context,
	indexName string,
	query map[string]any,
	highlight map[string]any,
	sourceFields []string,
	sortBy map[string]bool,
	from, pageSize int,
) (*opensearchapiV4.SearchResp, error) {
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
		log.Error(context.Background(), fmt.Sprintf("failed to encode search body: %v", err))
		return nil, err
	}

	req := &opensearchapiV4.SearchReq{
		Indices: []string{indexName},
		Body:    buf,
	}
	var searchResult opensearchapiV4.SearchResp
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPost, req, &searchResult)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to search documents: %v", err))
		return nil, err
	}
	if resp.IsError() {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Error(context.Background(), fmt.Sprintf("search document failed [%d]: %s", resp.StatusCode, string(bodyBytes)))
		return nil, ErrSearchDocument
	}
	return &searchResult, nil
}

// SearchBySQL 通过 SQL 查询数据，适用于 OpenSearch SQL 插件
func (c *Client) SearchBySQL(ctx context.Context, sql string) (*SQLResult, error) {
	// OpenSearch SQL 固定接口：/_plugins/_sql
	reqBody := map[string]any{
		"query":  sql,
		"format": "json",
	}

	jsonBody, err := c.codec.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	// 兼容 _plugins/_sql 和 _opendistro/_sql
	endpoint := "/_plugins/_sql"
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Client.Stream(req)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("opensearch sql query failed: %v", err))
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		log.Error(context.Background(), fmt.Sprintf("opensearch sql error response: %s", string(body)))
		return nil, ErrSearchDocument
	}

	var result SQLResult
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Error(context.Background(), fmt.Sprintf("decode sql result error: %v", err))
		return nil, err
	}

	return &result, nil
}

// SearchBySQLTo 执行SQL查询，直接映射到结构体切片
// out 必须是切片指针，例如 &[]User{}
func (c *Client) SearchBySQLTo(ctx context.Context, sql string, out any) error {
	result, err := c.SearchBySQL(ctx, sql)
	if err != nil {
		return err
	}

	// 把 datarows + schema 转成 map 切片
	var rows []map[string]any
	for _, row := range result.Datarows {
		item := make(map[string]any)
		for i, f := range result.Schema {
			item[f.Name] = row[i]
		}
		rows = append(rows, item)
	}

	// 转成目标结构体
	data, _ := c.codec.Marshal(rows)
	return c.codec.Unmarshal(data, out)
}

// SQLToDSL 将 SQL 转换为 OpenSearch DSL 查询体
func (c *Client) SQLToDSL(ctx context.Context, sql string) (map[string]any, error) {
	reqBody := map[string]any{
		"query": sql,
	}
	jsonBody, _ := c.codec.Marshal(reqBody)

	// OpenSearch SQL 翻译接口
	url := c.options.Addresses[0] + "/_plugins/_sql/translate"
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Client.Stream(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var dsl map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&dsl); err != nil {
		return nil, err
	}

	return dsl, nil
}

// SearchBySQLWithHighlight 执行SQL查询 + 高亮字段
// sql: 查询语句
// highlightFields: 需要高亮的字段，例如 []string{"title", "content"}
func (c *Client) SearchBySQLWithHighlight(
	ctx context.Context,
	indexName string,
	sql string,
	highlightFields []string,
) (*opensearchapiV4.SearchResp, error) {
	// 1. SQL 转 DSL
	dsl, err := c.SQLToDSL(ctx, sql)
	if err != nil {
		return nil, err
	}

	// 2. 加入高亮配置
	if len(highlightFields) > 0 {
		fields := make(map[string]any)
		for _, f := range highlightFields {
			fields[f] = map[string]any{
				"pre_tags":  []string{"<em>"},
				"post_tags": []string{"</em>"},
			}
		}
		dsl["highlight"] = map[string]any{
			"fields": fields,
		}
	}

	// 3. 拼接真实搜索 URL
	url := fmt.Sprintf("%s/%s/_search", c.options.Addresses[0], indexName)
	dslBody, _ := c.codec.Marshal(dsl)

	// 4. 构造真实 HTTP 请求
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(dslBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	// 5. 执行（唯一真实存在的方法）
	resp, err := c.Client.Stream(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// 6. 解析成你现有的 SearchResult
	var result opensearchapiV4.SearchResp
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}

	return &result, nil
}

// RefreshIndex 刷新索引，确保写入后可立即查询
func (c *Client) RefreshIndex(ctx context.Context, indexName string) error {
	endpoint := "/" + indexName + "/_refresh"
	req, err := http.NewRequestWithContext(ctx, "POST", endpoint, nil)
	if err != nil {
		return err
	}
	resp, err := c.Client.Stream(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("refresh index failed: %s", resp.Status)
	}
	return nil
}

// QueryWithSQLPagination 通过SQL和分页参数查询数据
// req: 分页请求参数
func (c *Client) QueryWithSQLPagination(ctx context.Context, indexName string, req *paginationV1.PagingRequest) (*SQLResult, error) {
	if req == nil {
		return nil, ErrInvalidRequest
	}

	qb := query.NewQueryBuilder()

	var err error

	// apply filters
	var filterExpr *paginationV1.FilterExpr
	filterExpr, err = paginationFilter.ConvertFilterByPagingRequest(req)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("convert filter string to filter expr failed: %s", err.Error()))
		return nil, ErrInvalidFilter
	}
	req.FilteringType = &paginationV1.PagingRequest_FilterExpr{FilterExpr: filterExpr}

	if _, err = c.structuredFilter.BuildSelectors(qb, req.GetFilterExpr()); err != nil {
		return nil, err
	}

	// select fields
	if req.FieldMask != nil && len(req.GetFieldMask().Paths) > 0 {
		if _, err = c.fieldSelector.BuildSelector(qb, req.GetFieldMask().GetPaths()); err != nil {
			log.Error(context.Background(), fmt.Sprintf("field selector build error: %v", err))
		}
	}

	// sorting
	if len(req.GetOrderBy()) > 0 {
		var sortings []*paginationV1.Sorting
		sortings, err = c.orderByStringConverter.Convert(req.GetOrderBy())
		if err != nil {
			log.Error(context.Background(), fmt.Sprintf("convert order by string to sorting failed: %s", err.Error()))
			return nil, err
		}
		_ = c.structuredSorting.BuildOrderClause(qb, sortings)
	} else if len(req.GetSorting()) > 0 {
		_ = c.structuredSorting.BuildOrderClause(qb, req.GetSorting())
	}

	// pagination
	if !req.GetNoPaging() {
		if req.Page != nil && req.PageSize != nil {
			_ = c.pagePaginator.BuildClause(qb, int(req.GetPage()), int(req.GetPageSize()))
		} else if req.Offset != nil && req.Limit != nil {
			_ = c.offsetPaginator.BuildClause(qb, int(req.GetOffset()), int(req.GetLimit()))
		} else if req.Token != nil && req.Offset != nil {
			_ = c.tokenPaginator.BuildClause(qb, req.GetToken(), int(req.GetOffset()))
		}
	} else if paginator.NoPagingMaxLimit > 0 {
		// no_paging 为客户端可设置字段，仍施加宽松的行数兜底，防止无界查询构成 DoS。
		_ = c.offsetPaginator.BuildClause(qb, 0, paginator.NoPagingMaxLimit)
	}

	sql := qb.BuildSQL(indexName)

	return c.SearchBySQL(ctx, sql)
}

// QueryWithSQLPaginationTo 通过SQL和分页参数查询数据，并直接映射到结构体切片
func (c *Client) QueryWithSQLPaginationTo(ctx context.Context, indexName string, req *paginationV1.PagingRequest, out any) error {
	result, err := c.QueryWithSQLPagination(ctx, indexName, req)
	if err != nil {
		return err
	}

	// 把 datarows + schema 转成 map 切片
	var rows []map[string]any
	for _, row := range result.Datarows {
		item := make(map[string]any)
		for i, f := range result.Schema {
			item[f.Name] = row[i]
		}
		rows = append(rows, item)
	}

	// 转成目标结构体
	data, _ := c.codec.Marshal(rows)
	return c.codec.Unmarshal(data, out)
}

// CreateAlias 创建或更新一个别名（指向指定索引）。
// 注意：OpenSearch 的别名创建接口不接受 body（别名与索引在路径中指定），
// bodyJSON 参数仅为与 elasticsearch 客户端对称保留，在此实现中忽略。
func (c *Client) CreateAlias(ctx context.Context, alias, index, bodyJSON string) error {
	_ = bodyJSON
	req := opensearchapiV4.AliasPutReq{
		Indices: []string{index},
		Alias:   alias,
	}
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPut, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to create alias: %v", err))
		return err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}
		log.Error(context.Background(), fmt.Sprintf("create alias failed: %s", errResp.Error))
		return ErrCreateIndex
	}
	return nil
}

// DeleteAlias 删除一个别名。
func (c *Client) DeleteAlias(ctx context.Context, alias, index string) error {
	req := opensearchapiV4.AliasDeleteReq{
		Indices: []string{index},
		Alias:   []string{alias},
	}
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodDelete, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to delete alias: %v", err))
		return err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}
		log.Error(context.Background(), fmt.Sprintf("delete alias failed: %s", errResp.Error))
		return ErrDeleteIndex
	}
	return nil
}

// GetAlias 查询别名信息。
func (c *Client) GetAlias(ctx context.Context, alias string) (map[string]any, error) {
	req := opensearchapiV4.AliasGetReq{
		Alias: []string{alias},
	}
	var out map[string]any
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodGet, req, &out)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to get alias: %v", err))
		return nil, err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("get alias failed: %s", errResp.Error))
		return nil, ErrGetDocument
	}
	return out, nil
}

// ExistsAlias 判断别名是否存在。
func (c *Client) ExistsAlias(ctx context.Context, alias string) (bool, error) {
	req := opensearchapiV4.AliasExistsReq{
		Alias: []string{alias},
	}
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodHead, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to check alias existence: %v", err))
		return false, err
	}
	return !resp.IsError(), nil
}

// GetMapping 获取索引的 mapping。
func (c *Client) GetMapping(ctx context.Context, index string) (map[string]any, error) {
	req := opensearchapiV4.MappingGetReq{
		Indices: []string{index},
	}
	var out map[string]any
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodGet, req, &out)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to get mapping: %v", err))
		return nil, err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("get mapping failed: %s", errResp.Error))
		return nil, ErrGetDocument
	}
	return out, nil
}

// PutMapping 更新索引的 mapping。
func (c *Client) PutMapping(ctx context.Context, index, mappingJSON string) error {
	req := opensearchapiV4.MappingPutReq{
		Indices: []string{index},
		Body:    bytes.NewReader([]byte(mappingJSON)),
	}
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPut, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to put mapping: %v", err))
		return err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}
		log.Error(context.Background(), fmt.Sprintf("put mapping failed: %s", errResp.Error))
		return ErrCreateIndex
	}
	return nil
}

// GetSettings 获取索引的 settings。
func (c *Client) GetSettings(ctx context.Context, index string) (map[string]any, error) {
	req := opensearchapiV4.SettingsGetReq{
		Indices: []string{index},
	}
	var out map[string]any
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodGet, req, &out)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to get settings: %v", err))
		return nil, err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("get settings failed: %s", errResp.Error))
		return nil, ErrGetDocument
	}
	return out, nil
}

// PutSettings 更新索引的 settings。
func (c *Client) PutSettings(ctx context.Context, index, settingsJSON string) error {
	req := opensearchapiV4.SettingsPutReq{
		Indices: []string{index},
		Body:    bytes.NewReader([]byte(settingsJSON)),
	}
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPut, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to put settings: %v", err))
		return err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}
		log.Error(context.Background(), fmt.Sprintf("put settings failed: %s", errResp.Error))
		return ErrCreateIndex
	}
	return nil
}

// OpenIndex 打开一个索引。
func (c *Client) OpenIndex(ctx context.Context, index string) error {
	req := opensearchapiV4.IndicesOpenReq{
		Index: index,
	}
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPost, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to open index: %v", err))
		return err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}
		log.Error(context.Background(), fmt.Sprintf("open index failed: %s", errResp.Error))
		return ErrCreateIndex
	}
	return nil
}

// CloseIndex 关闭一个索引。
func (c *Client) CloseIndex(ctx context.Context, index string) error {
	req := opensearchapiV4.IndicesCloseReq{
		Index: index,
	}
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPost, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to close index: %v", err))
		return err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}
		log.Error(context.Background(), fmt.Sprintf("close index failed: %s", errResp.Error))
		return ErrCreateIndex
	}
	return nil
}

// FlushIndex 刷新一个索引的内部缓冲区到磁盘。
func (c *Client) FlushIndex(ctx context.Context, index string) error {
	req := opensearchapiV4.IndicesFlushReq{
		Indices: []string{index},
	}
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPost, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to flush index: %v", err))
		return err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}
		log.Error(context.Background(), fmt.Sprintf("flush index failed: %s", errResp.Error))
		return ErrCreateIndex
	}
	return nil
}

// SearchWithBody 以任意 DSL body（例如聚合 aggs）执行搜索，返回库 SearchResp。
func (c *Client) SearchWithBody(ctx context.Context, index string, body map[string]any) (*opensearchapiV4.SearchResp, error) {
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(body); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to encode search body: %v", err))
		return nil, err
	}
	searchReq := &opensearchapiV4.SearchReq{
		Indices: []string{index},
		Body:    buf,
	}
	var searchResult opensearchapiV4.SearchResp
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPost, searchReq, &searchResult)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to search documents: %v", err))
		return nil, err
	}
	if resp.IsError() {
		bodyBytes, _ := io.ReadAll(resp.Body)
		log.Error(context.Background(), fmt.Sprintf("search document failed [%d]: %s", resp.StatusCode, string(bodyBytes)))
		return nil, ErrSearchDocument
	}
	return &searchResult, nil
}

// Count 返回匹配查询的文档数量。body 为查询体（DSL，可空）。
func (c *Client) Count(ctx context.Context, index string, body map[string]any) (int64, error) {
	if index == "" {
		return 0, ErrInvalidRequest
	}
	var req opensearchapiV4.IndicesCountReq
	req.Indices = []string{index}
	if body != nil {
		buf := &bytes.Buffer{}
		if err := json.NewEncoder(buf).Encode(body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to encode count body: %v", err))
			return 0, err
		}
		req.Body = bytes.NewReader(buf.Bytes())
	}
	var countResp opensearchapiV4.IndicesCountResp
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPost, req, &countResp)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to count: %v", err))
		return 0, err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return 0, err
		}
		log.Error(context.Background(), fmt.Sprintf("count failed: %s", errResp.Error))
		return 0, ErrSearchDocument
	}
	return int64(countResp.Count), nil
}

// SearchScroll 按已有的 scroll id 拉取下一批结果。返回库 SearchResp。
func (c *Client) SearchScroll(ctx context.Context, scrollID, keepAlive string) (*opensearchapiV4.SearchResp, error) {
	if scrollID == "" {
		return nil, ErrInvalidRequest
	}
	req := opensearchapiV4.ScrollGetReq{
		ScrollID: scrollID,
	}
	_ = keepAlive
	var searchResult opensearchapiV4.SearchResp
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPost, req, &searchResult)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to scroll: %v", err))
		return nil, err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("scroll failed: %s", errResp.Error))
		return nil, ErrSearchDocument
	}
	return &searchResult, nil
}

// ClearScroll 清理一个或多个 scroll 上下文。
func (c *Client) ClearScroll(ctx context.Context, scrollID string) error {
	if scrollID == "" {
		return ErrInvalidRequest
	}
	req := opensearchapiV4.ScrollDeleteReq{
		ScrollIDs: []string{scrollID},
	}
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodDelete, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to clear scroll: %v", err))
		return err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}
		log.Error(context.Background(), fmt.Sprintf("clear scroll failed: %s", errResp.Error))
		return ErrSearchDocument
	}
	return nil
}

// ClusterHealth 返回集群健康信息。
func (c *Client) ClusterHealth(ctx context.Context) (map[string]any, error) {
	req := opensearchapiV4.ClusterHealthReq{}
	var out map[string]any
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodGet, req, &out)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to get cluster health: %v", err))
		return nil, err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("cluster health failed: %s", errResp.Error))
		return nil, ErrRequestFailed
	}
	return out, nil
}

// ClusterInfo 返回集群信息（版本等）。
func (c *Client) ClusterInfo(ctx context.Context) (map[string]any, error) {
	req := opensearchapiV4.InfoReq{}
	var out map[string]any
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodGet, req, &out)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to get cluster info: %v", err))
		return nil, err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("cluster info failed: %s", errResp.Error))
		return nil, ErrRequestFailed
	}
	return out, nil
}

// GetISMPolicy 查询一个 ISM 策略。沿用现有 CreateISMPolicy/DeleteISMPolicy 的 raw Stream 模式。
func (c *Client) GetISMPolicy(ctx context.Context, policyName string) (map[string]any, error) {
	if c.Client == nil {
		return nil, ErrRequestFailed
	}
	endpoint := "/_plugins/_ism/policies/" + policyName
	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to create ISM policy get request: %v", err))
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Client.Stream(req)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to perform ISM policy get request: %v", err))
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("get ISM policy failed: %s", errResp.Error.Reason))
		return nil, ErrRequestFailed
	}

	var out map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to decode ISM policy response: %v", err))
		return nil, err
	}
	return out, nil
}

// CreateSnapshot 在指定仓库创建一个快照。bodyJSON 为快照体（可空）。
func (c *Client) CreateSnapshot(ctx context.Context, repository, snapshot, bodyJSON string) error {
	var req opensearchapiV4.SnapshotCreateReq
	req.Repo = repository
	req.Snapshot = snapshot
	if bodyJSON != "" {
		req.Body = bytes.NewReader([]byte(bodyJSON))
	}
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPut, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to create snapshot: %v", err))
		return err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}
		log.Error(context.Background(), fmt.Sprintf("create snapshot failed: %s", errResp.Error))
		return ErrRequestFailed
	}
	return nil
}

// GetSnapshot 查询一个快照的信息。
func (c *Client) GetSnapshot(ctx context.Context, repository, snapshot string) (map[string]any, error) {
	req := opensearchapiV4.SnapshotGetReq{
		Repo:      repository,
		Snapshots: []string{snapshot},
	}
	var out map[string]any
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodGet, req, &out)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to get snapshot: %v", err))
		return nil, err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("get snapshot failed: %s", errResp.Error))
		return nil, ErrRequestFailed
	}
	return out, nil
}

// DeleteSnapshot 删除一个快照。
func (c *Client) DeleteSnapshot(ctx context.Context, repository, snapshot string) error {
	req := opensearchapiV4.SnapshotDeleteReq{
		Repo:      repository,
		Snapshots: []string{snapshot},
	}
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodDelete, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to delete snapshot: %v", err))
		return err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}
		log.Error(context.Background(), fmt.Sprintf("delete snapshot failed: %s", errResp.Error))
		return ErrRequestFailed
	}
	return nil
}

// CreateSnapshotRepository 创建或更新一个快照仓库。bodyJSON 为仓库配置体。
func (c *Client) CreateSnapshotRepository(ctx context.Context, repository, bodyJSON string) error {
	req := opensearchapiV4.SnapshotRepositoryCreateReq{
		Repo: repository,
		Body: bytes.NewReader([]byte(bodyJSON)),
	}
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodPut, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to create snapshot repository: %v", err))
		return err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}
		log.Error(context.Background(), fmt.Sprintf("create snapshot repository failed: %s", errResp.Error))
		return ErrRequestFailed
	}
	return nil
}

// DeleteSnapshotRepository 删除一个快照仓库。
func (c *Client) DeleteSnapshotRepository(ctx context.Context, repository string) error {
	req := opensearchapiV4.SnapshotRepositoryDeleteReq{
		Repos: []string{repository},
	}
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodDelete, req, (*opensearchV4.NoBody)(nil))
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to delete snapshot repository: %v", err))
		return err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}
		log.Error(context.Background(), fmt.Sprintf("delete snapshot repository failed: %s", errResp.Error))
		return ErrRequestFailed
	}
	return nil
}

// GetSnapshotRepository 查询快照仓库信息。
func (c *Client) GetSnapshotRepository(ctx context.Context, repository string) (map[string]any, error) {
	req := opensearchapiV4.SnapshotRepositoryGetReq{
		Repos: []string{repository},
	}
	var out map[string]any
	resp, err := opensearchV4.Do(ctx, c.Client, http.MethodGet, req, &out)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to get snapshot repository: %v", err))
		return nil, err
	}
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("get snapshot repository failed: %s", errResp.Error))
		return nil, ErrRequestFailed
	}
	return out, nil
}
