package opensearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/go-kratos/kratos/v2/log"

	opensearchV4 "github.com/opensearch-project/opensearch-go/v4"
	opensearchapiV4 "github.com/opensearch-project/opensearch-go/v4/opensearchapi"

	"github.com/tx7do/go-wind-plugins/encoding"
	_ "github.com/tx7do/go-wind-plugins/encoding/json"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
	paginationFilter "github.com/tx7do/go-crud/pagination/filter"
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

	log   *log.Helper
	codec encoding.Codec

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
		log:     log.NewHelper(log.DefaultLogger),
		codec:   encoding.GetCodec("json"),

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
		metaBytes, err := c.codec.Marshal(meta)
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

// UpdateDocument 更新一条数据
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

// CreateIndexTemplate 创建或更新索引模板
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
		return ErrCreateTemplate
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

// DeleteIndexTemplate 删除索引模板
func (c *Client) DeleteIndexTemplate(ctx context.Context, templateName string) error {
	req := opensearchapiV4.IndexTemplateDeleteReq{
		IndexTemplate: templateName,
	}

	resp, err := c.Client.Do(ctx, req, nil)
	if err != nil {
		c.log.Errorf("failed to delete index template: %v", err)
		return err
	}

	if resp.IsError() {
		var errResp map[string]any
		if err = json.NewDecoder(resp.Body).Decode(&errResp); err == nil {
			c.log.Errorf("delete index template failed: %v", errResp)
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

	resp, err := c.Client.Do(ctx, req, nil)
	if err != nil {
		c.log.Errorf("failed to create component template: %v", err)
		return err
	}

	if resp.IsError() {
		var errResp map[string]any
		if err = json.NewDecoder(resp.Body).Decode(&errResp); err == nil {
			c.log.Errorf("create component template failed: %v", errResp)
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

	resp, err := c.Client.Do(ctx, req, nil)
	if err != nil {
		c.log.Errorf("failed to delete component template: %v", err)
		return err
	}

	if resp.IsError() {
		var errResp map[string]any
		if err = json.NewDecoder(resp.Body).Decode(&errResp); err == nil {
			c.log.Errorf("delete component template failed: %v", errResp)
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

	resp, err := c.Client.Do(ctx, req, nil)
	if err != nil {
		c.log.Errorf("failed to check if component template exists: %v", err)
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
		c.log.Errorf("failed to create ISM policy request: %v", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Client.Perform(req)
	if err != nil {
		c.log.Errorf("failed to perform ISM policy request: %v", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			c.log.Errorf("failed to parse error message: %v", err)
			return err
		}
		c.log.Errorf("create ISM policy failed: %s", errResp.Error.Reason)
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
		c.log.Errorf("failed to create ISM policy delete request: %v", err)
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Client.Perform(req)
	if err != nil {
		c.log.Errorf("failed to perform ISM policy delete request: %v", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			c.log.Errorf("failed to parse error message: %v", err)
			return err
		}
		c.log.Errorf("delete ISM policy failed: %s", errResp.Error.Reason)
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
		log.Errorf("convert filter string to filter expr failed: %s", err.Error())
		return nil, ErrInvalidFilter
	}
	req.FilteringType = &paginationV1.PagingRequest_FilterExpr{FilterExpr: filterExpr}

	if _, err = c.structuredFilter.BuildSelectors(qb, req.GetFilterExpr()); err != nil {
		return nil, err
	}

	// select fields
	if req.FieldMask != nil && len(req.GetFieldMask().Paths) > 0 {
		if _, err = c.fieldSelector.BuildSelector(qb, req.GetFieldMask().GetPaths()); err != nil {
			c.log.Errorf("field selector build error: %v", err)
		}
	}

	// sorting
	if len(req.GetOrderBy()) > 0 {
		var sortings []*paginationV1.Sorting
		sortings, err = c.orderByStringConverter.Convert(req.GetOrderBy())
		if err != nil {
			log.Errorf("convert order by string to sorting failed: %s", err.Error())
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
	}

	body := qb.Build()
	buf := &bytes.Buffer{}
	if err = json.NewEncoder(buf).Encode(body); err != nil {
		c.log.Errorf("failed to encode search body: %v", err)
		return nil, err
	}

	searchReq := &opensearchapiV4.SearchReq{
		Indices: []string{indexName},
		Body:    buf,
	}
	var searchResult opensearchapiV4.SearchResp
	resp, err := c.Client.Do(ctx, searchReq, &searchResult)
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
		c.log.Errorf("failed to encode search body: %v", err)
		return nil, err
	}

	req := &opensearchapiV4.SearchReq{
		Indices: []string{indexName},
		Body:    buf,
	}
	var searchResult opensearchapiV4.SearchResp
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

	resp, err := c.Client.Perform(req)
	if err != nil {
		c.log.Errorf("opensearch sql query failed: %v", err)
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		c.log.Errorf("opensearch sql error response: %s", string(body))
		return nil, ErrSearchDocument
	}

	var result SQLResult
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		c.log.Errorf("decode sql result error: %v", err)
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

	resp, err := c.Client.Perform(req)
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
	resp, err := c.Client.Perform(req)
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
	resp, err := c.Client.Perform(req)
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
		log.Errorf("convert filter string to filter expr failed: %s", err.Error())
		return nil, ErrInvalidFilter
	}
	req.FilteringType = &paginationV1.PagingRequest_FilterExpr{FilterExpr: filterExpr}

	if _, err = c.structuredFilter.BuildSelectors(qb, req.GetFilterExpr()); err != nil {
		return nil, err
	}

	// select fields
	if req.FieldMask != nil && len(req.GetFieldMask().Paths) > 0 {
		if _, err = c.fieldSelector.BuildSelector(qb, req.GetFieldMask().GetPaths()); err != nil {
			c.log.Errorf("field selector build error: %v", err)
		}
	}

	// sorting
	if len(req.GetOrderBy()) > 0 {
		var sortings []*paginationV1.Sorting
		sortings, err = c.orderByStringConverter.Convert(req.GetOrderBy())
		if err != nil {
			log.Errorf("convert order by string to sorting failed: %s", err.Error())
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
