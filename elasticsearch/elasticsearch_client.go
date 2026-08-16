package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/tx7do/go-wind/log"

	elasticsearchV9 "github.com/elastic/go-elasticsearch/v9"
	esapiV9 "github.com/elastic/go-elasticsearch/v9/esapi"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

type Client struct {
	*elasticsearchV9.Client
	esOpts []elasticsearchV9.Option

	// pendingBasicAuth buffers username/password set via WithUsername /
	// WithPassword until both are present, since WithBasicAuth requires them
	// together regardless of call order.
	pendingBasicAuth struct {
		username, password string
		haveUser, havePass bool
	}
}

func NewElasticsearchClient(opts ...Option) (*Client, error) {
	c := &Client{}

	for _, o := range opts {
		o(c)
	}

	// If both credentials were supplied, emit the single WithBasicAuth option
	// the transport expects.
	if c.pendingBasicAuth.haveUser && c.pendingBasicAuth.havePass {
		c.esOpts = append(c.esOpts, elasticsearchV9.WithBasicAuth(
			c.pendingBasicAuth.username, c.pendingBasicAuth.password))
	}
	c.pendingBasicAuth = struct {
		username, password string
		haveUser, havePass bool
	}{}

	cli, err := elasticsearchV9.New(c.esOpts...)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to create elasticsearch client: %v", err))
		return nil, err
	}

	c.Client = cli
	c.esOpts = nil

	return c, nil
}

// CheckConnectStatus 检查Elasticsearch连接
func (c *Client) CheckConnectStatus() bool {
	if c.Client == nil {
		return false
	}

	resp, err := c.Client.Info()
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to connect to elasticsearch: %v", err))
		return false
	}
	defer func(Body io.ReadCloser) {
		if err = Body.Close(); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to close response body: %v", err))
		}
	}(resp.Body)

	if resp.IsError() {
		log.Error(context.Background(), fmt.Sprintf("Error: %s", resp.String()))
		return false
	}

	var r map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&r); err != nil {
		log.Error(context.Background(), fmt.Sprintf("Error parsing the response body: %s", err))
		return false
	}

	log.Info(context.Background(), fmt.Sprintf("Client Version: %s", elasticsearchV9.Version))
	log.Info(context.Background(), fmt.Sprintf("Server Version: %s", r["version"].(map[string]any)["number"]))

	return true
}

// IndexExists 检查索引是否存在
func (c *Client) IndexExists(ctx context.Context, indexName string) (bool, error) {
	resp, err := c.Client.Indices.Exists(
		[]string{indexName},
		c.Client.Indices.Exists.WithContext(ctx),
	)
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

	resp, err := c.Client.Indices.Create(
		indexName,
		c.Client.Indices.Create.WithContext(ctx),
		c.Client.Indices.Create.WithBody(bytes.NewReader([]byte(body))),
	)
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

	resp, err := c.Client.Indices.Delete(
		[]string{indexName},
		c.Client.Indices.Delete.WithContext(ctx),
	)
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
	_, err := c.Client.Delete(
		indexName, id,
		c.Client.Delete.WithContext(ctx),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to delete document: %v", err))
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
		log.Error(context.Background(), fmt.Sprintf("failed to marshal data: %v", err))
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
		log.Error(context.Background(), fmt.Sprintf("failed to call Index API: %v", err))
		return err
	}
	defer resp.Body.Close()

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
	if len(ids) > 0 && len(ids) != len(dataSet) {
		return fmt.Errorf("ids length (%d) must match dataSet length (%d) or be empty", len(ids), len(dataSet))
	}

	// 批次大小限制：避免 OOM（建议 500~2000 条/批）
	const maxBatchSize = 1000
	if len(dataSet) > maxBatchSize {
		log.Warn(context.Background(), fmt.Sprintf("batch size %d exceeds limit %d, splitting into chunks", len(dataSet), maxBatchSize))
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
		var meta map[string]any
		if len(ids) > 0 && i < len(ids) && ids[i] != "" {
			meta = map[string]any{
				"index": map[string]any{
					"_id": ids[i],
				},
			}
		} else {
			meta = map[string]any{
				"index": map[string]any{}, // 无 ID 则让 ES 自动生成（不推荐）
			}
		}

		metaBytes, err := json.Marshal(meta)
		if err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to marshal meta for item %d: %v", i, err))
			failedCount++
			continue
		}
		metaBytes = append(metaBytes, '\n')

		dataBytes, err := json.Marshal(data)
		if err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to marshal data for item %d: %v", i, err))
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
		log.Error(context.Background(), fmt.Sprintf("failed to call Bulk API: %v", err))
		return err
	}

	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Warn(context.Background(), fmt.Sprintf("failed to close bulk response body: %v", closeErr))
		}
	}()

	if resp.IsError() {
		// 读取 Body（只能读一次）
		bodyBytes, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			log.Error(context.Background(), fmt.Sprintf("bulk API error [%d], failed to read body: %v", resp.StatusCode, readErr))
		} else {
			log.Error(context.Background(), fmt.Sprintf("bulk API error [%d]: %s", resp.StatusCode, string(bodyBytes)))
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
		log.Error(context.Background(), fmt.Sprintf("failed to decode bulk response: %v", err))
		return err
	}

	// 处理部分失败
	if bulkResp.Errors {
		var failedIDs []string
		for i, item := range bulkResp.Items {
			for _, result := range item {
				if result.Status >= 400 {
					failedIDs = append(failedIDs, result.ID)
					log.Warn(context.Background(), fmt.Sprintf("bulk item %d (id=%s) failed [%d]: %s - %s",
						i, result.ID, result.Status, result.Error.Type, result.Error.Reason))
				}
			}
		}
		log.Warn(context.Background(), fmt.Sprintf("bulk insert completed with %d/%d failures", len(failedIDs), len(dataSet)))
		return &PartialFailureError{
			Total:     len(dataSet),
			Failed:    len(failedIDs),
			FailedIDs: failedIDs,
		}
	}

	log.Debug(context.Background(), fmt.Sprintf("bulk insert succeeded: %d documents to %s", len(dataSet), indexName))
	return nil
}

// MultiGet 批量获取文档。返回每个 id 对应文档的 _source 原始 JSON（json.RawMessage）；
// 若该文档未找到或请求被拒绝，对应位置为 nil。sourceFields 为允许返回的字段白名单（可空）。
func (c *Client) MultiGet(ctx context.Context, index string, ids []string, sourceFields []string) ([]json.RawMessage, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	// 构造 mget 请求体：{"docs":[{"_index":index,"_id":id}, ...]}
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

	opts := []func(*esapiV9.MgetRequest){
		c.Client.Mget.WithContext(ctx),
		c.Client.Mget.WithIndex(index),
	}
	if len(sourceFields) > 0 {
		opts = append(opts, c.Client.Mget.WithSource(sourceFields...))
	}
	resp, err := c.Client.Mget(bytes.NewReader(bodyBytes), opts...)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to call mget: %v", err))
		return nil, err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("mget failed: %s", errResp.Error))
		return nil, ErrGetDocument
	}

	var parsed struct {
		Docs []struct {
			Found  bool            `json:"found"`
			Source json.RawMessage `json:"_source"`
		} `json:"docs"`
	}
	if err = json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to decode mget response: %v", err))
		return nil, err
	}
	results := make([]json.RawMessage, len(ids))
	// parsed.Docs 顺序与请求中的 docs 顺序一致；但跳过的空 id 不在请求中，
	// 因此按请求文档顺序回填，对齐到 ids 列表（空 id 位置保持 nil）。
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

// BatchUpdateDocument 批量更新文档。dataSet 与 ids 等长（或 ids 为空表示全部由 ES 自动匹配）。
// 采用 bulk update action（NDJSON），失败按 PartialFailureError 上报。空输入短路返回 nil。
func (c *Client) BatchUpdateDocument(ctx context.Context, indexName string, dataSet []any, ids []string) error {
	if len(dataSet) == 0 {
		return nil
	}
	if len(ids) > 0 && len(ids) != len(dataSet) {
		return fmt.Errorf("ids length (%d) must match dataSet length (%d) or be empty", len(ids), len(dataSet))
	}

	const maxBatchSize = 1000
	if len(dataSet) > maxBatchSize {
		log.Warn(context.Background(), fmt.Sprintf("batch size %d exceeds limit %d, splitting into chunks", len(dataSet), maxBatchSize))
		for i := 0; i < len(dataSet); i += maxBatchSize {
			end := i + maxBatchSize
			if end > len(dataSet) {
				end = len(dataSet)
			}
			var subIDs []string
			if len(ids) > 0 {
				subIDs = ids[i:end]
			}
			if err := c.BatchUpdateDocument(ctx, indexName, dataSet[i:end], subIDs); err != nil {
				return err
			}
		}
		return nil
	}

	var buf bytes.Buffer
	failedCount := 0
	provided := 0
	for i, data := range dataSet {
		var id string
		if len(ids) > 0 && i < len(ids) && ids[i] != "" {
			id = ids[i]
		}
		if id == "" {
			// update 必须指定 _id，否则跳过。
			log.Error(context.Background(), fmt.Sprintf("batch update item %d missing _id, skipped", i))
			failedCount++
			continue
		}
		provided++
		// update action meta
		meta := map[string]any{
			"update": map[string]any{
				"_id":    id,
				"_index": indexName,
			},
		}
		metaBytes, err := json.Marshal(meta)
		if err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to marshal update meta for item %d: %v", i, err))
			failedCount++
			continue
		}
		metaBytes = append(metaBytes, '\n')
		// ES bulk update body: {"doc": <partial>}
		docBytes, err := json.Marshal(map[string]any{"doc": data})
		if err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to marshal update data for item %d: %v", i, err))
			failedCount++
			continue
		}
		docBytes = append(docBytes, '\n')
		buf.Grow(len(metaBytes) + len(docBytes))
		buf.Write(metaBytes)
		buf.Write(docBytes)
	}

	if buf.Len() == 0 {
		return fmt.Errorf("no valid documents to update, %d items failed to marshal or missing id", failedCount)
	}
	_ = provided

	resp, err := c.Client.Bulk(
		bytes.NewReader(buf.Bytes()),
		c.Client.Bulk.WithContext(ctx),
		c.Client.Bulk.WithIndex(indexName),
		c.Client.Bulk.WithRefresh("wait_for"),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to call Bulk API (update): %v", err))
		return err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Warn(context.Background(), fmt.Sprintf("failed to close bulk response body: %v", closeErr))
		}
	}()

	if resp.IsError() {
		bodyBytes, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			log.Error(context.Background(), fmt.Sprintf("bulk update API error [%d], failed to read body: %v", resp.StatusCode, readErr))
		} else {
			log.Error(context.Background(), fmt.Sprintf("bulk update API error [%d]: %s", resp.StatusCode, string(bodyBytes)))
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
	if err := json.NewDecoder(resp.Body).Decode(&bulkResp); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to decode bulk response: %v", err))
		return err
	}
	if bulkResp.Errors {
		var failedIDs []string
		for i, item := range bulkResp.Items {
			for _, result := range item {
				if result.Status >= 400 {
					failedIDs = append(failedIDs, result.ID)
					log.Warn(context.Background(), fmt.Sprintf("bulk update item %d (id=%s) failed [%d]: %s - %s",
						i, result.ID, result.Status, result.Error.Type, result.Error.Reason))
				}
			}
		}
		log.Warn(context.Background(), fmt.Sprintf("bulk update completed with %d/%d failures", len(failedIDs), len(dataSet)))
		return &PartialFailureError{
			Total:     len(dataSet),
			Failed:    len(failedIDs),
			FailedIDs: failedIDs,
		}
	}

	log.Debug(context.Background(), fmt.Sprintf("bulk update succeeded: %d documents in %s", len(dataSet), indexName))
	return nil
}

// BatchDeleteDocument 批量删除文档。空 ids 短路返回 nil。
func (c *Client) BatchDeleteDocument(ctx context.Context, indexName string, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	const maxBatchSize = 1000
	if len(ids) > maxBatchSize {
		log.Warn(context.Background(), fmt.Sprintf("batch size %d exceeds limit %d, splitting into chunks", len(ids), maxBatchSize))
		for i := 0; i < len(ids); i += maxBatchSize {
			end := i + maxBatchSize
			if end > len(ids) {
				end = len(ids)
			}
			if err := c.BatchDeleteDocument(ctx, indexName, ids[i:end]); err != nil {
				return err
			}
		}
		return nil
	}

	var buf bytes.Buffer
	skipped := 0
	for _, id := range ids {
		if id == "" {
			skipped++
			continue
		}
		meta := map[string]any{
			"delete": map[string]any{
				"_id":    id,
				"_index": indexName,
			},
		}
		metaBytes, err := json.Marshal(meta)
		if err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to marshal delete meta for id %s: %v", id, err))
			continue
		}
		metaBytes = append(metaBytes, '\n')
		buf.Grow(len(metaBytes))
		buf.Write(metaBytes)
	}
	_ = skipped
	if buf.Len() == 0 {
		return fmt.Errorf("no valid ids to delete")
	}

	resp, err := c.Client.Bulk(
		bytes.NewReader(buf.Bytes()),
		c.Client.Bulk.WithContext(ctx),
		c.Client.Bulk.WithIndex(indexName),
		c.Client.Bulk.WithRefresh("wait_for"),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to call Bulk API (delete): %v", err))
		return err
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			log.Warn(context.Background(), fmt.Sprintf("failed to close bulk response body: %v", closeErr))
		}
	}()

	if resp.IsError() {
		bodyBytes, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			log.Error(context.Background(), fmt.Sprintf("bulk delete API error [%d], failed to read body: %v", resp.StatusCode, readErr))
		} else {
			log.Error(context.Background(), fmt.Sprintf("bulk delete API error [%d]: %s", resp.StatusCode, string(bodyBytes)))
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
	if err := json.NewDecoder(resp.Body).Decode(&bulkResp); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to decode bulk response: %v", err))
		return err
	}
	if bulkResp.Errors {
		var failedIDs []string
		for i, item := range bulkResp.Items {
			for _, result := range item {
				if result.Status >= 400 {
					failedIDs = append(failedIDs, result.ID)
					log.Warn(context.Background(), fmt.Sprintf("bulk delete item %d (id=%s) failed [%d]: %s - %s",
						i, result.ID, result.Status, result.Error.Type, result.Error.Reason))
				}
			}
		}
		log.Warn(context.Background(), fmt.Sprintf("bulk delete completed with %d/%d failures", len(failedIDs), len(ids)))
		return &PartialFailureError{
			Total:     len(ids),
			Failed:    len(failedIDs),
			FailedIDs: failedIDs,
		}
	}

	log.Debug(context.Background(), fmt.Sprintf("bulk delete succeeded: %d documents in %s", len(ids), indexName))
	return nil
}

// UpdateByQuery 按查询更新文档。bodyJSON 为查询体（DSL）。
func (c *Client) UpdateByQuery(ctx context.Context, index, bodyJSON string) error {
	resp, err := c.Client.UpdateByQuery(
		[]string{index},
		c.Client.UpdateByQuery.WithContext(ctx),
		c.Client.UpdateByQuery.WithBody(strings.NewReader(bodyJSON)),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to update by query: %v", err))
		return err
	}
	defer resp.Body.Close()
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
	resp, err := c.Client.DeleteByQuery(
		[]string{index}, strings.NewReader(bodyJSON),
		c.Client.DeleteByQuery.WithContext(ctx),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to delete by query: %v", err))
		return err
	}
	defer resp.Body.Close()
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

func (c *Client) UpdateDocument(ctx context.Context, indexName string, pk string, doc any) error {
	data, err := json.Marshal(doc)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to marshal data: %v", err))
		return err
	}

	_, err = c.Client.Update(
		indexName, pk,
		bytes.NewReader(data),
		c.Client.Update.WithContext(ctx),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to update document: %v", err))
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

	if err = json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to decode document: %v", err))
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
		log.Error(context.Background(), fmt.Sprintf("failed to search documents: %v", err))
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		if err = Body.Close(); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to close response body: %v", err))
		}
	}(resp.Body)

	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			return nil, err
		}

		log.Error(context.Background(), fmt.Sprintf("search document failed: %s", errResp.Error.Reason))

		return nil, ErrSearchDocument
	}

	var searchResult SearchResult
	if err = json.NewDecoder(resp.Body).Decode(&searchResult); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to decode search result: %v", err))
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
		log.Error(context.Background(), fmt.Sprintf("failed to create index template: %v", err))
		return err
	}
	defer func(Body io.ReadCloser) {
		if err = Body.Close(); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to close response body: %v", err))
		}
	}(resp.Body)

	if resp.IsError() {
		var errResp map[string]any
		if err = json.NewDecoder(resp.Body).Decode(&errResp); err == nil {
			log.Error(context.Background(), fmt.Sprintf("create index template failed: %v", errResp))
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
		log.Error(context.Background(), fmt.Sprintf("failed to check if index template exists: %v", err))
		return false, err
	}
	defer func(Body io.ReadCloser) {
		if err = Body.Close(); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to close response body: %v", err))
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
		log.Error(context.Background(), fmt.Sprintf("failed to create ILM policy: %v", err))
		return err
	}
	defer func(Body io.ReadCloser) {
		closeErr := Body.Close()
		if closeErr != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to close response body: %v", closeErr))
		}
	}(resp.Body)

	if resp.IsError() {
		var errResp map[string]any
		_ = json.NewDecoder(resp.Body).Decode(&errResp)
		log.Error(context.Background(), fmt.Sprintf("create ILM policy failed: status=%d, resp=%v", resp.StatusCode, errResp))
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
		log.Error(context.Background(), fmt.Sprintf("failed to delete ILM policy: %v", err))
		return err
	}
	defer func(Body io.ReadCloser) {
		if err = Body.Close(); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to close response body: %v", err))
		}
	}(resp.Body)

	if resp.IsError() {
		var errResp map[string]any
		if err = json.NewDecoder(resp.Body).Decode(&errResp); err == nil {
			log.Error(context.Background(), fmt.Sprintf("delete ILM policy failed: %v", errResp))
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
		log.Error(context.Background(), fmt.Sprintf("failed to encode search body: %v", err))
		return nil, err
	}

	resp, err := c.Client.Search(
		c.Client.Search.WithContext(ctx),
		c.Client.Search.WithIndex(indexName),
		c.Client.Search.WithBody(buf),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to search documents: %v", err))
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		if err := Body.Close(); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to close response body: %v", err))
		}
	}(resp.Body)

	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("search document failed: %s", errResp.Error.Reason))
		return nil, ErrSearchDocument
	}

	var searchResult SearchResult
	if err = json.NewDecoder(resp.Body).Decode(&searchResult); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to decode search result: %v", err))
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
		log.Error(context.Background(), fmt.Sprintf("sql query error: %v", err))
		return nil, err
	}
	defer resp.Body.Close()

	if resp.IsError() {
		body, _ := io.ReadAll(resp.Body)
		log.Error(context.Background(), fmt.Sprintf("sql query failed: %s", string(body)))
		return nil, ErrSearchDocument
	}

	// 解析 SQL 专用结构
	var result SQLResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		log.Error(context.Background(), fmt.Sprintf("decode sql result error: %v", err))
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
		log.Error(context.Background(), fmt.Sprintf("sql error: %s", string(body)))
		return ErrSearchDocument
	}

	return json.NewDecoder(resp.Body).Decode(out)
}

// CreateAlias 创建或更新一个别名（指向指定索引）。
func (c *Client) CreateAlias(ctx context.Context, alias, index, bodyJSON string) error {
	resp, err := c.Client.Indices.PutAlias(
		[]string{index}, alias,
		c.Client.Indices.PutAlias.WithContext(ctx),
		c.Client.Indices.PutAlias.WithBody(strings.NewReader(bodyJSON)),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to create alias: %v", err))
		return err
	}
	defer resp.Body.Close()
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
	resp, err := c.Client.Indices.DeleteAlias(
		[]string{index}, []string{alias},
		c.Client.Indices.DeleteAlias.WithContext(ctx),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to delete alias: %v", err))
		return err
	}
	defer resp.Body.Close()
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
	resp, err := c.Client.Indices.GetAlias(
		c.Client.Indices.GetAlias.WithContext(ctx),
		c.Client.Indices.GetAlias.WithName(alias),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to get alias: %v", err))
		return nil, err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("get alias failed: %s", errResp.Error))
		return nil, ErrGetDocument
	}
	var out map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to decode alias response: %v", err))
		return nil, err
	}
	return out, nil
}

// ExistsAlias 判断别名是否存在。
func (c *Client) ExistsAlias(ctx context.Context, alias string) (bool, error) {
	resp, err := c.Client.Indices.ExistsAlias(
		[]string{alias},
		c.Client.Indices.ExistsAlias.WithContext(ctx),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to check alias existence: %v", err))
		return false, err
	}
	defer resp.Body.Close()
	return !resp.IsError(), nil
}

// GetMapping 获取索引的 mapping。
func (c *Client) GetMapping(ctx context.Context, index string) (map[string]any, error) {
	resp, err := c.Client.Indices.GetMapping(
		c.Client.Indices.GetMapping.WithContext(ctx),
		c.Client.Indices.GetMapping.WithIndex(index),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to get mapping: %v", err))
		return nil, err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("get mapping failed: %s", errResp.Error))
		return nil, ErrGetDocument
	}
	var out map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to decode mapping response: %v", err))
		return nil, err
	}
	return out, nil
}

// PutMapping 更新索引的 mapping。
func (c *Client) PutMapping(ctx context.Context, index, mappingJSON string) error {
	resp, err := c.Client.Indices.PutMapping(
		[]string{index}, strings.NewReader(mappingJSON),
		c.Client.Indices.PutMapping.WithContext(ctx),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to put mapping: %v", err))
		return err
	}
	defer resp.Body.Close()
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
	resp, err := c.Client.Indices.GetSettings(
		c.Client.Indices.GetSettings.WithContext(ctx),
		c.Client.Indices.GetSettings.WithIndex(index),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to get settings: %v", err))
		return nil, err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("get settings failed: %s", errResp.Error))
		return nil, ErrGetDocument
	}
	var out map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to decode settings response: %v", err))
		return nil, err
	}
	return out, nil
}

// PutSettings 更新索引的 settings。
func (c *Client) PutSettings(ctx context.Context, index, settingsJSON string) error {
	resp, err := c.Client.Indices.PutSettings(
		strings.NewReader(settingsJSON),
		c.Client.Indices.PutSettings.WithContext(ctx),
		c.Client.Indices.PutSettings.WithIndex(index),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to put settings: %v", err))
		return err
	}
	defer resp.Body.Close()
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
	resp, err := c.Client.Indices.Open(
		[]string{index},
		c.Client.Indices.Open.WithContext(ctx),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to open index: %v", err))
		return err
	}
	defer resp.Body.Close()
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
	resp, err := c.Client.Indices.Close(
		[]string{index},
		c.Client.Indices.Close.WithContext(ctx),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to close index: %v", err))
		return err
	}
	defer resp.Body.Close()
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

// RefreshIndex 刷新一个索引，使最近写入的数据立即可搜索。
func (c *Client) RefreshIndex(ctx context.Context, index string) error {
	resp, err := c.Client.Indices.Refresh(
		c.Client.Indices.Refresh.WithContext(ctx),
		c.Client.Indices.Refresh.WithIndex(index),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to refresh index: %v", err))
		return err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return err
		}
		log.Error(context.Background(), fmt.Sprintf("refresh index failed: %s", errResp.Error))
		return ErrCreateIndex
	}
	return nil
}

// FlushIndex 刷新一个索引的内部缓冲区到磁盘。
func (c *Client) FlushIndex(ctx context.Context, index string) error {
	resp, err := c.Client.Indices.Flush(
		c.Client.Indices.Flush.WithContext(ctx),
		c.Client.Indices.Flush.WithIndex(index),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to flush index: %v", err))
		return err
	}
	defer resp.Body.Close()
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

// SearchWithBody 以任意 DSL body（例如聚合 aggs）执行搜索，返回 SearchResult。
func (c *Client) SearchWithBody(ctx context.Context, index string, body map[string]any) (*SearchResult, error) {
	buf := &bytes.Buffer{}
	if err := json.NewEncoder(buf).Encode(body); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to encode search body: %v", err))
		return nil, err
	}
	resp, err := c.Client.Search(
		c.Client.Search.WithContext(ctx),
		c.Client.Search.WithIndex(index),
		c.Client.Search.WithBody(buf),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to search documents: %v", err))
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		if err := Body.Close(); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to close response body: %v", err))
		}
	}(resp.Body)
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("search document failed: %s", errResp.Error.Reason))
		return nil, ErrSearchDocument
	}
	var searchResult SearchResult
	if err = json.NewDecoder(resp.Body).Decode(&searchResult); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to decode search result: %v", err))
		return nil, err
	}
	return &searchResult, nil
}

// Count 返回匹配查询的文档数量。body 为查询体（DSL，可空）。
func (c *Client) Count(ctx context.Context, index string, body map[string]any) (int64, error) {
	if index == "" {
		return 0, ErrInvalidRequest
	}
	var opts []func(*esapiV9.CountRequest)
	opts = append(opts, c.Client.Count.WithContext(ctx), c.Client.Count.WithIndex(index))
	if body != nil {
		buf := &bytes.Buffer{}
		if err := json.NewEncoder(buf).Encode(body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to encode count body: %v", err))
			return 0, err
		}
		opts = append(opts, c.Client.Count.WithBody(buf))
	}
	resp, err := c.Client.Count(opts...)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to count: %v", err))
		return 0, err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return 0, err
		}
		log.Error(context.Background(), fmt.Sprintf("count failed: %s", errResp.Error))
		return 0, ErrSearchDocument
	}
	var parsed struct {
		Count int64 `json:"count"`
	}
	if err = json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to decode count response: %v", err))
		return 0, err
	}
	return parsed.Count, nil
}

// SearchScroll 按已有的 scroll id 拉取下一批结果。keepAlive 为本次续期时长（如 "1m"）。
func (c *Client) SearchScroll(ctx context.Context, scrollID, keepAlive string) (*SearchResult, error) {
	if scrollID == "" {
		return nil, ErrInvalidRequest
	}
	dur, err := time.ParseDuration(keepAlive)
	if err != nil {
		return nil, fmt.Errorf("invalid keepAlive: %v", err)
	}
	resp, err := c.Client.Scroll(
		c.Client.Scroll.WithContext(ctx),
		c.Client.Scroll.WithScrollID(scrollID),
		c.Client.Scroll.WithScroll(dur),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to scroll: %v", err))
		return nil, err
	}
	defer func(Body io.ReadCloser) {
		if err := Body.Close(); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to close response body: %v", err))
		}
	}(resp.Body)
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("scroll failed: %s", errResp.Error.Reason))
		return nil, ErrSearchDocument
	}
	var searchResult SearchResult
	if err = json.NewDecoder(resp.Body).Decode(&searchResult); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to decode search result: %v", err))
		return nil, err
	}
	return &searchResult, nil
}

// ClearScroll 清理一个或多个 scroll 上下文。
func (c *Client) ClearScroll(ctx context.Context, scrollID string) error {
	if scrollID == "" {
		return ErrInvalidRequest
	}
	resp, err := c.Client.ClearScroll(
		c.Client.ClearScroll.WithContext(ctx),
		c.Client.ClearScroll.WithScrollID(scrollID),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to clear scroll: %v", err))
		return err
	}
	defer resp.Body.Close()
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
	resp, err := c.Client.Cluster.Health(
		c.Client.Cluster.Health.WithContext(ctx),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to get cluster health: %v", err))
		return nil, err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("cluster health failed: %s", errResp.Error))
		return nil, ErrRequestFailed
	}
	var out map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to decode cluster health response: %v", err))
		return nil, err
	}
	return out, nil
}

// ClusterInfo 返回集群信息（版本等）。
func (c *Client) ClusterInfo(ctx context.Context) (map[string]any, error) {
	resp, err := c.Client.Cluster.Info(
		[]string{},
		c.Client.Cluster.Info.WithContext(ctx),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to get cluster info: %v", err))
		return nil, err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("cluster info failed: %s", errResp.Error))
		return nil, ErrRequestFailed
	}
	var out map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to decode cluster info response: %v", err))
		return nil, err
	}
	return out, nil
}

// GetILMPolicy 查询一个 ILM 策略。
func (c *Client) GetILMPolicy(ctx context.Context, policyName string) (map[string]any, error) {
	resp, err := c.Client.ILM.GetLifecycle(
		c.Client.ILM.GetLifecycle.WithContext(ctx),
		c.Client.ILM.GetLifecycle.WithPolicy(policyName),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to get ILM policy: %v", err))
		return nil, err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("get ILM policy failed: %s", errResp.Error))
		return nil, ErrRequestFailed
	}
	var out map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to decode ILM policy response: %v", err))
		return nil, err
	}
	return out, nil
}

// CreateSnapshot 在指定仓库创建一个快照。bodyJSON 为快照体（可空）。
func (c *Client) CreateSnapshot(ctx context.Context, repository, snapshot, bodyJSON string) error {
	opts := []func(*esapiV9.SnapshotCreateRequest){
		c.Client.Snapshot.Create.WithContext(ctx),
	}
	if bodyJSON != "" {
		opts = append(opts, c.Client.Snapshot.Create.WithBody(strings.NewReader(bodyJSON)))
	}
	resp, err := c.Client.Snapshot.Create(repository, snapshot, opts...)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to create snapshot: %v", err))
		return err
	}
	defer resp.Body.Close()
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
	resp, err := c.Client.Snapshot.Get(
		repository, []string{snapshot},
		c.Client.Snapshot.Get.WithContext(ctx),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to get snapshot: %v", err))
		return nil, err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("get snapshot failed: %s", errResp.Error))
		return nil, ErrRequestFailed
	}
	var out map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to decode snapshot response: %v", err))
		return nil, err
	}
	return out, nil
}

// DeleteSnapshot 删除一个快照。
func (c *Client) DeleteSnapshot(ctx context.Context, repository, snapshot string) error {
	resp, err := c.Client.Snapshot.Delete(
		repository, []string{snapshot},
		c.Client.Snapshot.Delete.WithContext(ctx),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to delete snapshot: %v", err))
		return err
	}
	defer resp.Body.Close()
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
	resp, err := c.Client.Snapshot.CreateRepository(
		repository, strings.NewReader(bodyJSON),
		c.Client.Snapshot.CreateRepository.WithContext(ctx),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to create snapshot repository: %v", err))
		return err
	}
	defer resp.Body.Close()
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
	resp, err := c.Client.Snapshot.DeleteRepository(
		[]string{repository},
		c.Client.Snapshot.DeleteRepository.WithContext(ctx),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to delete snapshot repository: %v", err))
		return err
	}
	defer resp.Body.Close()
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
	resp, err := c.Client.Snapshot.GetRepository(
		c.Client.Snapshot.GetRepository.WithContext(ctx),
		c.Client.Snapshot.GetRepository.WithRepository(repository),
	)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to get snapshot repository: %v", err))
		return nil, err
	}
	defer resp.Body.Close()
	if resp.IsError() {
		var errResp *ErrorResponse
		if errResp, err = ParseErrorMessage(resp.Body); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to parse error message: %v", err))
			return nil, err
		}
		log.Error(context.Background(), fmt.Sprintf("get snapshot repository failed: %s", errResp.Error))
		return nil, ErrRequestFailed
	}
	var out map[string]any
	if err = json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to decode snapshot repository response: %v", err))
		return nil, err
	}
	return out, nil
}
