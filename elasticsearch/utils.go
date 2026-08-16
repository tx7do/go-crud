package elasticsearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/tx7do/go-wind-plugins/encoding"
	_ "github.com/tx7do/go-wind-plugins/encoding/json"
	"github.com/tx7do/go-wind/log"

	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

// ParseErrorMessage 解析 Elasticsearch 错误消息
func ParseErrorMessage(body io.ReadCloser) (*ErrorResponse, error) {
	defer body.Close()

	var errorResponse ErrorResponse
	if err := json.NewDecoder(body).Decode(&errorResponse); err != nil {
		return nil, ErrUnmarshalResponse
	}

	return &errorResponse, nil
}

// MergeOptions 合并 Elasticsearch 索引的映射和设置
func MergeOptions(mapping, settings string) (string, error) {
	codec := encoding.GetCodec("json")

	body := make(map[string]any)

	if mapping != "" {
		var mappingObj map[string]any
		if err := codec.Unmarshal([]byte(mapping), &mappingObj); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to unmarshal mapping: %v", err))
			return "", err
		}
		if existingMappings, ok := mappingObj["mappings"]; ok {
			body["mappings"] = existingMappings
		} else {
			body["mappings"] = mappingObj
		}
	}

	if settings != "" {
		var settingsObj map[string]any
		if err := codec.Unmarshal([]byte(settings), &settingsObj); err != nil {
			log.Error(context.Background(), fmt.Sprintf("failed to unmarshal settings: %v", err))
			return "", err
		}
		// 检查 settings 是否包含 settings 字段
		if existingSettings, ok := settingsObj["settings"]; ok {
			body["settings"] = existingSettings
		} else {
			body["settings"] = settingsObj
		}
	}

	bodyBytes, err := codec.Marshal(body)
	if err != nil {
		log.Error(context.Background(), fmt.Sprintf("failed to marshal request body: %v", err))
		return "", err
	}

	return string(bodyBytes), nil
}

// MaxPageSize 是分页每页条数的硬上限，与 Elasticsearch index.max_result_window
// 的默认值（10000）一致，防止客户端超大 page_size 造成 DoS。
const MaxPageSize = 10000

// DefaultPageSize 是未指定 page size 时的默认值。
const DefaultPageSize = 20

// ClampPageSize 将 page size 规整到 [1, MaxPageSize]；0 或未指定时取默认值。
func ClampPageSize(size uint32) int {
	switch {
	case size == 0:
		return DefaultPageSize
	case size > MaxPageSize:
		return MaxPageSize
	default:
		return int(size)
	}
}

// BuildSearchQuery 将 PagingRequest.query（JSON 对象或对象数组的查询串）
// 转换为 Elasticsearch query_string DSL（如 "field1:value1 AND field2:value2"）。
// 解析失败（空/非法 JSON）返回空串（match_all），与底层 search 的空查询语义一致。
func BuildSearchQuery(req *paginationV1.PagingRequest) string {
	if req == nil {
		return ""
	}
	return MakeQueryString(req.GetQuery(), "")
}

// queryKeyPattern 查询键（字段名）白名单：字母、数字、下划线、点、连字符。
// 键直接拼入 query_string DSL，必须先过白名单。
var queryKeyPattern = regexp.MustCompile(`^[A-Za-z0-9_.\-]+$`)

// escapeQueryValue 将值转为 query_string 的带引号字面量（转义内部引号与反斜杠），
// 防止值中携带 OR/AND/:/通配符等语法改变查询结构（DSL 注入）。
func escapeQueryValue(v string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(v) + `"`
}

func ParseQueryString(query string) []string {
	codec := encoding.GetCodec("json")

	var err error
	queryMap := make(map[string]string)
	if err = codec.Unmarshal([]byte(query), &queryMap); err == nil {
		var queries []string
		for k, v := range queryMap {
			if !queryKeyPattern.MatchString(k) {
				continue
			}
			queries = append(queries, k+":"+escapeQueryValue(v))
		}
		return queries
	}

	var queryMapArray []map[string]string
	if err = codec.Unmarshal([]byte(query), &queryMapArray); err == nil {
		var queries []string
		for _, item := range queryMapArray {
			for k, v := range item {
				if !queryKeyPattern.MatchString(k) {
					continue
				}
				queries = append(queries, k+":"+escapeQueryValue(v))
			}
		}
		return queries
	}

	return nil
}

func MakeQueryString(andQuery, orQuery string) string {
	a := ParseQueryString(andQuery)
	o := ParseQueryString(orQuery)

	if len(a) == 0 && len(o) == 0 {
		return ""
	}

	if len(a) > 0 && len(o) == 0 {
		return strings.Join(a, " AND ")
	} else if len(a) == 0 && len(o) > 0 {
		return strings.Join(o, " OR ")
	} else if len(a) > 0 && len(o) > 0 {
		return strings.Join(a, " AND ") + " AND (" + strings.Join(o, " OR ") + ")"
	} else {
		return strings.Join(a, " AND ") + " AND " + strings.Join(o, " OR ")
	}
}
