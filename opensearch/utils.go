package opensearch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/tx7do/go-wind-plugins/encoding"
	_ "github.com/tx7do/go-wind-plugins/encoding/json"
	"github.com/tx7do/go-wind/log"
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

