package opensearch

import "encoding/json"

// ErrorResponse 表示 Elasticsearch 错误响应的结构
type ErrorResponse struct {
	Error struct {
		RootCause []struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"root_cause"`
		Type     string `json:"type"`
		Reason   string `json:"reason"`
		CausedBy struct {
			Type   string `json:"type"`
			Reason string `json:"reason"`
		} `json:"caused_by,omitempty"`
	} `json:"error"`
	Status int `json:"status"`
}

type SQLResult struct {
	Schema []struct {
		Name string `json:"name"`
		Type string `json:"type"`
	} `json:"schema"`
	Total    int     `json:"total"`
	Datarows [][]any `json:"datarows"` // 核心数据
	Size     int     `json:"size"`
	Status   int     `json:"status"`
}

type SearchBody struct {
	Query  *QueryDSL  `json:"query,omitempty"`
	From   int        `json:"from,omitempty"`
	Size   int        `json:"size,omitempty"`
	Source []string   `json:"_source,omitempty"`
	Sort   []SortItem `json:"sort,omitempty"`
	Aggs   AggsMap    `json:"aggs,omitempty"`
}

// QueryDSL 顶层查询
type QueryDSL struct {
	MatchAll *struct{} `json:"match_all,omitempty"`
	Match    *Match    `json:"match,omitempty"`
	Bool     *Bool     `json:"bool,omitempty"`
}

// Match 查询
type Match struct {
	Field string `json:"-"`
	Value string `json:",inline"`
}

func (m Match) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{m.Field: m.Value})
}

// Bool 查询
type Bool struct {
	Must []QueryDSL `json:"must,omitempty"`
}

type SortItem map[string]string

type AggsMap map[string]Agg

type Agg struct {
	Terms *TermsAgg `json:"terms,omitempty"`
}

type TermsAgg struct {
	Field string `json:"field"`
	Size  int    `json:"size"`
}
