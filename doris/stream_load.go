package doris

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// StreamLoad uploads data to Doris using the Fe Stream Load API.
// - db: target database
// - table: target table
// - params: additional query params or stream load properties (e.g. columns, format)
// - data: io.Reader containing the payload (CSV/TSV/JSON as required)
// Returns the response body and the status code.
func (c *Client) StreamLoad(ctx context.Context, db, table string, params map[string]string, data io.Reader) ([]byte, int, error) {
	if c.streamLoadEndpoint == "" {
		return nil, 0, fmt.Errorf("stream load endpoint not configured")
	}
	if db == "" || table == "" {
		return nil, 0, fmt.Errorf("db and table must be provided")
	}

	// build URL: {endpoint}/api/{db}/{table}/_stream_load
	endpoint := strings.TrimRight(c.streamLoadEndpoint, "/")
	path := fmt.Sprintf("/api/%s/%s/_stream_load", url.PathEscape(db), url.PathEscape(table))
	u := endpoint + path

	// attach params as query string
	if params != nil && len(params) > 0 {
		q := url.Values{}
		for k, v := range params {
			q.Set(k, v)
		}
		u = u + "?" + q.Encode()
	}

	req, err := http.NewRequestWithContext(ctx, c.streamLoadMethod, u, data)
	if err != nil {
		return nil, 0, err
	}

	// set default headers Doris expects
	req.Header.Set("Expect", "")
	req.Header.Set("Content-Type", "application/octet-stream")

	// basic auth if provided
	if c.streamLoadUser != "" {
		req.SetBasicAuth(c.streamLoadUser, c.streamLoadPass)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, resp.StatusCode, err
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return body, resp.StatusCode, fmt.Errorf("stream load failed: status %d", resp.StatusCode)
	}

	return body, resp.StatusCode, nil
}
