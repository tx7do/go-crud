package opensearch

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	paginationV1 "github.com/tx7do/go-crud/api/gen/go/pagination/v1"
)

// newUnitClient builds a Client without any address. The OpenSearch v4 client
// constructs lazily, so this is enough for exercising input-validation guards
// that short-circuit before any network call.
func newUnitClient(t *testing.T) *Client {
	t.Helper()
	c, err := NewOpenSearchClient()
	assert.Nil(t, err, "client must construct without options")
	assert.NotNil(t, c)
	return c
}

// TestNewOpenSearchClient_NoOptions ensures construction does not panic or
// error when no options are supplied. The internal opensearch-go client is
// created lazily and the field is non-nil afterwards.
func TestNewOpenSearchClient_NoOptions(t *testing.T) {
	c, err := NewOpenSearchClient()
	assert.Nil(t, err)
	if assert.NotNil(t, c) {
		assert.NotNil(t, c.Client, "embedded opensearch client should be initialized")
	}
}

// TestSearch_NilRequest verifies the nil-request guard returns ErrInvalidRequest
// without contacting the cluster.
func TestSearch_NilRequest(t *testing.T) {
	c := newUnitClient(t)
	_, err := c.Search(context.Background(), "any", nil)
	assert.True(t, errors.Is(err, ErrInvalidRequest))
}

// TestQueryWithSQLPagination_NilRequest verifies the nil-request guard in the
// SQL pagination path returns ErrInvalidRequest without a cluster round-trip.
func TestQueryWithSQLPagination_NilRequest(t *testing.T) {
	c := newUnitClient(t)
	_, err := c.QueryWithSQLPagination(context.Background(), "any", nil)
	assert.True(t, errors.Is(err, ErrInvalidRequest))
}

// TestBatchInsertDocument_EmptyDataSet verifies the empty-dataSet short-circuit
// returns nil immediately, without performing any bulk work.
func TestBatchInsertDocument_EmptyDataSet(t *testing.T) {
	c := newUnitClient(t)
	err := c.BatchInsertDocument(context.Background(), "any", nil, nil)
	assert.Nil(t, err)
	err = c.BatchInsertDocument(context.Background(), "any", []any{}, nil)
	assert.Nil(t, err)
}

// TestUpdateDocument_EmptyPK verifies the empty-primary-key guard returns
// ErrDocumentNotFound before any marshalling or network call.
func TestUpdateDocument_EmptyPK(t *testing.T) {
	c := newUnitClient(t)
	err := c.UpdateDocument(context.Background(), "any", "", struct{ X int }{X: 1})
	assert.True(t, errors.Is(err, ErrDocumentNotFound))
}

// TestPagingRequestDefaults confirms the PagingRequest zero value reports the
// defaults the Search path branches on, so the unit tests above are anchored to
// stable behaviour.
func TestPagingRequestDefaults(t *testing.T) {
	var req paginationV1.PagingRequest
	// NoPaging defaults to false (paging enabled) on a zero-value request.
	assert.False(t, req.GetNoPaging())
	assert.Empty(t, req.GetOrderBy())
	assert.Empty(t, req.GetSorting())
}
