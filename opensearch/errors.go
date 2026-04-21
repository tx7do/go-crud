package opensearch

import (
	"fmt"

	"github.com/go-kratos/kratos/v2/errors"
)

var (
	// ErrRequestFailed is returned when a request to Elasticsearch fails.
	ErrRequestFailed = errors.InternalServer("REQUEST_FAILED", "request failed")

	// ErrIndexNotFound is returned when the specified index does not exist.
	ErrIndexNotFound = errors.InternalServer("INDEX_NOT_FOUND", "index not found")

	// ErrIndexAlreadyExists is returned when trying to create an index that already exists.
	ErrIndexAlreadyExists = errors.InternalServer("INDEX_ALREADY_EXISTS", "index already exists")

	ErrCreateIndex = errors.InternalServer("CREATE_INDEX_FAILED", "failed to create index")

	ErrDeleteIndex = errors.InternalServer("DELETE_INDEX_FAILED", "failed to delete index")

	// ErrDocumentNotFound is returned when a document is not found in the index.
	ErrDocumentNotFound = errors.InternalServer("DOCUMENT_NOT_FOUND", "document not found")

	// ErrUnmarshalResponse is returned when the response from Elasticsearch cannot be unmarshalled.
	ErrUnmarshalResponse = errors.InternalServer("UNMARSHAL_RESPONSE_FAILED", "failed to unmarshal response")

	ErrInsertDocument = errors.InternalServer("INSERT_DOCUMENT_FAILED", "failed to insert document")

	ErrBatchInsertDocument = errors.InternalServer("BATCH_INSERT_DOCUMENT_FAILED", "failed to batch insert documents")

	ErrGetDocument = errors.InternalServer("GET_DOCUMENT_FAILED", "failed to get document")

	ErrSearchDocument = errors.InternalServer("SEARCH_DOCUMENT_FAILED", "failed to search document")

	ErrCreateISMPolicy = errors.InternalServer("CREATE_ISM_POLICY_FAILED", "create ISM policy failed")

	ErrDocumentConflict = errors.InternalServer("DOCUMENT_CONFLICT", "document conflict occurred")

	ErrDeleteDocument = errors.InternalServer("DELETE_DOCUMENT_FAILED", "failed to delete document")

	ErrUpdateDocument = errors.InternalServer("UPDATE_DOCUMENT_FAILED", "failed to update document")

	ErrDeleteISMPolicy = errors.InternalServer("DELETE_ISM_POLICY_FAILED", "delete ISM policy failed")

	ErrInvalidRequest = errors.BadRequest("INVALID_REQUEST", "invalid request")

	ErrInvalidFilter = errors.BadRequest("INVALID_FILTER", "invalid filter")

	ErrCreateTemplate = errors.InternalServer("CREATE_TEMPLATE_FAILED", "failed to create index template")

	ErrDeleteTemplate = errors.InternalServer("DELETE_TEMPLATE_FAILED", "failed to delete index template")
)

// PartialFailureError 表示批量操作部分失败
type PartialFailureError struct {
	Total     int
	Failed    int
	FailedIDs []string
}

func (e *PartialFailureError) Error() string {
	return fmt.Sprintf("bulk insert: %d/%d failed, failed IDs: %v", e.Failed, e.Total, e.FailedIDs)
}
