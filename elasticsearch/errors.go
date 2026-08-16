package elasticsearch

import (
	"fmt"

	"github.com/tx7do/go-wind/errors"
)

var (
	// ErrRequestFailed is returned when a request to Elasticsearch fails.
	ErrRequestFailed = errors.Internal("REQUEST_FAILED")

	ErrInvalidRequest = errors.BadRequest("INVALID_REQUEST")

	// ErrIndexNotFound is returned when the specified index does not exist.
	ErrIndexNotFound = errors.Internal("INDEX_NOT_FOUND")

	// ErrIndexAlreadyExists is returned when trying to create an index that already exists.
	ErrIndexAlreadyExists = errors.Internal("INDEX_ALREADY_EXISTS")

	ErrCreateIndex = errors.Internal("CREATE_INDEX_FAILED")

	ErrDeleteIndex = errors.Internal("DELETE_INDEX_FAILED")

	// ErrDocumentNotFound is returned when a document is not found in the index.
	ErrDocumentNotFound = errors.Internal("DOCUMENT_NOT_FOUND")

	// ErrDocumentAlreadyExists is returned when trying to create a document that already exists.
	ErrDocumentAlreadyExists = errors.Internal("DOCUMENT_ALREADY_EXISTS")

	// ErrInvalidQuery is returned when the query provided to Elasticsearch is invalid.
	ErrInvalidQuery = errors.Internal("INVALID_QUERY")

	// ErrUnmarshalResponse is returned when the response from Elasticsearch cannot be unmarshalled.
	ErrUnmarshalResponse = errors.Internal("UNMARSHAL_RESPONSE_FAILED")

	ErrInsertDocument = errors.Internal("INSERT_DOCUMENT_FAILED")

	ErrBatchInsertDocument = errors.Internal("BATCH_INSERT_DOCUMENT_FAILED")

	ErrGetDocument = errors.Internal("GET_DOCUMENT_FAILED")

	ErrSearchDocument = errors.Internal("SEARCH_DOCUMENT_FAILED")

	ErrUpdateDocument = errors.Internal("UPDATE_DOCUMENT_FAILED")

	ErrDeleteDocument = errors.Internal("DELETE_DOCUMENT_FAILED")

	ErrCreateILMPolicy = errors.Internal("CREATE_ILM_POLICY_FAILED")

	ErrDocumentConflict = errors.Internal("DOCUMENT_CONFLICT")
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
