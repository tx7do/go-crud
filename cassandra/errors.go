package cassandra

import "github.com/tx7do/go-wind/errors"

var (
	// ErrClientNotInitialized is returned when the repository has no session
	// executor bound (nil client).
	ErrClientNotInitialized = errors.Internal("CLIENT_NOT_INITIALIZED")

	// ErrInvalidRequest is returned when an input guard rejects a call before
	// it reaches the cluster (e.g. empty statement / illegal identifier).
	ErrInvalidRequest = errors.BadRequest("INVALID_REQUEST")

	// ErrExecQuery is returned when a query or batch execution fails.
	ErrExecQuery = errors.Internal("EXEC_QUERY_FAILED")

	// ErrSessionClosed is returned when the underlying session has been closed.
	ErrSessionClosed = errors.Internal("SESSION_CLOSED")

	// ErrQueryFailed is returned when a SELECT fails.
	ErrQueryFailed = errors.Internal("QUERY_FAILED")

	// ErrInsertFailed is returned when an INSERT/batch fails.
	ErrInsertFailed = errors.Internal("INSERT_FAILED")

	// ErrDeleteFailed is returned when a DELETE fails.
	ErrDeleteFailed = errors.Internal("DELETE_FAILED")

	// ErrCountFailed is returned when a count query fails.
	ErrCountFailed = errors.Internal("COUNT_FAILED")

	// ErrPayloadConversion is returned when entity/row conversion fails.
	ErrPayloadConversion = errors.Internal("PAYLOAD_CONVERSION_FAILED")

	// ErrInvalidPointID is returned when the entity carries an unsupported
	// primary key field.
	ErrInvalidPointID = errors.BadRequest("INVALID_POINT_ID")

	// ErrPointNotFound is returned when the requested row does not exist
	// in the caller's scope (missing or foreign-tenant rows are
	// indistinguishable to avoid existence leaks).
	ErrPointNotFound = errors.NotFound("POINT_NOT_FOUND")
)
