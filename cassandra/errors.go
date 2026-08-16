package cassandra

import "github.com/tx7do/go-wind/errors"

var (
	// ErrInvalidRequest is returned when an input guard rejects a call before
	// it reaches the cluster (e.g. empty statement).
	ErrInvalidRequest = errors.BadRequest("INVALID_REQUEST")

	// ErrExecQuery is returned when a query or batch execution fails.
	ErrExecQuery = errors.Internal("EXEC_QUERY_FAILED")

	// ErrSessionClosed is returned when the underlying session has been closed.
	ErrSessionClosed = errors.Internal("SESSION_CLOSED")
)
