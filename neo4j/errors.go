package neo4j

import "github.com/tx7do/go-wind/errors"

var (
	ErrClientNotInitialized = errors.Internal("CLIENT_NOT_INITIALIZED")
	ErrQueryFailed          = errors.Internal("QUERY_FAILED")
	ErrInsertFailed         = errors.Internal("INSERT_FAILED")
	ErrDeleteFailed         = errors.Internal("DELETE_FAILED")
	ErrCountFailed          = errors.Internal("COUNT_FAILED")

	// ErrInvalidRequest is returned when request parameters are invalid.
	ErrInvalidRequest = errors.BadRequest("INVALID_REQUEST")

	// ErrPayloadConversion is returned when entity/property conversion fails.
	ErrPayloadConversion = errors.Internal("PAYLOAD_CONVERSION_FAILED")

	// ErrPointNotFound is returned when the requested node does not exist
	// in the caller's scope (missing or foreign-tenant nodes are
	// indistinguishable to avoid existence leaks).
	ErrPointNotFound = errors.NotFound("POINT_NOT_FOUND")
)
