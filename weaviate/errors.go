package weaviate

import "github.com/tx7do/go-wind/errors"

var (
	ErrClientNotInitialized = errors.Internal("CLIENT_NOT_INITIALIZED")
	ErrQueryFailed          = errors.Internal("QUERY_FAILED")
	ErrInsertFailed         = errors.Internal("INSERT_FAILED")
	ErrDeleteFailed         = errors.Internal("DELETE_FAILED")
	ErrCountFailed          = errors.Internal("COUNT_FAILED")

	// ErrInvalidRequest is returned when request parameters are invalid.
	ErrInvalidRequest = errors.BadRequest("INVALID_REQUEST")

	// ErrInvalidVectorQuery is returned when the vector query is invalid.
	ErrInvalidVectorQuery = errors.BadRequest("INVALID_VECTOR_QUERY")

	// ErrVectorSearchFailed is returned when a vector search fails.
	ErrVectorSearchFailed = errors.Internal("VECTOR_SEARCH_FAILED")

	// ErrPayloadConversion is returned when entity/property conversion fails.
	ErrPayloadConversion = errors.Internal("PAYLOAD_CONVERSION_FAILED")

	// ErrInvalidPointID is returned when the entity carries an unsupported id field.
	ErrInvalidPointID = errors.BadRequest("INVALID_POINT_ID")

	// ErrPointNotFound is returned when the requested object does not exist
	// in the caller's scope (missing or foreign-tenant objects are
	// indistinguishable to avoid existence leaks).
	ErrPointNotFound = errors.NotFound("POINT_NOT_FOUND")
)
