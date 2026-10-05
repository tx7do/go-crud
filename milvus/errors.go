package milvus

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

	// ErrSchemaBuildFailed is returned when the entity cannot be mapped to a
	// Milvus schema (unsupported field kinds, missing vector field, etc.).
	ErrSchemaBuildFailed = errors.BadRequest("SCHEMA_BUILD_FAILED")

	// ErrColumnConversion is returned when entity/column conversion fails.
	ErrColumnConversion = errors.Internal("COLUMN_CONVERSION_FAILED")

	// ErrInvalidPointID is returned when the entity carries an unsupported
	// primary key field.
	ErrInvalidPointID = errors.BadRequest("INVALID_POINT_ID")

	// ErrPointNotFound is returned when the requested row does not exist
	// in the caller's scope (missing or foreign-tenant rows are
	// indistinguishable to avoid existence leaks).
	ErrPointNotFound = errors.NotFound("POINT_NOT_FOUND")
)
