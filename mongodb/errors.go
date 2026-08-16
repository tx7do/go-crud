package mongodb

import "github.com/tx7do/go-wind/errors"

// Sentinel errors for the mongodb client. These are defined for API parity
// with the other go-crud client modules; the CRUD methods currently still
// return the underlying mongo-driver errors unchanged. Callers may match on
// these sentinels if/when the methods are migrated to return them.
var (
	ErrClientNotInitialized = errors.Internal("CLIENT_NOT_INITIALIZED")
	ErrQueryFailed          = errors.Internal("QUERY_FAILED")
	ErrInsertFailed         = errors.Internal("INSERT_FAILED")
	ErrUpdateFailed         = errors.Internal("UPDATE_FAILED")
	ErrDeleteFailed         = errors.Internal("DELETE_FAILED")
	ErrCountFailed          = errors.Internal("COUNT_FAILED")
)
