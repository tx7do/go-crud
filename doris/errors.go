package doris

import "github.com/tx7do/go-wind/errors"

// Sentinel errors for the doris client. These are defined for API parity with
// the other go-crud client modules; the CRUD methods currently still return
// the underlying sqlx/database errors (or fmt.Errorf) unchanged. Callers may
// match on these sentinels if/when the methods are migrated to return them.
var (
	ErrClientNotInitialized = errors.Internal("CLIENT_NOT_INITIALIZED")
	ErrQueryFailed          = errors.Internal("QUERY_FAILED")
	ErrInsertFailed         = errors.Internal("INSERT_FAILED")
	ErrBatchInsertFailed    = errors.Internal("BATCH_INSERT_FAILED")
)
