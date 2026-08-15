package clickhouse

import "github.com/tx7do/go-wind/errors"

var (
	// ErrInvalidColumnName is returned when an invalid column name is used.
	ErrInvalidColumnName = errors.Internal("INVALID_COLUMN_NAME")

	// ErrInvalidTableName is returned when an invalid table name is used.
	ErrInvalidTableName = errors.Internal("INVALID_TABLE_NAME")

	// ErrInvalidCondition is returned when an invalid condition is used in a query.
	ErrInvalidCondition = errors.Internal("INVALID_CONDITION")

	// ErrQueryExecutionFailed is returned when a query execution fails.
	ErrQueryExecutionFailed = errors.Internal("QUERY_EXECUTION_FAILED")

	// ErrExecutionFailed is returned when a general execution fails.
	ErrExecutionFailed = errors.Internal("EXECUTION_FAILED")

	// ErrAsyncInsertFailed is returned when an asynchronous insert operation fails.
	ErrAsyncInsertFailed = errors.Internal("ASYNC_INSERT_FAILED")

	// ErrRowScanFailed is returned when scanning rows from a query result fails.
	ErrRowScanFailed = errors.Internal("ROW_SCAN_FAILED")

	// ErrRowsIterationError is returned when there is an error iterating over rows.
	ErrRowsIterationError = errors.Internal("ROWS_ITERATION_ERROR")

	// ErrRowNotFound is returned when a specific row is not found in the result set.
	ErrRowNotFound = errors.Internal("ROW_NOT_FOUND")

	// ErrConnectionFailed is returned when the connection to ClickHouse fails.
	ErrConnectionFailed = errors.Internal("CONNECTION_FAILED")

	// ErrDatabaseNotFound is returned when the specified database is not found.
	ErrDatabaseNotFound = errors.Internal("DATABASE_NOT_FOUND")

	// ErrTableNotFound is returned when the specified table is not found.
	ErrTableNotFound = errors.Internal("TABLE_NOT_FOUND")

	// ErrInsertFailed is returned when an insert operation fails.
	ErrInsertFailed = errors.Internal("INSERT_FAILED")

	// ErrUpdateFailed is returned when an update operation fails.
	ErrUpdateFailed = errors.Internal("UPDATE_FAILED")

	// ErrDeleteFailed is returned when a delete operation fails.
	ErrDeleteFailed = errors.Internal("DELETE_FAILED")

	// ErrTransactionFailed is returned when a transaction fails.
	ErrTransactionFailed = errors.Internal("TRANSACTION_FAILED")

	// ErrClientNotInitialized is returned when the ClickHouse client is not initialized.
	ErrClientNotInitialized = errors.Internal("CLIENT_NOT_INITIALIZED")

	// ErrGetServerVersionFailed is returned when getting the server version fails.
	ErrGetServerVersionFailed = errors.Internal("GET_SERVER_VERSION_FAILED")

	// ErrPingFailed is returned when a ping to the ClickHouse server fails.
	ErrPingFailed = errors.Internal("PING_FAILED")

	// ErrCreatorFunctionNil is returned when the creator function is nil.
	ErrCreatorFunctionNil = errors.Internal("CREATOR_FUNCTION_NIL")

	// ErrBatchPrepareFailed is returned when a batch prepare operation fails.
	ErrBatchPrepareFailed = errors.Internal("BATCH_PREPARE_FAILED")

	// ErrBatchSendFailed is returned when a batch send operation fails.
	ErrBatchSendFailed = errors.Internal("BATCH_SEND_FAILED")

	// ErrBatchAppendFailed is returned when appending to a batch fails.
	ErrBatchAppendFailed = errors.Internal("BATCH_APPEND_FAILED")

	// ErrBatchInsertFailed is returned when a batch insert operation fails.
	ErrBatchInsertFailed = errors.Internal("BATCH_INSERT_FAILED")

	// ErrInvalidDSN is returned when the data source name (DSN) is invalid.
	ErrInvalidDSN = errors.Internal("INVALID_DSN")

	// ErrInvalidProxyURL is returned when the proxy URL is invalid.
	ErrInvalidProxyURL = errors.Internal("INVALID_PROXY_URL")

	// ErrPrepareInsertDataFailed is returned when preparing insert data fails.
	ErrPrepareInsertDataFailed = errors.Internal("PREPARE_INSERT_DATA_FAILED")

	// ErrInvalidColumnData is returned when the column data type is invalid.
	ErrInvalidColumnData = errors.Internal("INVALID_COLUMN_DATA")

	ErrInvalidArgument = errors.BadRequest("INVALID_ARGUMENT")
)
