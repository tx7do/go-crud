package influxdb

import "github.com/tx7do/go-wind/errors"

var (
	ErrInfluxDBClientNotInitialized = errors.Internal("INFLUXDB_CLIENT_NOT_INITIALIZED")

	ErrInfluxDBConnectFailed = errors.Internal("INFLUXDB_CONNECT_FAILED")

	ErrInfluxDBCreateDatabaseFailed = errors.Internal("INFLUXDB_CREATE_DATABASE_FAILED")

	ErrInfluxDBQueryFailed = errors.Internal("INFLUXDB_QUERY_FAILED")

	ErrClientNotConnected = errors.Internal("INFLUXDB_CLIENT_NOT_CONNECTED")

	ErrInvalidPoint = errors.Internal("INFLUXDB_INVALID_POINT")

	ErrNoPointsToInsert = errors.Internal("INFLUXDB_NO_POINTS_TO_INSERT")

	ErrEmptyData = errors.Internal("INFLUXDB_EMPTY_DATA")

	ErrBatchInsertFailed = errors.Internal("INFLUXDB_BATCH_INSERT_FAILED")

	ErrInsertFailed = errors.Internal("INFLUXDB_INSERT_FAILED")
)
