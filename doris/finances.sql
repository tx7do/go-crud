
CREATE DATABASE IF NOT EXISTS finances;

USE finances;

STOP ROUTINE LOAD FOR finances.job_sync_candles;

DROP MATERIALIZED VIEW IF EXISTS finances.mv_symbol_daily_summary;
DROP MATERIALIZED VIEW IF EXISTS finances.mv_symbol_hour_summary;
DROP MATERIALIZED VIEW IF EXISTS finances.mv_symbol_total_summary;
DROP MATERIALIZED VIEW IF EXISTS finances.mv_market_daily_summary;
DROP MATERIALIZED VIEW IF EXISTS finances.mv_symbol_weekly_summary;
DROP MATERIALIZED VIEW IF EXISTS finances.mv_symbol_monthly_summary;
DROP MATERIALIZED VIEW IF EXISTS finances.mv_market_overview;


-- 蜡烛图主表
CREATE TABLE IF NOT EXISTS finances.candles (
    timestamp DATETIME NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,

    trade_date DATE NOT NULL,

    INDEX idx_symbol (symbol) USING INVERTED
)
ENGINE = OLAP
UNIQUE KEY(timestamp, symbol)
PARTITION BY RANGE(timestamp) ()
DISTRIBUTED BY HASH(symbol) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "MONTH",
    "dynamic_partition.start" = "-12",
    "dynamic_partition.end" = "1",
    "dynamic_partition.prefix" = "p",
    "enable_unique_key_merge_on_write" = "true"
);


-- Kafka 实时同步
CREATE ROUTINE LOAD finances.job_sync_candles ON candles
COLUMNS(
    timestamp,
    symbol,
    open,
    high,
    low,
    close,
    volume,
    trade_date = date(timestamp)
)
PROPERTIES (
    "format" = "json",
    "jsonpaths" = "[\"$.ts\", \"$.ticker\", \"$.o\", \"$.h\", \"$.l\", \"$.c\", \"$.v\"]",
    "max_batch_interval" = "5",
    "max_error_number" = "0"
)
FROM KAFKA (
    "kafka_broker_list" = "kafka:9092",
    "kafka_topic" = "market_candles",
    "property.group.id" = "doris_candles_consumer",
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);


-- =============================================
-- 1. 【个股每日汇总】（包含：振幅、换手率、成交额、涨跌幅）
-- =============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS finances.mv_symbol_daily_summary
DISTRIBUTED BY HASH(symbol) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
)
AS
SELECT
    symbol,
    trade_date,
    SUM(volume)           AS total_volume,   -- 总成交量
    MAX(high)             AS max_price,      -- 当日最高价
    MIN(low)              AS min_price,      -- 当日最低价
    MAX(close)            AS close_price,    -- 收盘价（最新/收盘）
    MIN(open)             AS open_price,     -- 开盘价
    ROUND((MAX(close) - MIN(open)) / MIN(open) * 100, 2)  AS change_ratio  -- 涨跌幅 %
FROM finances.candles
GROUP BY symbol, trade_date;


-- =============================================
-- 2. 【个股小时级汇总】
-- =============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS finances.mv_symbol_hour_summary
DISTRIBUTED BY HASH(symbol) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
)
AS
SELECT
    symbol,
    trade_date,
    HOUR(timestamp) AS trade_hour,
    SUM(volume) AS total_volume,
    MAX(high) AS max_price,
    MIN(low) AS min_price,
    MAX(close) AS close_price,
    MIN(open) AS open_price,
    ROUND((MAX(close)-MIN(open))/MIN(open)*100, 2) AS change_ratio
FROM finances.candles
GROUP BY symbol, trade_date, trade_hour;


-- =============================================
-- 3. 【个股全周期指标】
-- =============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS finances.mv_symbol_total_summary
DISTRIBUTED BY HASH(symbol) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
)
AS
SELECT
    symbol,
    SUM(volume) AS total_volume_all,
    MAX(high) AS highest_price_all,
    MIN(low) AS lowest_price_all,
    MAX(close) AS latest_close
FROM finances.candles
GROUP BY symbol;


-- =============================================
-- 4. 【大盘每日统计】（涨跌家数、上涨率、总市值）
-- =============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS finances.mv_market_daily_summary
DISTRIBUTED BY HASH(trade_date) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
)
AS
SELECT
    trade_date,
    COUNT(DISTINCT symbol) AS symbol_count,
    SUM(volume) AS market_total_volume,
    AVG(close) AS avg_close_price
FROM finances.candles
GROUP BY trade_date;


-- =============================================
-- 5. 【周 K 线】
-- =============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS finances.mv_symbol_weekly_summary
DISTRIBUTED BY HASH(symbol) BUCKETS 4
PROPERTIES ("replication_num" = "1")
AS
SELECT
    symbol,
    YEAR(timestamp) AS trade_year,
    WEEK(timestamp) AS trade_week,
    SUM(volume)                AS total_volume,
    SUM(close * volume)        AS total_amount,
    MAX(high)                  AS max_price,
    MIN(low)                   AS min_price,
    MAX(close)                 AS close_price,
    MIN(open)                  AS open_price
FROM finances.candles
GROUP BY symbol, trade_year, trade_week;


-- =============================================
-- 6. 【月 K 线】
-- =============================================
CREATE MATERIALIZED VIEW IF NOT EXISTS finances.mv_symbol_monthly_summary
DISTRIBUTED BY HASH(symbol) BUCKETS 4
PROPERTIES ("replication_num" = "1")
AS
SELECT
    symbol,
    YEAR(timestamp)  AS trade_year,
    MONTH(timestamp) AS trade_month,
    SUM(volume)                AS total_volume,
    SUM(close * volume)        AS total_amount,
    MAX(high)                  AS max_price,
    MIN(low)                   AS min_price,
    MAX(close)                 AS close_price,
    MIN(open)                  AS open_price
FROM finances.candles
GROUP BY symbol, trade_year, trade_month;
