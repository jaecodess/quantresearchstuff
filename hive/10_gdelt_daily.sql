-- GDELT daily aggregates (PIPE-03 prototype; PIPE-11 uses the same tables on the full run).
-- Input: output of streaming/gdelt_daily (one row per archive date, tab-separated).
-- Run with the run_id used for the Streaming job, e.g.:
--   hive --hivevar run_id=20261103-1500-dev-ref -f hive/10_gdelt_daily.sql
-- Safe to rerun on a fresh cluster (DDL kept in versioned scripts, decision D-013).

-- 1. Register the Streaming output exactly as written (external: dropping the table keeps the files).
DROP TABLE IF EXISTS raw.gdelt_daily_audit;
CREATE EXTERNAL TABLE raw.gdelt_daily_audit (
    archive_date STRING,
    n_archives INT,
    n_corrupt_archive INT,
    n_lines BIGINT,
    n_malformed BIGINT,
    n_bad_value BIGINT,
    n_dup BIGINT,
    n_events BIGINT,
    n_us BIGINT,
    n_quad1 BIGINT,
    n_quad2 BIGINT,
    n_quad3 BIGINT,
    n_quad4 BIGINT,
    n_conflict BIGINT,
    sum_mentions BIGINT,
    sum_articles BIGINT,
    sum_tone_x_articles DOUBLE,
    sum_goldstein DOUBLE,
    n_dateadded_mismatch BIGINT,
    n_lag_0 BIGINT,
    n_lag_1_7 BIGINT,
    n_lag_8_30 BIGINT,
    n_lag_31_plus BIGINT,
    n_lag_negative BIGINT,
    min_event_id BIGINT,
    max_event_id BIGINT,
    source_file STRING,
    tone_wmean DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/inf2006/staging/streaming/${hivevar:run_id}/gdelt_daily';

-- 2. Contract table clean.gdelt_daily (HANDOVER section 9), Parquet, readable from Spark.
--    Corrupt or empty archives stay out of the clean table; they remain visible in the audit table.
DROP TABLE IF EXISTS clean.gdelt_daily;
CREATE TABLE clean.gdelt_daily
STORED AS PARQUET
AS
SELECT
    CAST(archive_date AS DATE) AS archive_date,
    n_events,
    n_us,
    n_conflict,
    tone_wmean,
    n_malformed,
    n_dup,
    source_file
FROM raw.gdelt_daily_audit
WHERE n_corrupt_archive = 0 AND n_events > 0 AND archive_date != '_unknown';

-- 3. Reconciliation for the report: lines read = events kept + every rejected category.
SELECT
    COUNT(*) AS n_dates,
    SUM(n_archives) AS n_archives,
    SUM(n_corrupt_archive) AS n_corrupt_archives,
    SUM(n_lines) AS n_lines,
    SUM(n_events) AS n_events,
    SUM(n_malformed) AS n_malformed,
    SUM(n_bad_value) AS n_bad_value,
    SUM(n_dup) AS n_dup,
    SUM(n_lines) - SUM(n_events) - SUM(n_malformed) - SUM(n_bad_value) - SUM(n_dup) AS unexplained,
    SUM(n_dateadded_mismatch) AS n_dateadded_mismatch,
    SUM(n_lag_31_plus) AS n_lag_31_plus
FROM raw.gdelt_daily_audit;

-- 4. Missing archive dates in the expected window (Jul 2015 to Jun 2017, 731 days).
SELECT d.expected_date
FROM (
    SELECT DATE_ADD('2015-07-01', pos) AS expected_date
    FROM (SELECT 1 AS one) AS t
    LATERAL VIEW POSEXPLODE(SPLIT(SPACE(730), ' ')) pe AS pos, val
) AS d
LEFT JOIN clean.gdelt_daily AS g ON d.expected_date = g.archive_date
WHERE g.archive_date IS NULL
ORDER BY d.expected_date;

-- 5. Cross-archive duplicate check: GDELT event IDs rise over time, so overlapping ID ranges
--    between two archive dates point to events published twice.
SELECT
    a.archive_date,
    b.archive_date AS overlapping_date
FROM raw.gdelt_daily_audit AS a
INNER JOIN raw.gdelt_daily_audit AS b
    ON a.archive_date < b.archive_date
    AND a.max_event_id >= b.min_event_id
    AND a.min_event_id <= b.max_event_id;
