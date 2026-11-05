-- Contract table features.weekly (PIPE-08), written by spark/weekly_features.py, partitioned by split.
--   hive --hivevar root=/inf2006 --hivevar run_id=<run_id> -f hive/40_features_weekly.sql
-- MODEL and ANLY read this table (Spark: spark.table("features.weekly")).

DROP TABLE IF EXISTS features.weekly;
CREATE EXTERNAL TABLE features.weekly (
    ticker STRING,
    start_date DATE,
    cutoff_date DATE,
    next_start_date DATE,
    eligible BOOLEAN,
    ineligible_reason STRING,
    ratio DOUBLE,
    label INT,
    rev_1w DOUBLE,
    mom_4w DOUBLE,
    mom_12w DOUBLE,
    mom_12w_skip DOUBLE,
    vol_20d DOUBLE,
    vol_ratio DOUBLE,
    max_move_20d DOUBLE,
    rel_volume DOUBLE,
    dollar_volume DOUBLE,
    rev_1w_rank DOUBLE,
    mom_4w_rank DOUBLE,
    mom_12w_rank DOUBLE,
    mom_12w_skip_rank DOUBLE,
    vol_20d_rank DOUBLE,
    vol_ratio_rank DOUBLE,
    max_move_20d_rank DOUBLE,
    rel_volume_rank DOUBLE,
    dollar_volume_rank DOUBLE,
    n_eligible_week BIGINT,
    run_id STRING
)
PARTITIONED BY (split STRING)
STORED AS PARQUET
LOCATION '${hivevar:root}/features/${hivevar:run_id}/weekly';

MSCK REPAIR TABLE features.weekly;

-- Per-split counts for the report (PIPE-10): weeks, stock-weeks, eligible, label balance.
SELECT
    split,
    COUNT(DISTINCT start_date) AS n_weeks,
    COUNT(*) AS n_ticker_weeks,
    SUM(CASE WHEN eligible THEN 1 ELSE 0 END) AS n_eligible,
    ROUND(AVG(label), 4) AS label_share_1,
    MIN(start_date) AS first_start,
    MAX(start_date) AS last_start
FROM features.weekly
GROUP BY split
ORDER BY first_start;

-- Exclusions by reason and split.
SELECT
    split,
    ineligible_reason,
    COUNT(*) AS n_ticker_weeks
FROM features.weekly
WHERE NOT eligible
GROUP BY split, ineligible_reason
ORDER BY split, n_ticker_weeks DESC;
