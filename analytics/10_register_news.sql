-- Task 4 input: register the existing weekly news table (features.weekly_news, PIPE-11) over its saved location.
-- Unlike hive/41_weekly_news.sql this only points a table at data Task 2 already wrote (for example the S3 copy
-- of /inf2006/features/<task2_run_id>/), so it needs no GDELT audit tables and recomputes nothing.
--   hive --hivevar in_root=s3://<bucket>/inf2006 --hivevar features_run_id=<task2_run_id> \
--        -f analytics/10_register_news.sql
-- features.weekly and model.predictions are registered with the existing scripts, pointed at the same root:
--   hive/40_features_weekly.sql (root=<in_root>, run_id=<task2_run_id>)
--   modeling/model_tables.sql   (root=<in_root>, run_id=<task3_run_id>)

DROP TABLE IF EXISTS features.weekly_news;
CREATE EXTERNAL TABLE features.weekly_news (
    start_date DATE,
    news_abn DOUBLE,
    tone_mean DOUBLE,
    conflict_share DOUBLE,
    news_abn_same_week DOUBLE,
    n_days_pre INT,
    n_days_base INT,
    n_days_same_week INT
)
STORED AS PARQUET
LOCATION '${hivevar:in_root}/features/${hivevar:features_run_id}/weekly_news';

-- Coverage check for the log: weeks with a pre-start and a same-week measure.
SELECT
    COUNT(*) AS n_weeks,
    SUM(CASE WHEN news_abn IS NOT NULL THEN 1 ELSE 0 END) AS n_pre_start,
    SUM(CASE WHEN news_abn_same_week IS NOT NULL THEN 1 ELSE 0 END) AS n_same_week,
    MIN(start_date) AS first_week,
    MAX(start_date) AS last_week
FROM features.weekly_news;
