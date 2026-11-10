-- Model output tables (HANDOVER section 9 contracts), written by modeling/run_model.py test mode.
--   hive --hivevar root=/inf2006 --hivevar run_id=<run_id> -f modeling/model_tables.sql
-- Registered as external Parquet tables so Hive and Spark (Task 4 analytics) can query them.

DROP TABLE IF EXISTS model.predictions;
CREATE EXTERNAL TABLE model.predictions (
    ticker STRING,
    start_date DATE,
    model_id STRING,
    score DOUBLE,
    pred INT,
    label DOUBLE,
    ratio DOUBLE,
    run_id STRING
)
PARTITIONED BY (split STRING)
STORED AS PARQUET
LOCATION '${hivevar:root}/predictions/${hivevar:run_id}/predictions';

MSCK REPAIR TABLE model.predictions;

DROP TABLE IF EXISTS model.weekly_eval;
CREATE EXTERNAL TABLE model.weekly_eval (
    model_id STRING,
    start_date DATE,
    split STRING,
    n BIGINT,
    n_pos DOUBLE,
    auc DOUBLE,
    rank_ic DOUBLE,
    decile_spread DOUBLE,
    run_id STRING
)
STORED AS PARQUET
LOCATION '${hivevar:root}/eval/${hivevar:run_id}/weekly_eval';

DROP TABLE IF EXISTS model.weights;
CREATE EXTERNAL TABLE model.weights (
    ticker STRING,
    start_date DATE,
    weight DOUBLE,
    model_id STRING,
    run_id STRING
)
STORED AS PARQUET
LOCATION '${hivevar:root}/predictions/${hivevar:run_id}/weights';

-- Checks: weights sum to 1 each week; weekly AUC by split and model.
SELECT
    MIN(total) AS min_weight_sum,
    MAX(total) AS max_weight_sum,
    COUNT(*) AS n_weeks
FROM (
    SELECT
        start_date,
        SUM(weight) AS total
    FROM model.weights
    GROUP BY start_date
) AS w;

SELECT
    split,
    model_id,
    COUNT(*) AS n_weeks,
    ROUND(AVG(auc), 4) AS mean_weekly_auc,
    ROUND(AVG(rank_ic), 4) AS mean_rank_ic
FROM model.weekly_eval
GROUP BY split, model_id
ORDER BY split, model_id;
