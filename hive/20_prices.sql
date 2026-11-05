-- Price tables (PIPE-01). Run after spark/clean_prices.py for the same run_id:
--   hive --hivevar root=/inf2006 --hivevar run_id=<run_id> \
--        --hivevar raw_prices_dir=/inf2006/raw/prices/plotly-datasets-0c447c4/acquired=<date> -f hive/20_prices.sql
-- External tables: dropping a table never deletes data. Safe to rerun on a fresh cluster (D-013).

-- Raw CSV exactly as staged, all columns as text so nothing is coerced on read.
DROP TABLE IF EXISTS raw.prices_csv;
CREATE EXTERNAL TABLE raw.prices_csv (
    date_s STRING,
    open_s STRING,
    high_s STRING,
    low_s STRING,
    close_s STRING,
    volume_s STRING,
    name_s STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:raw_prices_dir}'
TBLPROPERTIES ('skip.header.line.count' = '1');

-- Contract table clean.prices_daily (HANDOVER section 9), written by spark/clean_prices.py.
DROP TABLE IF EXISTS clean.prices_daily;
CREATE EXTERNAL TABLE clean.prices_daily (
    ticker STRING,
    `date` DATE,
    `open` DOUBLE,
    high DOUBLE,
    low DOUBLE,
    `close` DOUBLE,
    volume BIGINT,
    source_file STRING,
    source_row BIGINT,
    ingest_run_id STRING
)
STORED AS PARQUET
LOCATION '${hivevar:root}/clean/prices_daily/${hivevar:run_id}';

-- Contract table clean.prices_rejected: same columns + reason + the original line.
DROP TABLE IF EXISTS clean.prices_rejected;
CREATE EXTERNAL TABLE clean.prices_rejected (
    ticker STRING,
    `date` DATE,
    `open` DOUBLE,
    high DOUBLE,
    low DOUBLE,
    `close` DOUBLE,
    volume BIGINT,
    source_file STRING,
    source_row BIGINT,
    ingest_run_id STRING,
    reject_reason STRING,
    raw_line STRING
)
STORED AS PARQUET
LOCATION '${hivevar:root}/rejected/prices_daily/${hivevar:run_id}';

-- Reconciliation (PIPE-10): raw data rows = clean + rejected, and rejected rows by reason.
SELECT
    r.raw_rows,
    c.clean_rows,
    j.rejected_rows,
    r.raw_rows - c.clean_rows - j.rejected_rows AS unexplained
FROM (SELECT COUNT(*) AS raw_rows FROM raw.prices_csv WHERE date_s != 'date') AS r
CROSS JOIN (SELECT COUNT(*) AS clean_rows FROM clean.prices_daily) AS c
CROSS JOIN (SELECT COUNT(*) AS rejected_rows FROM clean.prices_rejected) AS j;

SELECT
    reject_reason,
    COUNT(*) AS n_rows
FROM clean.prices_rejected
GROUP BY reject_reason
ORDER BY n_rows DESC;
