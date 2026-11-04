-- Reviewed split events (PIPE-05): config/split_events.csv registered as a Hive table.
-- The run script copies the CSV to ${root}/config/split_events/${run_id}/ first. Then:
--   hive --hivevar root=/inf2006 --hivevar run_id=<run_id> -f hive/31_split_events.sql
-- Only decision = 'confirmed' rows affect eligibility (spark/weekly_features.py).

DROP TABLE IF EXISTS clean.split_events;
CREATE EXTERNAL TABLE clean.split_events (
    ticker STRING,
    `date` DATE,
    ratio_est DOUBLE,
    k DOUBLE,
    decision STRING,
    evidence STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:root}/config/split_events/${hivevar:run_id}'
TBLPROPERTIES ('skip.header.line.count' = '1');

SELECT
    decision,
    COUNT(*) AS n_events
FROM clean.split_events
WHERE ticker != 'ticker'
GROUP BY decision;
