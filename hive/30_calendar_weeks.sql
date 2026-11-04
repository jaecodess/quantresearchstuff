-- Market calendar and weekly start dates (PIPE-06, HANDOVER 7.1 and 7.6), computed in Hive SQL.
-- Run after hive/20_prices.sql:
--   hive --hivevar root=/inf2006 --hivevar run_id=<run_id> -f hive/30_calendar_weeks.sql
--
-- Rules:
--   market day   = a date on which at least half of all series have a clean bar
--   week anchor  = the Friday on or before a market day (1970-01-02 was a Friday)
--   start_date   = first market day on or after the anchor Friday (Friday, or the next day if Friday is a holiday)
--   cutoff_date  = the market day just before start_date (features use closes up to and including it)
--   outcome      = open on start_date to open on next_start_date
--   split        = train 2013-2015, validation 2016, test 2017, by start year; a week whose outcome
--                  ends in the next year is 'boundary_excluded'; the last week has 'no_outcome'.

DROP TABLE IF EXISTS features.market_calendar;
CREATE EXTERNAL TABLE features.market_calendar (
    trade_date DATE,
    n_series INT,
    day_idx INT
)
STORED AS PARQUET
LOCATION '${hivevar:root}/features/${hivevar:run_id}/market_calendar';

INSERT OVERWRITE TABLE features.market_calendar
SELECT
    d.trade_date,
    CAST(d.n_series AS INT) AS n_series,
    CAST(ROW_NUMBER() OVER (ORDER BY d.trade_date) AS INT) AS day_idx
FROM (
    SELECT
        `date` AS trade_date,
        COUNT(DISTINCT ticker) AS n_series
    FROM clean.prices_daily
    GROUP BY `date`
) AS d
CROSS JOIN (SELECT COUNT(DISTINCT ticker) AS n_total FROM clean.prices_daily) AS t
WHERE d.n_series >= 0.5 * t.n_total;

-- Contract table features.calendar_weeks (HANDOVER section 9) plus helper columns used downstream.
DROP TABLE IF EXISTS features.calendar_weeks;
CREATE EXTERNAL TABLE features.calendar_weeks (
    start_date DATE,
    next_start_date DATE,
    cutoff_date DATE,
    n_trading_days INT,
    split STRING,
    anchor_friday DATE,
    start_is_friday BOOLEAN,
    start_idx INT,
    cutoff_idx INT,
    next_idx INT
)
STORED AS PARQUET
LOCATION '${hivevar:root}/features/${hivevar:run_id}/calendar_weeks';

INSERT OVERWRITE TABLE features.calendar_weeks
SELECT
    s.start_date,
    s.next_start_date,
    c.trade_date AS cutoff_date,
    s.next_idx - s.start_idx AS n_trading_days,
    CASE
        WHEN s.next_start_date IS NULL THEN 'no_outcome'
        WHEN YEAR(s.start_date) != YEAR(s.next_start_date) THEN 'boundary_excluded'
        WHEN YEAR(s.start_date) <= 2015 THEN 'train'
        WHEN YEAR(s.start_date) = 2016 THEN 'validation'
        WHEN YEAR(s.start_date) = 2017 THEN 'test'
        ELSE 'other'
    END AS split,
    s.anchor_friday,
    s.start_date = s.anchor_friday AS start_is_friday,
    s.start_idx,
    c.day_idx AS cutoff_idx,
    s.next_idx
FROM (
    SELECT
        w.anchor_friday,
        w.start_date,
        w.start_idx,
        LEAD(w.start_date) OVER (ORDER BY w.start_date) AS next_start_date,
        LEAD(w.start_idx) OVER (ORDER BY w.start_date) AS next_idx
    FROM (
        SELECT
            k.anchor_friday,
            MIN(k.trade_date) AS start_date,
            MIN(k.day_idx) AS start_idx
        FROM (
            SELECT
                trade_date,
                day_idx,
                DATE_SUB(trade_date, PMOD(DATEDIFF(trade_date, '1970-01-02'), 7)) AS anchor_friday
            FROM features.market_calendar
        ) AS k
        GROUP BY k.anchor_friday
    ) AS w
) AS s
LEFT JOIN features.market_calendar AS c ON s.start_idx - 1 = c.day_idx;

-- Checks for the report.
-- (a) Weeks per split, date coverage and week lengths.
SELECT
    split,
    COUNT(*) AS n_weeks,
    MIN(start_date) AS first_start,
    MAX(start_date) AS last_start,
    SUM(CASE WHEN n_trading_days != 5 THEN 1 ELSE 0 END) AS n_short_or_long_weeks
FROM features.calendar_weeks
GROUP BY split
ORDER BY first_start;

-- (b) Weeks whose start is not a Friday (holiday Fridays); cross-check against HANDOVER 7.1.
SELECT
    anchor_friday AS holiday_friday,
    start_date
FROM features.calendar_weeks
WHERE NOT start_is_friday
ORDER BY anchor_friday;
