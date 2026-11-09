-- Task 4 Q1 (ANLY-01, HANDOVER section 10): when is trading volume unusually high or low compared with each
-- stock's own history, and how does that relate to next-week source price ratios?
--   hive --hivevar root=/inf2006 --hivevar run_id=<task4_run_id> -f analytics/20_q1_volume_quintiles.sql
--
-- Tool: Hive. This is a grouped aggregation over the full ticker-week table (window + GROUP BY), which Hive
-- expresses directly in SQL and runs distributed; only the small weekly result (week x quintile) leaves the cluster.
--
-- rel_volume (from features.weekly) = ln(mean volume over the 5 days to the cutoff / mean over the 60 days to the
-- cutoff), so it already measures "unusual for this stock"; it uses only data up to the cutoff. Each week, eligible stocks are split into
-- quintiles of rel_volume (1 = most unusually quiet, 5 = most unusually busy). Per week and quintile:
--   n             stocks in the group
--   share_above   share whose next-week source price ratio is above that week's median (label = 1)
--   mean_excess   mean of ln(ratio) minus the week's median ln(ratio): a source-price-ratio difference, not a return
-- Intervals come later from a week bootstrap (analytics/run_analytics.py); weeks, not rows, are the unit.
-- All three splits are included: the model is frozen (Gate 2), so this can no longer influence feature choices.

DROP TABLE IF EXISTS analytics.q1_weekly;
CREATE EXTERNAL TABLE analytics.q1_weekly (
    split STRING,
    start_date DATE,
    quintile INT,
    n BIGINT,
    share_above DOUBLE,
    mean_excess DOUBLE,
    rel_volume_min DOUBLE,
    rel_volume_max DOUBLE
)
STORED AS PARQUET
LOCATION '${hivevar:root}/analytics/${hivevar:run_id}/q1_weekly';

INSERT OVERWRITE TABLE analytics.q1_weekly
SELECT
    q.split,
    q.start_date,
    q.quintile,
    COUNT(*) AS n,
    AVG(q.label) AS share_above,
    AVG(q.ln_ratio - q.med_ln) AS mean_excess,
    MIN(q.rel_volume) AS rel_volume_min,
    MAX(q.rel_volume) AS rel_volume_max
FROM (
    SELECT
        r.split,
        r.start_date,
        r.label,
        r.rel_volume,
        r.ln_ratio,
        m.med_ln,
        NTILE(5) OVER (PARTITION BY r.start_date ORDER BY r.rel_volume, r.ticker) AS quintile
    FROM (
        SELECT
            split,
            start_date,
            ticker,
            label,
            rel_volume,
            LN(ratio) AS ln_ratio
        FROM features.weekly
        WHERE eligible AND rel_volume IS NOT NULL AND ratio > 0
    ) AS r
    INNER JOIN (
        SELECT
            start_date,
            PERCENTILE_APPROX(LN(ratio), 0.5, 100000) AS med_ln
        FROM features.weekly
        WHERE eligible AND rel_volume IS NOT NULL AND ratio > 0
        GROUP BY start_date
    ) AS m
        ON r.start_date = m.start_date
) AS q
GROUP BY q.split, q.start_date, q.quintile;

-- Overview for the log (row-weighted; the reported figures are the week-level means with intervals).
SELECT
    split,
    quintile,
    COUNT(*) AS n_weeks,
    SUM(n) AS n_rows,
    ROUND(AVG(share_above), 4) AS mean_weekly_share_above,
    ROUND(AVG(mean_excess), 5) AS mean_weekly_excess_ln
FROM analytics.q1_weekly
GROUP BY split, quintile
ORDER BY split, quintile;
