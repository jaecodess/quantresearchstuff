-- Task 4 Q3a (ANLY-03, HANDOVER sections 8.9 and 10): do weeks with high market-wide news activity show
-- different market-wide volatility or cross-sectional dispersion? Association only, not cause.
--   hive --hivevar root=/inf2006 --hivevar run_id=<task4_run_id> -f analytics/30_q3a_news_market.sql
--
-- Tool: Hive. One GROUP BY over the full ticker-week table gives one row per week, joined to the weekly news
-- table; the terciles are window functions over ~100 week rows.
--
-- Per week, over eligible stocks:
--   dispersion      sample standard deviation of ln(ratio) across stocks (outcome week)
--   market_ln       mean ln(ratio) across stocks: the equal-weighted market move in the outcome week
--   abs_market_ln   |market_ln|, a market-wide volatility measure for that week
--   mean_vol_20d    mean 20-day volatility at the cutoff (backward-looking, known before the week starts)
-- News terciles (1 = lowest, 3 = highest abnormal GDELT event count), over weeks with coverage (Jul 2015 - Jun 2017):
--   tercile_pre     by news_abn: archives dated at least one day before the cutoff (pre-start, D-009)
--   tercile_same    by news_abn_same_week: archives inside the outcome week (SAME-WEEK, association only)

DROP TABLE IF EXISTS analytics.q3a_weekly;
CREATE EXTERNAL TABLE analytics.q3a_weekly (
    start_date DATE,
    split STRING,
    n BIGINT,
    dispersion DOUBLE,
    market_ln DOUBLE,
    abs_market_ln DOUBLE,
    mean_vol_20d DOUBLE,
    news_abn DOUBLE,
    news_abn_same_week DOUBLE,
    tone_mean DOUBLE,
    conflict_share DOUBLE,
    tercile_pre INT,
    tercile_same INT
)
STORED AS PARQUET
LOCATION '${hivevar:root}/analytics/${hivevar:run_id}/q3a_weekly';

INSERT OVERWRITE TABLE analytics.q3a_weekly
SELECT
    w.start_date,
    w.split,
    w.n,
    w.dispersion,
    w.market_ln,
    ABS(w.market_ln) AS abs_market_ln,
    w.mean_vol_20d,
    w.news_abn,
    w.news_abn_same_week,
    w.tone_mean,
    w.conflict_share,
    CASE
        WHEN w.news_abn IS NOT NULL
            THEN NTILE(3) OVER (PARTITION BY w.news_abn IS NULL ORDER BY w.news_abn, w.start_date)
    END AS tercile_pre,
    CASE
        WHEN w.news_abn_same_week IS NOT NULL
            THEN NTILE(3) OVER (
                PARTITION BY w.news_abn_same_week IS NULL ORDER BY w.news_abn_same_week, w.start_date
            )
    END AS tercile_same
FROM (
    SELECT
        f.start_date,
        f.split,
        f.n,
        f.dispersion,
        f.market_ln,
        f.mean_vol_20d,
        nw.news_abn,
        nw.news_abn_same_week,
        nw.tone_mean,
        nw.conflict_share
    FROM (
        SELECT
            start_date,
            split,
            COUNT(*) AS n,
            STDDEV_SAMP(LN(ratio)) AS dispersion,
            AVG(LN(ratio)) AS market_ln,
            AVG(vol_20d) AS mean_vol_20d
        FROM features.weekly
        WHERE eligible AND ratio > 0
        GROUP BY start_date, split
    ) AS f
    INNER JOIN features.weekly_news AS nw
        ON f.start_date = nw.start_date
    WHERE nw.news_abn IS NOT NULL OR nw.news_abn_same_week IS NOT NULL
) AS w;

-- Overview for the log; intervals come from the week bootstrap in analytics/run_analytics.py.
SELECT
    'pre_start' AS timing,
    tercile_pre AS tercile,
    COUNT(*) AS n_weeks,
    ROUND(AVG(news_abn), 4) AS mean_news_abn,
    ROUND(AVG(dispersion), 5) AS mean_dispersion,
    ROUND(AVG(abs_market_ln), 5) AS mean_abs_market_ln,
    ROUND(AVG(mean_vol_20d), 5) AS mean_vol_20d
FROM analytics.q3a_weekly
WHERE tercile_pre IS NOT NULL
GROUP BY tercile_pre
UNION ALL
SELECT
    'same_week' AS timing,
    tercile_same AS tercile,
    COUNT(*) AS n_weeks,
    ROUND(AVG(news_abn_same_week), 4) AS mean_news_abn,
    ROUND(AVG(dispersion), 5) AS mean_dispersion,
    ROUND(AVG(abs_market_ln), 5) AS mean_abs_market_ln,
    ROUND(AVG(mean_vol_20d), 5) AS mean_vol_20d
FROM analytics.q3a_weekly
WHERE tercile_same IS NOT NULL
GROUP BY tercile_same;
