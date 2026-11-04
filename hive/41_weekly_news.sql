-- Weekly market-wide news measures from GDELT (PIPE-11, HANDOVER 8.9), computed in Hive SQL.
-- Needs raw.gdelt_daily_audit (hive/10_gdelt_daily.sql) and features.calendar_weeks (hive/30_calendar_weeks.sql):
--   hive --hivevar root=/inf2006 --hivevar run_id=<run_id> --hivevar gdelt_run_id=<run_id of the GDELT job> \
--        -f hive/41_weekly_news.sql
--
-- Timing (conservative rule, D-009): each archive is published the morning after its date, so a pre-start
-- measure only uses archives dated at least one calendar day before the cutoff day:
--   pre window   = archive dates cutoff-5 .. cutoff-1   (5 archive days)
--   base window  = archive dates cutoff-65 .. cutoff-6  (60 archive days before that)
--   same week    = archive dates start .. next_start-1  (Task 4 association only, never a model input)
-- Measures (null when the windows are not fully covered: 5 of 5 pre days, at least 50 of 60 base days):
--   news_abn           ln(mean daily events in pre window / mean daily events in base window)
--   tone_mean          article-weighted mean AvgTone over the pre window
--   conflict_share     share of pre-window events with QuadClass 3 or 4
--   news_abn_same_week same as news_abn but for the outcome week (labelled same-week)
-- GDELT covers Jul 2015 to Jun 2017 only; weeks outside it get no row (consumers LEFT JOIN on start_date).

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
LOCATION '${hivevar:root}/features/${hivevar:run_id}/weekly_news';

INSERT OVERWRITE TABLE features.weekly_news
SELECT
    a.start_date,
    CASE
        WHEN a.pre_days = 5 AND a.base_days >= 50 AND a.pre_events > 0
            THEN LN((a.pre_events / a.pre_days) / (a.base_events / a.base_days))
    END AS news_abn,
    CASE WHEN a.pre_days = 5 AND a.pre_articles > 0 THEN a.pre_tone_x_articles / a.pre_articles END AS tone_mean,
    CASE WHEN a.pre_days = 5 AND a.pre_events > 0 THEN a.pre_conflict / a.pre_events END AS conflict_share,
    CASE
        WHEN a.same_days = a.n_calendar_days_same AND a.base_days >= 50 AND a.same_events > 0
            THEN LN((a.same_events / a.same_days) / (a.base_events / a.base_days))
    END AS news_abn_same_week,
    CAST(a.pre_days AS INT) AS n_days_pre,
    CAST(a.base_days AS INT) AS n_days_base,
    CAST(a.same_days AS INT) AS n_days_same_week
FROM (
    SELECT
        w.start_date,
        DATEDIFF(w.next_start_date, w.start_date) AS n_calendar_days_same,
        SUM(CASE WHEN g.d BETWEEN DATE_SUB(w.cutoff_date, 5) AND DATE_SUB(w.cutoff_date, 1) THEN 1 ELSE 0 END)
            AS pre_days,
        SUM(CASE WHEN g.d BETWEEN DATE_SUB(w.cutoff_date, 5) AND DATE_SUB(w.cutoff_date, 1) THEN g.n_events ELSE 0 END)
            AS pre_events,
        SUM(CASE WHEN g.d BETWEEN DATE_SUB(w.cutoff_date, 5) AND DATE_SUB(w.cutoff_date, 1) THEN g.n_conflict ELSE 0 END)
            AS pre_conflict,
        SUM(
            CASE WHEN g.d BETWEEN DATE_SUB(w.cutoff_date, 5) AND DATE_SUB(w.cutoff_date, 1) THEN g.sum_articles ELSE 0 END
        ) AS pre_articles,
        SUM(
            CASE
                WHEN g.d BETWEEN DATE_SUB(w.cutoff_date, 5) AND DATE_SUB(w.cutoff_date, 1)
                    THEN g.sum_tone_x_articles
                ELSE 0
            END
        ) AS pre_tone_x_articles,
        SUM(CASE WHEN g.d BETWEEN DATE_SUB(w.cutoff_date, 65) AND DATE_SUB(w.cutoff_date, 6) THEN 1 ELSE 0 END)
            AS base_days,
        SUM(CASE WHEN g.d BETWEEN DATE_SUB(w.cutoff_date, 65) AND DATE_SUB(w.cutoff_date, 6) THEN g.n_events ELSE 0 END)
            AS base_events,
        SUM(CASE WHEN g.d BETWEEN w.start_date AND DATE_SUB(w.next_start_date, 1) THEN 1 ELSE 0 END) AS same_days,
        SUM(CASE WHEN g.d BETWEEN w.start_date AND DATE_SUB(w.next_start_date, 1) THEN g.n_events ELSE 0 END)
            AS same_events
    FROM features.calendar_weeks AS w
    CROSS JOIN (
        SELECT
            CAST(archive_date AS DATE) AS d,
            n_events,
            n_conflict,
            sum_articles,
            sum_tone_x_articles
        FROM raw.gdelt_daily_audit
        WHERE n_corrupt_archive = 0 AND n_events > 0 AND archive_date != '_unknown'
    ) AS g
    WHERE
        w.next_start_date IS NOT NULL
        AND g.d BETWEEN DATE_SUB(w.cutoff_date, 65) AND DATE_SUB(w.next_start_date, 1)
    GROUP BY w.start_date, w.next_start_date
) AS a;

-- Coverage check: weeks with a usable pre-start measure, per split.
SELECT
    w.split,
    COUNT(*) AS n_weeks,
    SUM(CASE WHEN n.news_abn IS NOT NULL THEN 1 ELSE 0 END) AS n_weeks_with_news_abn,
    MIN(CASE WHEN n.news_abn IS NOT NULL THEN w.start_date END) AS first_week_with_news,
    MAX(CASE WHEN n.news_abn IS NOT NULL THEN w.start_date END) AS last_week_with_news
FROM features.calendar_weeks AS w
LEFT JOIN features.weekly_news AS n ON w.start_date = n.start_date
GROUP BY w.split
ORDER BY first_week_with_news;
