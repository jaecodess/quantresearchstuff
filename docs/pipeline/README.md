# Task 2 data pipeline

How raw prices (and GDELT) become `features.weekly`, which engine does what, and how to run it. Rules come from HANDOVER sections 6 to 9; decisions taken while building are in [decision-log.md](../decisions/decision-log.md) (D-015 onwards). Development-run results are in [dev_run_2026-11-04](dev_run_2026-11-04/reconciliation_local-dev.json); the assessed run is on EMR ([aws_task2.md](aws_task2.md)).

## Stages and engines

| # | Stage | Engine | Code | Output (HDFS under `/inf2006`) | Hive table |
|---|---|---|---|---|---|
| 1 | Price audit per ticker: rows, dates, missing, invalid, OHLC, zero volume, duplicates, overnight jumps | Hadoop Streaming | `streaming/price_audit/` | `staging/streaming/<run_id>/price_audit/` | (evidence log) |
| 2 | Cleaning: validity rules, every raw row to clean or rejected with a reason | PySpark | `spark/clean_prices.py` | `clean/prices_daily/<run_id>/`, `rejected/prices_daily/<run_id>/` | `clean.prices_daily`, `clean.prices_rejected`, `raw.prices_csv` (`hive/20_prices.sql`) |
| 3 | Market calendar, weekly start/cutoff dates, split assignment | **Hive SQL** | `hive/30_calendar_weeks.sql` | `features/<run_id>/market_calendar`, `calendar_weeks` | `features.market_calendar`, `features.calendar_weeks` |
| 4 | Split screening (price and volume rules) | PySpark | `spark/split_detection.py` | evidence CSV | |
| 5 | Reviewed split events | manual review + Hive | `config/split_events.csv`, `hive/31_split_events.sql` | `config/split_events/<run_id>/` | `clean.split_events` |
| 6 | Universe, labels, 9 features, within-week ranks | PySpark | `spark/weekly_features.py` | `features/<run_id>/weekly/split=*` | `features.weekly` (`hive/40_features_weekly.sql`) |
| 7 | GDELT daily aggregates, one mapper per ZIP | Hadoop Streaming | `streaming/gdelt_daily/` | `staging/streaming/<run_id>/gdelt_daily/` | `raw.gdelt_daily_audit`, `clean.gdelt_daily` (`hive/10_gdelt_daily.sql`) |
| 8 | Weekly market-wide news measures (conservative timing) | **Hive SQL** | `hive/41_weekly_news.sql` | `features/<run_id>/weekly_news` | `features.weekly_news` |
| 9 | Reconciliation and split report | PySpark | `spark/reconcile.py` | evidence JSON/CSV | |

Every run writes to new `<run_id>` folders; Spark jobs refuse to overwrite. Hive tables are external, so dropping them never deletes data, and all DDL is in versioned scripts (D-013). Spark reads Hive tables through the metastore (`enableHiveSupport`).

## Rules in brief

- **Cleaning** (first reason wins): wrong field count, missing ticker, bad date, missing or non-numeric value, price ≤ 0, negative volume, high < low, open or close outside [low, high], duplicate (ticker, date) (all copies rejected), outside the study window 2013-02-08 to 2017-12-29. No repairs, no filling. Zero volume alone is kept.
- **Calendar**: a market day has bars for at least half of the series. A week starts on the first market day on or after a Friday; the cutoff is the market day before; the outcome runs open-to-open to the next start. Split by start year (2013-15 train, 2016 validation, 2017 test); a week whose outcome ends in the next year is `boundary_excluded`.
- **Eligible stock-week**: bar on the cutoff day with 61 consecutive clean market-day closes ending there (a gap anywhere in the lookback makes it ineligible; nothing is filled), clean opens on both start dates, and no confirmed split in (cutoff − 60, next start] other than exactly on the start date.
- **Label**: `ratio = open(next start) / open(start)`; label 1 if above that week's median among eligible stocks. `ratio` and `label` are never features.
- **Features** (all as of the cutoff close): `rev_1w`, `mom_4w`, `mom_12w`, `mom_12w_skip`, `vol_20d`, `vol_ratio`, `max_move_20d`, `rel_volume`, `dollar_volume`. F1 = mom_4w, mom_12w, vol_20d, rel_volume. F2 = the other eight-feature set in HANDOVER 7.4. `<feature>_rank` = percent rank within the week among eligible stocks (fits nothing, so no leakage across splits).
- **News** (market-wide, same value for every stock in a week): pre-start window = archives dated cutoff−5 … cutoff−1; baseline = cutoff−65 … cutoff−6; `news_abn_same_week` uses the outcome week and is for Task 4 association only.

## `features.weekly` contract (for MODEL and ANLY)

Grain: ticker × week, all train / validation / test weeks, partitioned by `split`. Use `WHERE eligible`.

| Column | Meaning |
|---|---|
| `ticker`, `start_date`, `split` | key and split |
| `cutoff_date`, `next_start_date` | last feature day; outcome end |
| `eligible`, `ineligible_reason` | universe flag and the first failed rule |
| `ratio`, `label` | source price ratio and weekly label (**never features**) |
| 9 raw features | see above; null when not eligible |
| 9 `*_rank` columns | within-week percent ranks, 0 to 1 |
| `n_eligible_week`, `run_id` | eligible stocks that week; provenance |

## Development-run results (local, full price file; EMR rerun is the evidence)

- Prices: 619,040 raw rows = 605,887 clean + 13,153 rejected (13,130 outside the window, 23 invalid). Clean count equals the brief's 605,887 accepted bars.
- Calendar: 1,233 market days; 148 train, 52 validation, 51 test weeks; 4 boundary weeks; 9 holiday-Friday weeks, matching HANDOVER 7.1 exactly.
- Eligible stock-weeks: train 65,086 (from 2013-05-10; the first 13 weeks lack 61 closes), validation 25,720, test 25,531; 471 to 504 eligible stocks per week; label share 0.4995 to 0.4997.
- Exclusions: missing history or gaps 6,474; no bar on the cutoff day (late starters) 3,905; missing open 11; split in window 28 (DISCA/DISCK, train only).
- Hadoop Streaming audit agrees with the PySpark profile: 505 tickers, 619,040 rows, 11 missing, 12 inconsistent OHLC, 32 overnight jumps.
- Features checked against an independent recomputation from the raw CSV for AAPL 2016-06-10 and GOOG 2017-03-17 (max difference 6e-17).

## Running

- EMR (assessed): `bash spark/run_task2.sh <run_id> <raw_prices_hdfs_dir>`; steps in [aws_task2.md](aws_task2.md).
- Local development: `bash spark/run_task2_local.sh <work_dir> <price_csv> [ticker_mod]` (Hive SQL runs through Spark SQL; no Streaming or GDELT). `ticker_mod 10` gives a dev profile of about 1/10 of the tickers.
- Tests: `make streaming` (Streaming jobs), `make spark-test` (includes `tests/pipeline/test_pipeline_local.py`, synthetic data with a planted split, gap, late starter and bad rows).
