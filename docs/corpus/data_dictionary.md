# Data dictionary (CORP-04, CORP-05, CORP-10)

Scope, versions, schemas, units, time semantics and relationships for the raw corpus. Numbers marked *EMR* come from the EMR profile of the pinned file (run `20261103-2250-full-ref`, [summary.json](../../evidence/corpus/prices_profile_20261103-2250-full-ref/summary.json)), identical to the earlier local profile.

## 1. Selected sources

| Source | Version / identifier | Where from | Coverage used | Raw size | Role |
|---|---|---|---|---|---|
| Daily price CSV | `plotly-datasets-0c447c4`: git commit `0c447c47b757ad74edecab31f0d72f849d2e67c2`, git blob `2a1f02e3675ad8bb6f3a8e536e15f601cfe32fbf`, SHA-256 `6aea253c…e716cf` | Brief link `https://raw.githubusercontent.com/plotly/datasets/master/all_stocks_5yr.csv`, acquired from the commit-pinned URL so the same bytes can be fetched again | File: 2013-02-08 to 2018-02-07. Study window (brief): 2013-02-08 to 2017-12-29 | 1 file, 29,580,549 bytes, 619,041 lines | Labels and all model features |
| GDELT 1.0 Events | `v1-daily`, one archive per day, `YYYYMMDD.export.CSV.zip`; GDELT's own `md5sums` and `filesizes` lists saved as provenance | `http://data.gdeltproject.org/events/` | 731 archives, 2015-07-01 to 2017-06-30 | 731 files, 9,611,684,528 bytes measured (brief: 9.61 GB ZIP, 54.71 GB expanded) | Market-wide daily news measures; not a model feature (D-008) |
| SEC EDGAR, Wikinews | Stored raw, not analysed | | | | Acquired unchanged into S3 and HDFS with manifests ([aws_aux_sources.md](aws_aux_sources.md)); not used by the pipeline because they link only weakly to individual stocks (HANDOVER section 5) |

Storage: raw files kept byte-for-byte as acquired, in S3 (durable) and copied to HDFS each session. CSV stays CSV and ZIPs stay zipped (no expanded GDELT copy, D-010). Everything downstream is Parquet (PIPE).

```
/inf2006/raw/prices/plotly-datasets-0c447c4/acquired=<YYYY-MM-DD>/all_stocks_5yr.csv
/inf2006/raw/gdelt/v1-daily/acquired=<YYYY-MM-DD>/<YYYYMMDD>.export.CSV.zip
/inf2006/raw/gdelt/v1-daily/acquired=<YYYY-MM-DD>/_provenance/{md5sums,filesizes}
/inf2006/raw/_manifests/<source>_acquired=<YYYY-MM-DD>.csv
```
Source, version and acquisition date are all visible in the path, so two acquisitions never overwrite each other.

## 2. Daily price CSV

Comma-separated, one header line, UTF-8, `\n` line endings. One row = one stock on one trading day.

| Column | Type (raw) | Type (meaning) | Unit | Meaning and notes |
|---|---|---|---|---|
| `date` | text `yyyy-MM-dd` | date | US trading day | Exchange trading date (no time, no time zone). Open is the first trade of that day, close the last |
| `open` | text | decimal | USD per share (assumed; the source does not state currency) | Opening price. 11 blanks *EMR* |
| `high` | text | decimal | USD per share | Highest price of the day. 8 blanks *EMR* |
| `low` | text | decimal | USD per share | Lowest price of the day. 8 blanks *EMR* |
| `close` | text | decimal | USD per share | Closing price. No blanks *EMR* |
| `volume` | text | integer | shares | Shares traded that day. 4 zero values *EMR* |
| `Name` | text | identifier | | Ticker symbol, the series identifier (505 distinct *EMR*). Called `ticker` downstream |

- **Natural key:** (`Name`, `date`). No duplicate keys *EMR*.
- **Price adjustment:** mostly split-adjusted (AAPL 7-for-1 in June 2014 and NFLX 7-for-1 in July 2015 show no jump; pre-split values carry 4 decimals, a sign of back-adjustment), but not always: DISCA and DISCK halve on 2014-08-07. Spin-offs are not adjusted. Dividends: unknown, assume unadjusted. Handled by split detection (PIPE-05).
- **Calendar:** 1,259 market days in the file, 1,233 inside the study window *EMR* (a market day = a date where at least half of the series have a bar). The study window starts and ends on a Friday.
- **Not in the source:** company name, sector, exchange, currency field, adjustment factors, delisted stocks.

## 3. GDELT 1.0 daily event export

Tab-separated, **no header**, 58 fields per line, one line per event, UTF-8. Verified on a real daily archive (109,627 lines, all with 58 fields). Archives from 1 April 2013 onward use this 58-field layout; our window (Jul 2015 to Jun 2017) is entirely within it. Full field list: GDELT 1.0 Event Codebook. Fields used by the pipeline:

| # (1-based) | Field | Type | Meaning and notes |
|---|---|---|---|
| 1 | `GLOBALEVENTID` | integer | Unique event ID; rises over time. Duplicate check key |
| 2 | `SQLDATE` | `YYYYMMDD` | Date the event is said to have happened (parsed from the article; can be wrong, e.g. "last year" gives a date 365 days back) |
| 7, 17 | `Actor1Name`, `Actor2Name` | text | Who acted / was acted on: countries, organisations, roles (e.g. POLICE). Often blank. No tickers |
| 8, 18 | `Actor1CountryCode`, `Actor2CountryCode` | CAMEO 3-letter (e.g. `USA`) | Actor's country |
| 27 | `EventCode` | CAMEO code, 3 to 4 digits | Detailed action type (e.g. 173 = arrest or detain) |
| 29 | `EventRootCode` | CAMEO 2-digit | Broad action type, 01 to 20 (e.g. 04 consult, 19 fight) |
| 30 | `QuadClass` | 1 to 4 | 1 verbal cooperation, 2 material cooperation, 3 verbal conflict, 4 material conflict |
| 31 | `GoldsteinScale` | decimal, -10 to +10 | Theoretical impact of this type of event on stability; fixed per event code |
| 32 to 34 | `NumMentions`, `NumSources`, `NumArticles` | integer | How widely the event was reported (mentions, distinct sources, distinct articles) in that day's coverage |
| 35 | `AvgTone` | decimal, about -100 to +100 | Average tone of the articles: % positive words minus % negative words. Usually -10 to +10 |
| 52 | `ActionGeo_CountryCode` | FIPS 2-letter (e.g. `US`) | Country where the action happened. Note: FIPS here, CAMEO in fields 8 and 18 |
| 57 | `DATEADDED` | `YYYYMMDD` | Date GDELT added the event = the archive's date |
| 58 | `SOURCEURL` | text | First article URL that reported the event |

- **Time semantics:** each archive holds events *added* that day; GDELT publishes it the next morning. Events are assigned to days by `DATEADDED` / archive date, never `SQLDATE` (HANDOVER section 5). For prediction, only archives dated at least one calendar day before the feature cutoff are used (D-009).
- **Unit of a row:** one coded event, not one article. One article usually yields several events (109,627 events from 21,687 URLs in the inspected archive).
- **Coding is automatic** and makes mistakes (e.g. a politician climbing a bridge coded as `Fight`, Goldstein -10). Use counts and averages over many events, not single events.

## 4. Relationships between tables

```
prices (ticker, date) ──── date ────► market calendar (date)
                                            │  week = Friday start date (PIPE-06)
gdelt archive (archive_date) ── archive_date ≤ cutoff_date - 1 day ──► weekly_news (start_date)
                                            │
features.weekly (ticker, start_date) ◄──────┘  joined on start_date only (market-wide)
```
- Prices to GDELT: **by date only**. GDELT has no tickers, so there is no stock-level link; every stock in a week gets the same news measures.
- Price `date` is a trading day; GDELT `archive_date` is a calendar day (weekends included). Weekend archives roll forward into the next trading week's measures.
- Overlap: GDELT covers Jul 2015 to Jun 2017 only (6 of 36 training months, all of validation 2016, first half of test 2017).
