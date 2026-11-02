# Raw quality findings (CORP-08, CORP-09)

What is wrong or missing in the raw sources, with counts, and what the pipeline must do about it. Raw files are never edited; every handling rule below belongs to PIPE (cleaning, split detection, calendar).

Status: price numbers are from a **local development profile** of the pinned file ([dev_profile_2026-11-02](dev_profile_2026-11-02/summary.json)), produced by `corpus/profile_prices.py`. Rerun the same script on EMR and replace the figures with the EMR output (`evidence/corpus/prices_profile_<run_id>/`); they should be identical because the input bytes are identical (same SHA-256). GDELT numbers come from the `streaming/gdelt_daily` audit job once the archives are staged.

## 1. Daily price CSV

### 1.1 Headline counts *(dev)*

- 619,041 lines = 1 header + 619,040 data rows, 7 fields on every line, header `date,open,high,low,close,volume,Name`.
- 505 series, 2013-02-08 to **2018-02-07**. The brief's window ends 2017-12-29: **13,130 rows** (about 5.5 weeks) fall after it.
- 605,910 rows inside the study window. After removing the 23 rows with a hard problem below, **605,887** remain, exactly the brief's "accepted bars". The brief's acceptance rule therefore matches ours: drop rows with missing prices or inconsistent OHLC, keep everything else.

| Step | Rows |
|---|---|
| Data rows in file | 619,040 |
| minus rows after 2017-12-29 | −13,130 |
| = rows in study window | 605,910 |
| minus rows with a hard problem (all 23 are inside the window) | −23 |
| = accepted bars (matches brief) | **605,887** |

### 1.2 Problems found *(dev)*

| Check | Rows | Example | Pipeline handling (PIPE-04) |
|---|---|---|---|
| Missing `open` | 11 | `2015-07-17,,48.49,47.85,47.92,1246786,ES` | Reject (reason `missing_open`) |
| Missing `high` and `low` (and open) | 8 | `2016-01-12,,,,88.55,0,DHR` | Reject. 4 of these also have volume 0: looks like a placeholder row on a day with no trading |
| `close` outside `[low, high]` | 9 | `2016-05-19,35.19,35.785,35.085,17.87,1231722,LNT` (close is about half of every other price that day) | Reject; do not repair |
| `open` outside `[low, high]` | 4 | `2014-05-19,18.77,33.9525,33.695,33.895,1078888,CHD` | Reject |
| `high < low` | 1 | `2014-05-19,23.89,23.595,23.85,24.445,1944854,AOS` | Reject |
| Rows with any hard problem | **23** | full list: [problem_rows.csv](dev_profile_2026-11-02/problem_rows.csv) | Reject, keep in `clean.prices_rejected` with the reason |
| Zero volume | 4 | all 4 are also missing prices | Rejected anyway |
| Duplicate (ticker, date) | 0 | | Rule still needed: keep none silently, report |
| Non-numeric values, bad dates, wrong field count | 0 | | Rule still needed for reruns |
| Price ≤ 0, negative volume | 0 | | |

### 1.3 Coverage and gaps *(dev)*

- Full grid inside the study window would be 505 series × 1,233 market days = 622,665 bars; 605,910 present ≈ 97.3%. Almost all of the shortfall is series that start late, not holes.
- **29 series start after 2013-02-08** (e.g. GOOG 2014-03-27, PYPL 2015-07-06, HPE 2015-10-19, APTV 2017-12-05). **0 series end early.** Every series runs to the last date in the file.
- **6 series have holes** inside their own date range, 51 missing market days in total: DHR 16, O 16, ES 10, ICE 5, BMY 2, ORCL 2. Some dates are shared (2013-04-26, 2013-09-09/10), which suggests gaps in the source's collection rather than trading halts. Handling: keep the gap (never fill from later data); weeks that need a missing bar become ineligible (PIPE-07).
- No dates where fewer than half of the series trade, so the market calendar is clean.

### 1.4 Survivorship (CORP-08) *(dev)*

Series ending before the last date: **0**. Series starting after the first date: **29**. A list where stocks only ever join and never leave was built from the index members at the end of the period (early 2018). Stocks that were dropped or delisted between 2013 and 2017 are missing. **Survivorship bias is confirmed**, and it goes in the report limitations. It affects the weekly median (computed only over survivors) and makes any result look better than it would have in real time.

### 1.5 Splits and other large overnight moves (input to PIPE-05) *(dev)*

32 overnight moves with |ln(open / previous close)| > 0.25 in rows without a hard problem ([overnight_jumps.csv](dev_profile_2026-11-02/overnight_jumps.csv)):

- The source is **mostly split-adjusted**: AAPL (7-for-1, June 2014) and NFLX (7-for-1, July 2015) show no jump, and their pre-split prices carry 4 decimals (a sign of back-adjustment).
- **But not always**: DISCA and DISCK both halve on 2014-08-07 with volume up, which looks like an unadjusted 2-for-1 split. DISCK matches the price rule (ratio 0.507); DISCA misses the ±3% tolerance (0.521) and needs the volume check.
- The price rule alone also flags VRTX twice (+54% on 2013-04-19, +47% on 2014-06-24), which look like news moves, not reverse splits. Hence the manual review step in HANDOVER section 6.2.
- Most other large drops do not match a split ratio and look like spin-offs or news (for example BAX 2015-07-01, EBAY 2015-07-20, NI 2015-07-02, DHR 2016-07-05, HPE 2017-04-03); verify by hand before recording in `config/split_events.csv`. Spin-offs are not adjusted in the source and stay in the data as genuine price changes.

### 1.6 Limitations the pipeline must handle

1. Rows after 2017-12-29: outside the brief's window. Keep in raw, exclude when building weeks (no outcome may end after the test period).
2. 23 invalid rows: reject with reasons; reconciliation `raw = clean + rejected (+ out-of-window)` must add up.
3. Late starters: need 61 clean closes before they become eligible (§7.2).
4. Holes in 6 series: never forward-fill; affected weeks ineligible.
5. Partial split adjustment: detect, review, exclude affected stock-weeks (D-005).
6. Survivorship: cannot be fixed with these sources; report it.
7. No sector, market cap, currency field or delisting data.

## 2. GDELT 1.0 events

From one real archive inspected locally (2026-10-02 export, outside our window, used only to confirm the format):

- 58 tab-separated fields on every line, no header, no malformed lines, no duplicate event IDs.
- `SQLDATE` differs from `DATEADDED` for 2.0% of events (2,186 of 109,627); 603 are more than 30 days earlier, many exactly 365 days (a parsing artefact). This is why days are assigned by archive date.
- Actor fields are often blank (Actor1 missing on 9.9%, Actor2 on 30.4%); 2.7% of events have no action location.
- One article gives several events (109,627 events from 21,687 URLs).
- Automatic coding errors exist (see data dictionary section 3).

To fill in from the EMR audit run (`hive/10_gdelt_daily.sql` reconciliation, missing-date and overlap queries):

| Measure | Value |
|---|---|
| Archives expected / acquired / failed checksum | 731 / … / … |
| Total lines, events kept, malformed, bad values, duplicates | … |
| Missing archive dates | … |
| Archives with overlapping event-ID ranges | … |
| Share of events with `DATEADDED − SQLDATE` > 30 days | … |
