# Task 4: analytics

Code: [`analytics/`](../../analytics/) (Hive SQL, PySpark, charts, EMR and local runners). EMR steps: [aws_task4.md](aws_task4.md). Rules: HANDOVER sections 8.5, 8.9 and 10. Report draft: [section5_draft.md](section5_draft.md).

All analyses read the **stored** predictions of the frozen model (`lr_f1_r001`, Task 3 run `20261106-1400-full-ref`). Nothing is refitted, retuned or reselected, so the single test evaluation stays single. Row-level work stays in Hive or Spark; only week-level tables (at most a few hundred rows) reach the driver, are bootstrapped and plotted. Weeks are the unit: every interval is a 10,000-draw week bootstrap with seed 2026 (same code as Task 3).

## The three questions and the tool for each

| # | Question | Files | Tool and why |
|---|---|---|---|
| Q1 | When is volume unusually high or low for a stock, and how does that relate to next-week source price ratios? | `20_q1_volume_quintiles.sql` → `analytics.q1_weekly` | **Hive**: one window (`NTILE(5)` per week) and a `GROUP BY` over the full ticker-week table; plain SQL, runs distributed, output is week × quintile. |
| Q2 | Where is the model most and least reliable compared with B3? | `run_analytics.py` (`q2_weekly`, `q2`) | **PySpark**: needs tie-aware weekly AUC per subgroup (rank windows) and reuses the Task 3 evaluation module (`evaluation/metrics.py`), so the numbers are computed exactly as in Task 3. |
| Q3a | Do high-news weeks show different market volatility or dispersion? | `30_q3a_news_market.sql` → `analytics.q3a_weekly` | **Hive**: one `GROUP BY` per week joined to `features.weekly_news`, terciles by window functions. |
| Q3b | Is the model more or less accurate in high-news weeks? | `run_analytics.py` (`q3b`) | **PySpark**: same weekly AUC code as Q2, conditioned on `news_abn` terciles. |

Charts: `analytics/plots.py` reads only the aggregated CSVs and writes `evidence/analytics/fig_*.png`.

## Definitions
- **Q1** `rel_volume` = ln(mean volume over the 5 days to the cutoff / mean over 60 days), known before the week starts. Per week and quintile: share above the week's median (label 1) and mean of ln(ratio) minus the week's median ln(ratio), a source-price-ratio difference, not a return. Reported per split; the paired weekly Q5 − Q1 difference has its own interval.
- **Q2** groups: calendar quarter; within-week tercile of `vol_20d_rank` (AUC computed inside each tercile, about 170 stocks per week); market-dispersion tercile = sd of ln(ratio) across stocks that week, terciles within each year. 2016 validation and 2017 test are reported separately; a pattern counts only if it points the same way in both (`same_sign_2016_2017`). Dispersion is measured in the outcome week, so it describes the conditions the model faced; it is not something the model could know in advance.
- **Q3a** per week: dispersion (sd of ln ratio), market move |mean ln ratio| (both outcome week) and mean `vol_20d` (pre-start). Terciles of `news_abn` (pre-start, archives dated at least one day before the cutoff) and, separately labelled, `news_abn_same_week`. About 100 weeks, Jul 2015 to Jun 2017. High minus low tercile with an unpaired week bootstrap, and Spearman correlation over all weeks.
- **Q3b** out-of-sample weeks with GDELT coverage (2016 and Jan to Jun 2017, about 76) split into `news_abn` terciles; model and B3 mean weekly AUC, paired difference and rank IC, pooled and per year. Expect wide intervals.
- **ANLY-05** (optional interaction experiment): not run. It would train new models and score Jan to Jun 2017 a second time, which conflicts with evaluating the test set once.

## Outputs (`evidence/analytics/`)
`task4_<run_id>.json` (all summaries, application ID, git commit), `q1_summary`, `q1_weekly`, `q2_summary`, `q2_weekly`, `q2_dispersion_weekly`, `q3a_summary`, `q3a_weekly`, `q3b_summary` (CSV, `_<run_id>`), `task4_<run_id>.log`, `ids_task4_<run_id>.txt`, `timings_task4_<run_id>.csv`, `fig_q1_volume.png`, `fig_q2_subgroups.png`, `fig_q3a_news_market.png`, `fig_q3b_news_auc.png`.

## Tests
`tests/analytics/test_run_analytics.py` runs the whole chain (Hive SQL through Spark SQL, then PySpark) on synthetic data with three planted patterns and checks each analysis finds its pattern, that week and row counts add up, and that no output file has row-level (ticker) data. Run with `pytest -m spark tests/analytics`.

## Development run (local, not evidence)
Local Spark on the full price features from the Task 2 dev run, with a stand-in score on train and validation only (no model, no 2017 predictions) and a synthetic GDELT table: the whole chain runs in about 70 seconds and the charts render. Q1 on the real 2013-15 features (135 weeks): no clear difference between volume quintiles (mean weekly excess ln ratio between -0.0004 and +0.0005; Q5 - Q1 = -0.0008 [-0.0019, +0.0003]). The EMR run is the evidence.

## EMR results (run `20261110-1000-full-ref`, assessed evidence)

Inputs: features from Task 2 run `20261105-1430-full-ref` and predictions from Task 3 run `20261106-1400-full-ref`, both read from S3. Commit `5dcccf6`, application `application_1794278999744_0007` (PySpark) plus 9 Hive queries (`evidence/analytics/ids_task4_20261110-1000-full-ref.txt`). About 4 minutes in total (`timings_task4_...csv`). Check: Q2 "test, all weeks" reproduces Task 3 exactly (+0.0143 [-0.0084, +0.0364]).

**Q1 volume quintiles** (`fig_q1_volume.png`). Share of stocks finishing above the weekly median, busiest (Q5) minus quietest (Q1) quintile, paired by week:
- train 2013-15 (135 weeks): -0.015 [-0.031, +0.001]
- validation 2016 (52 weeks): -0.010 [-0.035, +0.015]
- test 2017 (51 weeks): -0.023 [-0.046, +0.001]

Unusually busy stocks did slightly worse next week than unusually quiet ones in all three periods (about 49% vs 51% above the median), but every interval includes 0. Mean excess ln ratio, Q5 - Q1: -0.0009, -0.0004, -0.0015 (all intervals include 0). About 100 stocks per quintile per week.

**Q2 subgroups** (`fig_q2_subgroups.png`). Paired weekly AUC, model minus B3, 2016 / 2017:
- all weeks: -0.001 / +0.014
- by quarter: Q1 -0.027 / -0.010, Q2 +0.001 / +0.038, Q3 +0.027 / +0.001, Q4 -0.009 / +0.029 (12 to 14 weeks each)
- by stock volatility tercile (about 8,500 rows per year each): low -0.008 / +0.015, mid -0.010 / +0.014, high +0.007 / +0.018
- by market dispersion tercile (17 or 18 weeks each): low +0.005 / +0.010, mid +0.034 / +0.014, high -0.043 / +0.019

Every interval includes 0. Only high-volatility stocks and low and mid dispersion weeks have the same sign in both years, and the differences between groups are much smaller than the intervals. **Finding: no subgroup where the model is reliably better or worse than B3.** This is consistent with the Task 3 null result.

**Q3a news and market measures** (`fig_q3a_news_market.png`). The analysis covers 95 weeks (Jul 2015 to Jun 2017), with about 32 weeks per tercile. High minus low news tercile:
- pre-start news:
  - dispersion +0.0049 [+0.0005, +0.0093] (0.030 to 0.035), Spearman 0.20
  - market move +0.0023 [-0.0044, +0.0091]
  - pre-start 20-day volatility +0.0037 [+0.0020, +0.0054], Spearman 0.37
- same-week news (association only):
  - dispersion +0.0051 [+0.0012, +0.0091]
  - market move +0.0072 [+0.0021, +0.0126] (0.008 to 0.015)

Weeks with more abnormal news showed more dispersion between stocks. Weeks with abnormal news inside the outcome week also showed larger market-wide moves. This is an association between two time series over 95 weeks. It does not show that news causes either, and both may follow the same market events.

**Q3b model by news tercile** (`fig_q3b_news_auc.png`). 78 out-of-sample weeks, 26 per tercile. Mean weekly AUC, model / B3:
- low news: 0.515 / 0.501, paired +0.014 [-0.029, +0.055]
- mid news: 0.499 / 0.508, paired -0.009 [-0.037, +0.020]
- high news: 0.491 / 0.483, paired +0.007 [-0.030, +0.047]

All intervals include 0.5 for AUC and 0 for the paired difference. By year, the paired difference changes sign in the low and high terciles (e.g. high news: 2016 +0.019, 2017 -0.019). The terciles are also unevenly split across years (2017 has 13, 5 and 8 weeks), so the per-year figures rest on very few weeks. **Finding: no evidence that model accuracy depends on news activity.**
