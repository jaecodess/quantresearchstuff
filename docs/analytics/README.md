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

## EMR results
Pending the EMR run (aws_task4.md E2).
