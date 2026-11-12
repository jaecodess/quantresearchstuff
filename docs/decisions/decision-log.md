# Decision log

Append decisions; do not rewrite history. Status: `PROPOSED` (from review, not yet confirmed), `ACCEPTED`, `REJECTED`. Record the date and who agreed. TEAM-05 is done when no row says PROPOSED.

## Items from HANDOVER §3 and §6 to §9

| ID | Item | Handover | Status | Decided | By |
|---|---|---|---|---|---|
| D-001 | F2: 8 indicators in four families, fixed standard windows, validation never tunes windows | §7.4 | PROPOSED | | |
| D-002 | Within-week percentile-rank features | §7.5 | PROPOSED | | |
| D-003 | Baselines B2 (1-week reversal) and B3 (best single-indicator rule); headline comparison is vs B3 | §8.1 | PROPOSED | | |
| D-004 | Weeks as the unit of analysis; week-level bootstrap intervals; paired difference vs B3 | §8.5 | PROPOSED | | |
| D-005 | Split detection rule and reviewed `config/split_events.csv`; stock-weeks crossing a split are ineligible | §6.2, §7.2 | PROPOSED | | |
| D-006 | Pre-declared 8-config grid and one-standard-error simplicity rule | §8.2, §8.3 | PROPOSED | | |
| D-007 | Pre-registered success rule | §8.6 | PROPOSED | | |
| D-008 | GDELT is not a main-model feature; conditional evaluation across `news_abn` terciles | §8.9 | PROPOSED | | |
| D-009 | Conservative GDELT timing rule (archives at least one calendar day before the cutoff day) | §8.9 | PROPOSED | | |
| D-010 | No expanded GDELT copy in HDFS; one mapper per archive via NLineInputFormat | §5, §9 | PROPOSED | | |
| D-011 | Task 4 analyses that could influence choices run on training data only until the model is frozen | §7.6 | PROPOSED | | |
| D-012 | Survivorship check reported either way | §5 | PROPOSED | | |
| D-013 | Hive DDL kept in versioned scripts (metastore may vanish with the cluster) | §9 | PROPOSED | | |
| D-014 | Quarters and terciles instead of months for subgroup analysis; a subgroup finding needs the same direction in 2016 and 2017 | §8.5 | PROPOSED | | |

## Teaching-team answers (HANDOVER §16)

| # | Question | Answer | Date |
|---|---|---|---|
| 1 | Output weights format; assessed workload | | |
| 2 | EMR release, permitted configuration changes, timing policy | | |
| 3 | Does market-wide GDELT processing count as meaningful pipeline use? | | |
| 4 | Full GDELT run = all 731 archives, or documented subset? | | |
| 5 | Where and how to acquire the price snapshot; version identifier | | |
| 6 | External data allowed? (current plan: no) | | |
| 7 | Appendix lettering (AI use C vs contributions D) | | |

## Gate 1: grid (MODEL-03). Lock BEFORE running validation

Pre-declared grid (HANDOVER §8.2). Confirm or edit, then fill "Locked":

- LogisticRegression on F1 and F2, `regParam` in {0.0, 0.01}: 4 configs
- RandomForestClassifier on F1 and F2, `numTrees` = 200, `maxDepth` in {4, 8}, fixed `seed`: 4 configs

Inputs: the within-week `*_rank` columns of the feature set (F1: mom_4w, mom_12w, vol_20d, rel_volume; F2: rev_1w, mom_4w, mom_12w_skip, vol_20d, vol_ratio, max_move_20d, rel_volume, dollar_volume). Seed 2026 for every random step. Pipeline: Imputer (median) → VectorAssembler → StandardScaler → model, fitted on 2013-2015 train rows only.
Simplicity order for the one-standard-error rule (simplest first), with config IDs: LR F1 regParam 0.01 (`lr_f1_r001`), LR F1 0.0 (`lr_f1_r0`), LR F2 0.01 (`lr_f2_r001`), LR F2 0.0 (`lr_f2_r0`), RF F1 depth 4 (`rf_f1_d4`), RF F1 depth 8 (`rf_f1_d8`), RF F2 depth 4 (`rf_f2_d4`), RF F2 depth 8 (`rf_f2_d8`). Standard error = standard deviation of the best config's weekly validation AUCs / sqrt(number of weeks); the simplest config within one SE of the best is chosen.

Locked on: 2026-11-05 by: @husyGIT in commit: (the commit that adds this line, on branch ws/model/task3, before any validation run)

## Gate 2: frozen model and success rule (MODEL-06). Lock BEFORE the test set is touched

Frozen configuration: `lr_f1_r001` (selected on 2016 mean weekly AUC; simplicity rule applied: the best mean was `rf_f2_d8`, and `lr_f1_r001` is the simplest config within one standard error of it, SE = 0.0070; EMR validation run 20261106-1400-full-ref, 2026-11-08)

Success rule (HANDOVER §8.6): the frozen model shows evidence of predictive signal only if, on 2017 test, (1) its mean weekly AUC has a 95% interval above 0.5, and (2) the mean paired difference against B3 has a 95% interval above 0. Otherwise the finding is "no evidence that the model adds signal beyond simple rules".

Locked on: 2026-11-08 by: @husyGIT in commit: (the commit that adds this line, before any test run)

## Other decisions

<!-- Newest last. Format: D-0xx · date · @handle · decision · why · affected tasks -->
- D-015 · 2026-11-04 · @jaecodess · ACCEPTED: core storage is final at one 100 GiB gp3 volume per core node (3,000 IOPS, 125 MiB/s), 2 core nodes = 200 GiB physical, about 100 GiB logical HDFS at replication 2, before HDFS overhead and YARN shuffle/temp space. Supersedes the 200 GiB per node in HANDOVER §2 and §9. · Brief update for Learner Lab limits (logged by @husyGIT, 2026-11-03). GDELT ZIPs (~19.2 GB physical) still fit; an expanded GDELT copy (~109 GB physical) does not, reinforcing D-010. Hardware stays fixed across all timed runs. · CORP-06, CORP-07, CORP-11, PERF-04, PERF-10
- D-016 · 2026-11-04 · @husyGIT · Rows after 2017-12-29 (13,130) are rejected as `outside_study_period` in cleaning, so `clean.prices_daily` holds exactly the brief's 605,887 accepted bars and raw = clean + rejected still holds · The file runs to 2018-02-07; no outcome or feature may use data past the test period · PIPE-04, PIPE-10
- D-017 · 2026-11-04 · @husyGIT · Duplicate (ticker, date) rows: every copy is rejected (`duplicate_key`), none is picked; zero volume alone is kept · No basis to choose between copies; none occur in the current file · PIPE-04
- D-018 · 2026-11-04 · @husyGIT · Eligibility needs 61 *consecutive* market-day closes ending on the cutoff day; a gap anywhere in the lookback makes the stock-week ineligible (no filling) · Rolling windows over gapped series would silently mix periods · PIPE-07
- D-019 · 2026-11-04 · @husyGIT · Split assignment by start year; a week whose outcome ends in the next year is `boundary_excluded` (4 weeks); the last week has `no_outcome` · Implements HANDOVER 7.6 "outcome ends within that period" · PIPE-06
- D-020 · 2026-11-04 · @husyGIT · Split review: DISCK and DISCA 2014-08-07 confirmed (2-for-1 share distribution, verify in filings); VRTX 2013-04-19, VRTX 2014-06-24 and BBY 2014-01-16 rejected (volume moves contradict a split). Screening flags moves within 5% of a split ratio for review, because the 3% price rule and the volume rule each miss one Discovery class · Evidence in config/split_events.csv and the split screen CSV · PIPE-05
- D-021 · 2026-11-04 · @husyGIT · Calendar and weekly news are computed in Hive SQL (hive/30, hive/41); cleaning, splits, universe, labels and features in PySpark · Gives Hive real transformation work, not only table registration (brief Task 2) · PIPE-06, PIPE-11
- D-022 · 2026-11-04 · @husyGIT · Additive columns on section 9 contracts (no column removed or renamed): `features.calendar_weeks` + anchor_friday, start_is_friday, start_idx, cutoff_idx, next_idx; `features.weekly` + cutoff_date, next_start_date, ineligible_reason, n_eligible_week, run_id; `clean.prices_rejected` + raw_line; `features.weekly_news` + n_days_pre, n_days_base, n_days_same_week · Provenance and auditability; consumers selecting contract columns are unaffected · PIPE-01, MODEL, ANLY
- D-023 · 2026-11-12 · @jaecodess · PROPOSED: timed PERF runs (`perf/run_pipeline.sh`) cover Task 2 (all stages), Task 3 for the Gate 2 frozen config only (fit on train, score, write predictions and weights, fresh-session reload check, Hive model tables) and Task 4. The 8-config validation grid is not rerun: selection is finished and is not production work. Each run refits and rescores the frozen model under its own model root (`<root>/perf/<run_id>`) and marker directory, so the official Task 3 test marker and results in `evidence/model/` are never touched; nothing in a PERF run selects or tunes anything, and every run's outputs are checked against the first successful reference run of its profile. · The brief requires timed runs to include training and output writes with no reuse of fitted models, and three repetitions per profile/config; the test-once guard in `modeling/run_model.py` would otherwise refuse every repetition. · PERF-02, PERF-06, PERF-07, PERF-09, MODEL-07
- D-024 · 2026-11-12 · @jaecodess · PROPOSED (for TEAM-06): development profile = tickers with crc32(ticker) % 10 == 0 (the `clean_prices.py --ticker-mod` rule, about 1/10 of the 505 series) plus the GDELT archives for one month, January 2016 (`201601*`). The dev price file is derived once into `<root>/profiles/` before timing (untimed, like staging); raw data is unchanged. Full profile = all staged prices and all staged GDELT archives. · Deterministic, matches the dev rule PIPE already used locally, and gives genuine input growth from dev to full for both sources. · TEAM-06, PERF-03, PERF-06
