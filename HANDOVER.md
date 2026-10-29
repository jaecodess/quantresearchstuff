# INF2006 Big Data Project — Handover (v2)

- **Last updated:** 2026-10-29 (SGT)
- **Replaces:** v1 handover (volatility-forecasting framing, now parked; see §20).
- **Basis:** the team's plan "INF2006 Project Settings (Final Draft)", 29 Oct 2026, plus fixes for five risks found in review (§3).
- **Origin:** planning chat in the Claude app. No code written yet.
- **Deadline:** one team ZIP by **22 Nov 2026, 23:59 SGT**
- **Project:** *Cross-Sectional Signal Research: Predicting Weekly Relative Stock Performance from Price, Volume and Volatility Indicators*

> **Before you start:** copy the official brief PDF into `docs/brief/` and an export of the Final Draft doc into `docs/decisions/`. This file condenses both; the brief is the source of truth.

Status tags used below:
- `[DECIDED]` agreed by the team (from the Final Draft)
- `[PROPOSED]` improvement from review, not yet confirmed by the team; record in the decision log once accepted
- `[ASK TT]` needs confirmation from the teaching team

---

## 1. Project at a glance

| Item | Value |
|---|---|
| Core data | Daily OHLCV price CSV: 505 series, 605,887 bars, 2013-02-08 to 2017-12-29 |
| Auxiliary data | GDELT: pipeline workload, Task 4 news question, conditional evaluation. **Not a main-model feature** |
| Label | 1 if a stock's next-week source price ratio (next start open ÷ start open) is above the median eligible stock that week, else 0 |
| Features | Two pre-declared sets: the Final Draft's 4 indicators (F1) and an expanded 8 in four families (F2), as within-week percentile ranks |
| Baselines | Majority class; 4-week momentum rule; 1-week reversal rule; best single-indicator rule |
| Models | Logistic regression, random forest (Spark ML Pipeline) |
| Main results | Mean weekly AUC with week-level bootstrap intervals, paired against the strongest baseline; balanced accuracy, precision, recall |
| Output | Top-10% equal weights per week (hypothetical selection, not returns) |
| Splits | Train 2013–2015, validation 2016, test 2017 (chronological, fixed by brief) |
| Stack | AWS EMR, HDFS, Hadoop Streaming, Hive, PySpark / Spark ML |

---

## 2. The brief, condensed

### Task 1 — Build the corpus (15 marks)
- Build a **traceable raw-data corpus in HDFS on AWS EMR**. Retain original records; distinguish sources, versions, acquisition dates. Justify storage formats and organization.
- Deliver: documented scope and **data dictionary** (sources, versions, date coverage, native identifiers, schemas, units, time semantics), explicit relationships between tables.
- Investigate coverage, missing/incomplete records, duplicates, corrupted fields. Identify limitations the pipeline must handle.
- Deliver a **raw-corpus inventory + reproducibility record**: source references, acquisition times, **checksums**, configuration, evidence the inputs can be re-acquired. Measured rows/bytes and a quality summary.
- Not required to use all datasets, fields or entries in later tasks.

Available sources (reference sizes from the brief):

| Source | Scope |
|---|---|
| Daily price CSV | 505 series; 605,887 accepted bars; 8 Feb 2013 – 29 Dec 2017 |
| GDELT | 731 daily archives, Jul 2015 – Jun 2017; 144.2M accepted events; 9.61 GB ZIP / 54.71 GB expanded |
| SEC EDGAR | Fixed inventory of 311 filings (156 10-K, 155 10-Q) from 303 issuers, 2013–2017; 2.78 GB |
| Wikinews | Two October 2026 history dumps, ~662 MB compressed; filter to 2013–2017 revisions |

### Task 2 — Build the data pipeline (20 marks)
- Use **Hadoop Streaming, HDFS, Hive and PySpark**, each with a **meaningful contribution**:
  - Hadoop Streaming: source processing and audit summaries
  - Hive: table registration and SQL transformations (**Spark SQL alone does not count as Hive**)
  - PySpark: distributed cleaning, integration, feature preparation (**no collecting financial tables into pandas**)
- Justify handling of missing values, duplicates, inconsistent/corrupted records, rejected data. **Reconcile source vs. prepared row counts.** Keep raw, cleaned and rejected data distinct.
- Validate prices and volumes (valid, internally consistent). **Preserve genuine gaps**; never fill using future observations. Exclude identifiers and target-revealing information from features.
- **Chronological splits:** 2013–2015 train, 2016 validation, 2017 test. Report date coverage, row counts, exclusions per split. No random shuffling across periods.
- **Leakage:** features use only information available at the prediction date; a target is included only when its full outcome is known **within that period**. Fit imputation/scaling on train only. Document treatment near split boundaries.
- **Execution evidence:** unique job/query IDs, traceable inputs/outputs, all mapper/reducer scripts, job configs and output schemas sufficient to reproduce. Hive tables must be usable by Spark.

### Task 3 — Train and evaluate a model (20 marks)
- Define the question, target, horizon, and how predictions are **converted into the required output weights**.
- Learn preprocessing and parameters on train; select on validation; **freeze before test**; never tune on test.
- Compare against a **simple, clearly defined baseline** on the same observations; report handling of unavailable targets.
- Compare a **manageable** set of model/feature choices. For auxiliary sources, establish when information became available.
- Save the **complete Spark ML PipelineModel** (with learned preprocessing). In a **fresh session**, reload it and reproduce predictions without retraining; report differences against a stated numerical tolerance.

### Task 4 — Data analytics (15 marks)
- Use Hive and/or PySpark on prepared data and model outputs; explain tool choice per analysis.
- Labelled visualizations, interpreted findings, evidence-linked conclusions. Distinguish association from causation. Discuss limitations and uncertainty.
- Full-data processing stays distributed; plotting libraries only for aggregated results.
- Compare model results with the Task 3 baseline.
- Report source-price-ratio/model diagnostics **without treating them as economic returns or proof of investability**.

### Cloud resource constraints (provisional until confirmed) `[ASK TT]`

| Setting | Provisional reference |
|---|---|
| EMR release | emr-7.10.0 |
| Cluster | 1 primary + 2 core **m5.xlarge**; no task nodes, no autoscaling |
| Worker storage | One 200 GiB gp3 volume per core node (3,000 IOPS, 125 MiB/s) |
| HDFS replication | `dfs.replication=2` (set explicitly) |
| Spark | 2 executors × 2 cores × 6 GiB heap; **dynamic allocation off** |
| Memory allowance | 2 GiB overhead per executor; 2 GiB driver heap |

- 400 GiB physical worker storage = nominal **200 GiB logical HDFS ceiling** at replication 2, before overhead and working space. Do not count primary/root storage. Compressed ≠ expanded sizes.
- Compare the reference config with **one justified alternative** on identical input manifests and output checks. Keep hardware, release, replication and storage performance fixed when testing software changes; evaluate storage-size changes separately.
- Local development is allowed but **is not evidence** of the assessed EMR run. Develop on a small sample until the teaching team confirms the workload.

### Runtime comparison rules
- Timing starts **after raw inputs are staged in HDFS**. Exclude (and report separately): downloading, S3→HDFS copy, dependency setup, cluster startup.
- Include: parsing/decompression from staged inputs, Hadoop Streaming, Hive transformations, feature prep, training, required output writes and checks. No reusing prepared features or fitted models to skip timed work.
- Compare **development vs. full profiles** with genuine input growth. **Three repetitions** per profile/config: all runs, median, range, stage timings, bytes/rows, resource settings, measured memory and disk use, failures.
- Fresh application state and output locations per run; consistent cache treatment.
- No speedup is a valid finding. **60-minute marking limit is provisional.**

### Report (max 3,000 words main text)
Required numbered headings, in order:
1. Project overview and objectives (incl. labelled end-to-end data-flow diagram)
2. Building the corpus (Task 1)
3. Building the data pipeline (Task 2)
4. Model training and evaluation (Task 3)
5. Data analytics and business findings (Task 4)
6. Performance and scalability
7. Conclusions and limitations

Plus: Cover (title, team code, names, student IDs, submission date, **main-text word count**), References (with versions), Appendices:
- **A:** EMR setup and rerun guide (prerequisites/cluster config; environment prep; data acquisition and staging; upload and execute notebook; reload saved model and verify, with dated fresh-session rerun evidence; troubleshooting and cleanup)
- **B:** Supporting evidence with file / notebook-cell / job locators
- **C:** Use of AI assistance
- **D:** Team contributions table (name, work delivered, est. contribution %, total 100%)

> The brief's submission checklist calls the contributions table "Appendix C", contradicting the report section above. `[ASK TT]`

### Submission checklist (from brief)
- ZIP contains `report.pdf`, the **actual .ipynb exported from EMR**, ML model(s), all helper scripts and assets.
- Notebook rerun in a fresh EMR session; saved model reproduces predictions without retraining.
- Each verification record: check, steps/command, expected result, actual result, evidence location. Report failed checks honestly.
- Appendix A records data versions, training cutoff, software versions, random seeds, saved-model locations.
- Every referenced file exists at a path **relative to the ZIP root**.
- **No credentials, tokens, raw datasets or machine-specific paths.**
- Data sources, reused code and AI assistance declared.

### Rubric (100)

| Criterion | Marks |
|---|---|
| Problem framing and design | 10 |
| Corpus quality and provenance (Task 1) | 15 |
| Distributed data pipeline (Task 2) | 20 |
| Model training and evaluation (Task 3) | 20 |
| Data analytics and interpretation (Task 4) | 15 |
| EMR execution, performance and reproducibility | 15 |
| Communication, contribution and attribution | 5 |

Marker principle: marks go to **verifiable evidence**. Screenshots alone don't establish execution. Missing evidence = no credit for the claim.

---

## 3. What changed from the Final Draft `[PROPOSED]`

| # | Risk | Final Draft | Improved setting |
|---|---|---|---|
| 1 | **Weak signal:** weekly beat-the-median labels are mostly noise; 4 features rarely lift AUC much above 0.5 | 4 indicators; window lengths tuned on validation | 8 indicators in four families with fixed standard windows (§7.4); within-week percentile ranks (§7.5); small pre-declared grid (§8.2); success rule written before test (§8.6); a null result is a valid answer |
| 2 | **Reliability question drowned in noise:** a monthly AUC on ~2,000 rows is roughly ±0.025, wider because stocks in a week move together | Metrics by month and volatility group | Weeks are the unit of analysis: weekly AUC, week-resampling bootstrap intervals, paired differences against the baseline; quarters and terciles instead of months; same analysis on 2016 as a consistency check (§8.5, §10) |
| 3 | **Momentum baseline may point the wrong way:** short-horizon returns tend to partly reverse | Majority class + 4-week momentum rule | Add a 1-week reversal rule and a best single-indicator rule whose direction is fixed on training data; the headline comparison is against the strongest simple rule (§8.1) |
| 4 | **No split handling:** an unadjusted 2-for-1 split forces label 0 and corrupts momentum features | Listed as a limitation | Detection rule + reviewed split-event table; stock-weeks whose outcome or lookback windows cross a split are ineligible; counts reported (§6.2, §7.2) |
| 5 | **GDELT's role is thin:** market-wide news is the same for every stock, so it can't rank stocks | Optional interaction experiment trained on ~26 weeks | Secondary question answered by **conditional evaluation** of the frozen main model across news-activity terciles on out-of-sample weeks; news measured as abnormal intensity vs. a trailing average; interaction experiment kept only as an exploratory extra (§8.9, §10) |

Also carried over from the v1 review: no expanded GDELT copy in HDFS; one mapper per GDELT archive; DDL scripts so Hive tables can be recreated; an explicit survivorship check; a conservative GDELT timing rule.

---

## 4. Project definition

### Questions `[DECIDED; wording PROPOSED]`
- **Main:** Can momentum, reversal, relative-volume and volatility indicators predict whether a stock will finish the next trading week above the median eligible stock, better than simple single-indicator rules?
- **Secondary (optional):** Does market-wide news activity change how well these indicators work?

### Business problem
Quant research teams screen many candidate indicators, and most apparent signals fail out of sample. Common causes are look-ahead bias, survivorship, unadjusted prices, shuffled splits and overstated certainty. False discoveries waste research time and can lead to capital being committed to signals that never worked. The project builds a reproducible, leakage-controlled pipeline that tests whether simple price and volume indicators carry signal beyond simple rules, and states how certain that conclusion is.

### Intended users

| User | Decision | What they need from the project |
|---|---|---|
| Quant research analysts (primary) | Which indicators merit further research | Honest signal estimates vs. simple rules, with uncertainty |
| Research leads / model reviewers | Whether to approve a signal for the next research stage | Leakage controls, provenance, a reloadable model and a rerun guide |

### Scope
- Signal quality only. **No claims about profit, trading costs or historical investability.**
- Expected outcome: any edge will be small. A well-measured null result is a valid answer.

### Draft wording for report section 1 (~110 words)
> Quantitative research teams screen many candidate indicators, but most apparent signals fail out of sample, often because of look-ahead bias, survivorship, unadjusted prices or overstated certainty. This project builds a reproducible, distributed pipeline that tests whether momentum, reversal, relative-volume and volatility indicators computed from daily prices can predict which of 505 US stocks will finish the next trading week above the median eligible stock, compared with simple rule-based baselines and with explicit uncertainty. Intended users are quantitative research analysts deciding which indicators merit further research, and reviewers who must be able to audit and rerun the evidence. The project assesses signal quality only and makes no claims about profit, trading costs or investability.

AI-generated wording must be declared in the AI-assistance appendix.

---

## 5. Data sources

| Dataset | Coverage | Used for | Not used for |
|---|---|---|---|
| Daily price CSV `[DECIDED]` | 505 series, 8 Feb 2013 – 29 Dec 2017, ~606k rows | Labels, all main-model features, Task 4 volume and reliability questions | — |
| GDELT daily archives `[DECIDED]` | 731 archives, Jul 2015 – Jun 2017, ~144M events, 9.61 GB zipped / 54.71 GB expanded | Hadoop Streaming parse and audit, Hive daily aggregates, Task 4 news question, conditional evaluation, optional interaction experiment | Main-model features |
| SEC EDGAR, Wikinews | — | Not used: SEC has ~1 filing per issuer; Wikinews has weak links to specific stocks | — |

### Price CSV notes
- A full grid of 505 series × ~1,233 trading days ≈ 622,665 bars; 605,887 actual ≈ 97% coverage. Likely some series start partway through. **Verify** in Task 1 profiling.
- Both endpoints are Fridays (2013-02-08 and 2017-12-29).
- Confirm the actual schema (expected something like date, open, high, low, close, volume, ticker).
- Acquisition location and version of the "price snapshot" not yet known `[ASK TT]`.
- **Survivorship check `[PROPOSED]`:** count series whose last bar is before 2017-12-29 and series whose first bar is after 2013-02-08. If almost none end early while some start late, the list was likely built from end-of-period constituents. Report the counts either way.

### GDELT notes
- Covers only Jul 2015 – Jun 2017: 6 of 36 training months, all of validation, half of test.
- Events are coded around actors (mostly countries, governments, organizations), not tickers → market-wide weekly measures only.
- Each daily archive holds events added that day (`DATEADDED`); events can describe earlier happenings (`SQLDATE`). Assign events to days by archive date / `DATEADDED`, not `SQLDATE`.
- Candidate fields (verify against the GDELT 1.0 event codebook and supplied metadata, including column count): `GLOBALEVENTID`, `SQLDATE`, `DATEADDED`, `EventRootCode`, `QuadClass`, `GoldsteinScale`, `NumMentions`, `NumSources`, `NumArticles`, `AvgTone`, `ActionGeo_CountryCode` (FIPS 2-letter, e.g. `US`), `Actor1CountryCode`/`Actor2CountryCode` (CAMEO 3-letter, e.g. `USA`).
- Storage: keep the 9.61 GB of ZIPs as the raw corpus (~19.2 GB physical at replication 2); **do not store the expanded 54.71 GB**. Decompress inside mappers and keep only needed columns/aggregates.

### Gaps no source fills (document as limitations)
- No adjusted prices, split or dividend data (splits handled by §6.2; dividends cannot be fixed).
- No delisted stocks if survivorship is confirmed.
- No sector labels, market cap or benchmark index (dollar volume is the size/liquidity proxy; the median eligible stock is the weekly reference).
- External data appears out of scope `[ASK TT]`.

---

## 6. Data quality checks

### 6.1 General checks
Price CSV:
- Missing fields; non-numeric values; unparseable dates.
- Duplicate `(ticker, date)` rows (exact vs. conflicting).
- OHLC consistency: `high >= max(open, close)`, `low <= min(open, close)`, `low > 0`, `high >= low`.
- `volume >= 0`; zero-volume days.
- Per-ticker date ranges; gaps vs. the market trading calendar.

GDELT:
- Archives present vs. 731 expected dates; missing/corrupt archives; malformed lines (wrong field count); unparseable numerics; duplicate `GLOBALEVENTID`s; distribution of `DATEADDED − SQLDATE` lags.

Reconciliation: `raw rows = clean rows + rejected rows (+ documented duplicate removals)`, per source and per split.

### 6.2 Split detection `[PROPOSED]`
1. Flag every overnight move with `|ln(open_t / close_{t-1})| > 0.25` (about ±22%).
2. Classify a flagged move as a **split candidate** if the ratio is within ±3% of `1/k` for a common ratio `k` (2, 3, 4, 1.5, 5, 10, or their reciprocals for reverse splits) **and** average volume over the next 20 days vs. the previous 20 shifts by roughly a factor of `k`.
3. Review the candidate list by hand (expected to be short) and record each decision with its evidence in a versioned table, `config/split_events.csv` (ticker, date, estimated ratio, k, decision, evidence). Register it as a Hive table.
4. **Handling:** do not back-adjust prices. Instead, a stock-week is **ineligible** if a confirmed split falls inside its outcome window or inside any feature lookback window (§7.2). Report how many stock-weeks this removes.
5. Large overnight moves that are not splits stay in the data; they are genuine price changes.
6. If no splits are found, record that the check ran and found none (the source may already be split-adjusted).

Dividends: ex-dividend drops (usually under 1–2%) are unadjusted and slightly bias dividend payers toward label 0 in ex-date weeks. Document; cannot be fixed with these sources.

---

## 7. Prediction setup

### 7.1 Weekly calendar `[DECIDED]`
- Build one market-wide trading calendar from the price data (dates where most series have bars).
- **Start date** = Friday, or the next trading day if Friday is not a trading day.
- The week's outcome runs from the **open on its start date** to the **open on the next start date**.
- **Feature cutoff** = close of the last trading day before the start date.
- Record each week's length in trading days and report how many weeks are affected by holidays.
- Holiday Fridays to cross-check against the data (verify): 2013-03-29, 2014-04-18, 2014-07-04, 2015-04-03, 2015-07-03, 2015-12-25, 2016-01-01, 2016-03-25, 2017-04-14.

### 7.2 Weekly universe `[DECIDED + PROPOSED]`
A stock is **eligible** in a week only if:
- it has a valid open on both start dates `[DECIDED]`;
- it has enough clean history for every feature (61 closes; §7.4) `[DECIDED]`;
- no confirmed split falls inside the outcome window or any feature lookback window `[PROPOSED]`.

Report the number of eligible stocks per week. The weekly median is computed over eligible stocks only.

### 7.3 Label `[DECIDED]`
- `ratio = open(next start date) / open(start date)`, the source price ratio. **Never a feature.**
- `label = 1` if `ratio` is strictly above the median ratio of eligible stocks that week, else 0.
- Unavailable outcomes are excluded, never filled. The label is balanced by design (about 50% ones).

### 7.4 Features `[PROPOSED]`
`C₀` = close on the feature cutoff day; `C₋k` = close k trading days earlier. All windows end at the cutoff.

| Feature | Family | Definition | Window | In F1 | In F2 |
|---|---|---|---|---|---|
| `rev_1w` | Reversal | `ln(C₀ / C₋5)` | 5 days | | ✓ |
| `mom_4w` | Momentum | `ln(C₀ / C₋20)` | 20 days | ✓ | ✓ |
| `mom_12w` | Momentum | `ln(C₀ / C₋60)` (Final Draft version) | 60 days | ✓ | |
| `mom_12w_skip` | Momentum | `ln(C₋5 / C₋60)`: skips the latest week so it doesn't overlap reversal | 60 days | | ✓ |
| `vol_20d` | Volatility | Std of daily close-to-close log ratios | 20 days | ✓ | ✓ |
| `vol_ratio` | Volatility | `ln(Parkinson vol 5d / Parkinson vol 60d)`, where daily Parkinson variance = `ln(H/L)² / (4·ln2)`: is volatility rising or falling | 5 / 60 days | | ✓ |
| `max_move_20d` | Volatility | Largest absolute daily log ratio | 20 days | | ✓ |
| `rel_volume` | Volume | `ln(mean volume 5d / mean volume 60d)` (stock vs. its own history) | 5 / 60 days | ✓ | ✓ |
| `dollar_volume` | Liquidity | `ln(mean(close × volume))`, size/liquidity proxy | 60 days | | ✓ |

- **Windows are fixed in advance** at these standard values, rather than tuned on validation. Validation chooses only among the pre-declared options in §8.2. This keeps the number of comparisons small and reduces the chance of a false discovery.
- Max lookback = 61 closes → the first eligible start date falls around early May 2013, removing roughly 13 weeks. Report the exact count.
- Ticker, date, `ratio`, `label` and any data after the cutoff are never features.

### 7.5 Within-week percentile ranks `[PROPOSED]`
- Convert each feature to its percentile rank (0–1) among that week's eligible stocks (PySpark `percent_rank` over a window partitioned by start date).
- Why: the label is relative within the week, so the relative position of a stock's features is what matters. Ranks also remove market-wide level shifts (a calm 2017 vs. a turbulent 2016) and tame outliers.
- Ranks use same-week information only and **fit no parameters**, so they are leakage-safe and identical across splits. State this explicitly in the report.
- The Spark ML Pipeline keeps Imputer → VectorAssembler → StandardScaler → model, all fitted on training data only. (With ranks, the scaler changes little, but it keeps the learned preprocessing inside the saved PipelineModel.)

### 7.6 Splits and leakage controls `[DECIDED]`

| Split | Period | Rule |
|---|---|---|
| Train | 2013–2015 | Week used only if its outcome ends within 2015 |
| Validation | 2016 | Week used only if its outcome ends within 2016 |
| Test | 2017 | Used once, after the final model is frozen |

- Features may look back across a split boundary (past information).
- Task 4 analyses that could influence feature or model choices run on **training data only** until the model is frozen `[PROPOSED]`.

### 7.7 Rough sample sizes (estimates; replace with measured counts)
- Train: ~137 weeks × ~480 eligible stocks ≈ 65k rows
- Validation: ~51 weeks × ~490 ≈ 25k rows
- Test: ~51 weeks × ~495 ≈ 25k rows

---

## 8. Models and evaluation

### 8.1 Baselines `[DECIDED + PROPOSED]`
All baselines produce a score per stock-week and are evaluated on exactly the same rows as the model.

| ID | Baseline | Score | Status |
|---|---|---|---|
| B0 | Majority class | Constant (AUC 0.5 by construction) | `[DECIDED]` |
| B1 | 4-week momentum rule | `rank(mom_4w)` | `[DECIDED]` |
| B2 | 1-week reversal rule | `1 − rank(rev_1w)` | `[PROPOSED]` |
| B3 | Best single-indicator rule | For each F2 indicator, score = `rank(x)`, flipped to `1 − rank(x)` if its training mean weekly AUC is below 0.5. Pick the indicator with the highest **training** mean weekly AUC, then freeze | `[PROPOSED]` |

The **headline comparison is the final model vs. B3**, the strongest simple rule. B1 stays for continuity with the Final Draft.

### 8.2 Models and pre-declared grid `[DECIDED models; grid PROPOSED]`

| Model | Feature set | Settings | Configs |
|---|---|---|---|
| LogisticRegression | F1, F2 | `regParam` ∈ {0.0, 0.01} | 4 |
| RandomForestClassifier | F1, F2 | `numTrees` = 200, `maxDepth` ∈ {4, 8}, fixed `seed` | 4 |

Eight configurations in total. Write this grid into the decision log **before** running validation.

### 8.3 Selection and freeze
- Select on **mean weekly AUC on 2016 validation**.
- If two configurations are within one standard error of each other, choose the simpler one (logistic regression, F1).
- Freeze the choice and write the success rule (§8.6) into the decision log, then evaluate on 2017 **once**.

### 8.4 Metrics

| Role | Measure | How it is described |
|---|---|---|
| Main | Mean weekly AUC, with 95% interval | Model and every baseline on the same rows |
| Main | Paired difference: model − B3 weekly AUC, with 95% interval | Headline result |
| Main | Balanced accuracy, precision, recall | Predict 1 for stocks above that week's median score, so predictions are balanced each week |
| Reference | Pooled AUC (`BinaryClassificationEvaluator`) | Over all test rows |
| Secondary | Weekly rank IC | Spearman correlation between scores and next-week source price ratios |
| Secondary | Decile spread | Mean `ln(ratio)` of the top decile minus the bottom decile per week: a source-price-ratio difference, **not a return** |
| Secondary | Turnover | Change in top-10% membership between consecutive weeks |

### 8.5 Inference: weeks as the unit `[PROPOSED]`
- Stocks in the same week move together, so rows are not independent. Treat each **week** as one observation.
- Per week `w`, compute AUC with the rank-sum formula, using average ranks for ties (random-forest probabilities often tie):
  `AUC_w = (R₊ − n₊(n₊+1)/2) / (n₊ · n₋)`
  where `R₊` = sum of ranks of the positive rows by score. Compute it in Spark with window functions; the output is one row per week.
- Bootstrap: resample weeks with replacement (10,000 resamples, fixed seed) from the ~50-row weekly table to get 95% intervals for mean AUC and for the paired difference. The weekly table is aggregated data, so running the bootstrap on the driver is allowed.
- Rough sensitivity: one week's AUC from ~500 stocks carries about ±0.05 sampling error (95%). Averaged over ~50 weeks, differences much smaller than roughly 0.01–0.015 AUC are unlikely to be distinguishable from noise. Measure the actual week-to-week spread on 2016 validation before testing.
- Subgroups: quarters (~13 weeks each), stock volatility terciles (within-week rank of `vol_20d`), and market-regime terciles (weekly cross-sectional dispersion of `ratio`). Always report the number of weeks and rows, and an interval.
- A subgroup pattern counts as a finding only if it points the same way in 2016 validation and 2017 test.

### 8.6 Success rule (pre-registered) `[PROPOSED]`
Written into the decision log before the test set is touched:
> The frozen model shows evidence of predictive signal only if, on 2017 test, (1) its mean weekly AUC has a 95% interval above 0.5, **and** (2) the mean paired difference against B3 has a 95% interval above 0. Otherwise the finding is "no evidence that the model adds signal beyond simple rules".

### 8.7 Output weights `[DECIDED; format ASK TT]`
- Each week, rank eligible stocks by model score. The top 10% (`k = ceil(0.1 · n_w)`) get weight `1/k`; all others get 0. Weights sum to 1 each week.
- Described as a **hypothetical selection output**, never as evidence of attainable returns.

### 8.8 Model reproducibility
- Save the fitted `PipelineModel` to HDFS and copy it into the ZIP under `models/`.
- Fresh Spark session: load, predict on the test set, compare against stored predictions. Stated tolerance: max absolute difference in predicted probability ≤ `1e-9` (expected 0). Record in Appendix B.
- Record seeds, software versions, training cutoff and data versions.

### 8.9 GDELT: conditional evaluation and optional experiment `[PROPOSED]`

**Weekly news measures** (market-wide, from `clean.gdelt_daily`):
- `news_abn` = `ln(events over the 5 archive days before the cutoff / (5 × mean daily events over the prior 60 archive days))`. Abnormal intensity, because raw GDELT counts drift as its source base changes.
- `tone_mean` = mean `AvgTone` weighted by `NumArticles` over the same 5 days.
- `conflict_share` = share of events with `QuadClass` 3 or 4.
- Optional US-only versions (`ActionGeo_CountryCode = 'US'`).

**Timing:** pre-start measures use only archives dated **at least one calendar day before the feature cutoff day**, because each archive is published the following morning. Same-week measures (archives within the outcome week) are allowed only for Task 4 association analysis and must be labelled as same-week.

**Conditional evaluation (answers the secondary question):**
- Out-of-sample weeks with GDELT coverage: 2016 validation (~51 weeks) + Jan–Jun 2017 test (~25 weeks) ≈ 76 weeks.
- Group weeks into `news_abn` terciles (~25 weeks each). Compare mean weekly AUC (model and B3) and rank IC by tercile, with intervals.
- Uses the frozen main model's predictions, so no model is trained on GDELT's short window. 2016 was used for model selection, so also report 2016 and 2017 separately. Expect wide intervals.

**Optional interaction experiment (exploratory only):** the Final Draft's matched design. Price-only vs. price + interactions (`news_abn × rank(vol_20d)`, `news_abn × rank(mom_4w)`, `news_abn × rank(rev_1w)`), both trained on Jul–Dec 2015 start dates whose outcome ends in 2015, both tested on Jan–Jun 2017, using the frozen main configuration with no tuning. Report the ~26-week training set as a limitation.

---

## 9. Pipeline architecture and tool roles `[PROPOSED]`

### Engine responsibilities
- **HDFS:** raw, clean, rejected, feature, model, prediction, evaluation and log data in separate folders; each engine reads the previous stage's output.
- **Hadoop Streaming:**
  1. Price CSV parsing + per-ticker audit summaries (row counts, date range, null/invalid/duplicate counts, overnight-jump flags).
  2. GDELT per-archive parsing → daily aggregates (event counts, counts by `QuadClass`, US-related counts, weighted tone, malformed-line counts, duplicate IDs). Use `NLineInputFormat` over a manifest of archive paths so each mapper handles one ZIP (ZIPs aren't splittable); the mapper copies the archive from HDFS to local temp, decompresses and emits aggregates.
- **Hive:** external tables over raw and Streaming outputs; managed Parquet tables for clean/rejected; split-event table; trading calendar and weekly start-date table; eligibility and label tables; split tables; reconciliation queries; Task 4 SQL analyses.
- **PySpark:** validity rules, split detection, calendar and universe logic, labels, features and ranks, news measures, Spark ML pipelines, predictions, weekly evaluation tables, output weights, distributed analytics aggregations.
- Spark reads Hive tables via `enableHiveSupport()`. The EMR Hive metastore may live on the primary node and disappear with the cluster → keep all DDL in versioned scripts.

### Storage formats
- Raw: exactly as acquired (CSV, ZIP), plus a manifest (source reference, retrieval time, size, SHA-256).
- Everything downstream: **Parquet**, partitioned by year or split.

### Proposed HDFS layout
```
/inf2006/
  raw/prices/<snapshot_version>/acquired=<YYYY-MM-DD>/...
  raw/gdelt/v1-daily/acquired=<YYYY-MM-DD>/<YYYYMMDD>.export.CSV.zip
  manifests/
  staging/streaming/<run_id>/
  clean/prices_daily/
  rejected/prices_daily/
  clean/gdelt_daily/
  features/<run_id>/
  models/<run_id>/
  predictions/<run_id>/
  eval/<run_id>/
  analytics/<run_id>/
  logs/<run_id>/
```
Hive databases: `raw`, `clean`, `features`, `model`, `analytics`.

### Hand-off table contracts (agree before splitting work)

| Table | Grain | Key columns |
|---|---|---|
| `clean.prices_daily` | ticker × date | ticker, date, open, high, low, close, volume, source_file, source_row, ingest_run_id |
| `clean.prices_rejected` | rejected row | same + reject_reason |
| `clean.split_events` | confirmed split | ticker, date, ratio_est, k, decision, evidence |
| `features.calendar_weeks` | week | start_date, next_start_date, cutoff_date, n_trading_days, split |
| `clean.gdelt_daily` | archive day | archive_date, n_events, n_us, n_conflict, tone_wmean, n_malformed, n_dup, source_file |
| `features.weekly_news` | week | start_date, news_abn, tone_mean, conflict_share, news_abn_same_week |
| `features.weekly` | ticker × week | ticker, start_date, split, eligible, ratio, label, raw features, `*_rank` features |
| `model.predictions` | ticker × week × model | ticker, start_date, split, model_id, score, pred, label, ratio |
| `model.weekly_eval` | week × model | start_date, split, model_id, n, n_pos, auc, rank_ic |
| `model.weights` | ticker × week | ticker, start_date, model_id, weight |

### Storage budget (measure, don't assume)
- GDELT ZIPs: 9.61 GB logical → ~19.2 GB physical.
- Prices and derived Parquet: small (measure).
- Shuffle/temp and logs likely share the core nodes' data volumes with HDFS (verify). Measure peak use and free space per worker (`hdfs dfsadmin -report`, `df -h`).

---

## 10. Task 4 analytics questions `[DECIDED + PROPOSED]`
Each answered with Hive and/or PySpark on the full data; only aggregated results are plotted.

| # | Question | Data | Analysis | Tool |
|---|---|---|---|---|
| 1 | When is trading volume unusually high or low compared with each stock's own history, and how does that relate to next-week price ratios? | Clean prices, weekly table | Within-week quintiles of `rel_volume`; per quintile: share above median and mean of `ln(ratio)` minus the weekly median, with group sizes and intervals; box plot. Run on training years first if results could influence feature choices | Hive |
| 2 | Where is the model most and least reliable compared with the baselines? | 2017 test predictions (+ 2016 for consistency) | Weekly AUC and paired difference vs. B3 by quarter, volatility tercile and market-dispersion tercile; number of weeks/rows; error chart with intervals | PySpark |
| 3 | Do weeks with high market-wide news activity show different volatility, dispersion or model accuracy? | GDELT weekly measures, Jul 2015 – Jun 2017 | (a) Market-wide volatility and cross-sectional dispersion by `news_abn` tercile, pre-start and same-week labelled separately; (b) conditional evaluation (§8.9). Association only, not cause | Hive + PySpark |

Regime note for Q2: 2017 was an unusually calm year for US stocks. Lower dispersion makes the weekly label noisier, which may lower AUC in 2017 regardless of the indicators. Discuss honestly; never retune on test.

---

## 11. Performance and scalability plan `[DECIDED]`
A separate workstream with one owner; at least 9 timed full-pipeline runs, so it must start early.

**Profiles** (frozen before any comparison):
- Development: a documented subset, e.g. a fixed set of ~50 tickers chosen by a deterministic rule plus one fixed month of GDELT archives.
- Full: all selected price data and all selected GDELT archives.

| Run set | Profile | Config | Repetitions |
|---|---|---|---|
| A | Development | Reference | 3 |
| B | Full | Reference | 3 |
| C | Full | One justified alternative | 3 |

- The alternative changes one thing only (e.g. shuffle partitions, file format or join strategy), chosen from evidence in run set B. Keep hardware, release, replication and storage fixed.
- Timing starts after raw inputs are staged in HDFS. Report download, S3-to-HDFS copy, setup and cluster startup separately.
- Record for every run: run ID, stage timings, input/output rows and bytes, executor settings, measured memory and disk per worker, output checks (same row counts and same predictions between configs), failures.
- Built in from day one: a shared logging helper (writes `evidence/timings.csv`, records YARN application IDs, Hadoop job IDs, Hive query IDs) and a single run script that takes `--profile` and `--config` arguments.

---

## 12. Workstreams and team practices

### First, together (before splitting work) `[DECIDED]`
- [ ] Agree HDFS folder layout (§9)
- [ ] Agree table names and columns for each stage (§9 contracts), especially `features.weekly`
- [ ] Create one config file for paths, dates, profiles, seeds and Spark settings
- [ ] Set up the Git repo and a shared decision log; record which `[PROPOSED]` items are accepted
- [ ] Freeze development and full input profiles
- [ ] Send the teaching-team questions (§16)

### Workstreams `[DECIDED]`

| Workstream | Main deliverables | Report section |
|---|---|---|
| Corpus (Task 1) | Raw price and GDELT files in HDFS, unchanged; inventory with URLs, download times, sizes, row counts, checksums; data dictionary; raw quality findings; survivorship check | 2, Appendix B |
| Pipeline (Task 2) | Streaming parse and audit jobs; Hive tables per stage; PySpark cleaning, split detection, calendar, universe, labels, features, ranks; row-count reconciliation; split counts and exclusions | 3, Appendix B |
| Model (Task 3) | Baselines B0–B3; pre-declared grid; validation comparison; success rule; frozen test results; weekly evaluation and bootstrap; saved PipelineModel and reload check | 4, Appendix B |
| Analytics (Task 4) | Queries and code for the three questions; GDELT news measures and conditional evaluation; result tables; labelled charts | 5, Appendix B |
| Performance and reproducibility | Logging helper; run script; 9 timed runs; memory and disk measurements; EMR rerun guide | 6, Appendix A |

Shared by everyone: section 1 (overview and data-flow diagram), section 7 (conclusions), references, AI-use declaration and contribution table.

### Practices `[PROPOSED]`
- **One notebook, thin:** the brief wants the actual .ipynb exported from EMR. Keep logic in owned `.py`, Streaming and `.sql` files; the notebook only calls them, one section per stage. One person owns the notebook file.
- **Unblock with stubs:** the pipeline workstream publishes a rough `features.weekly` from the dev sample early, so the model and analytics workstreams can build against the contract in week 1.
- **Leakage cross-review:** the pipeline owner reviews the model's evaluation code; the model owner reviews feature, universe and split code.
- **Independent rerun test:** someone outside the performance workstream follows Appendix A on a fresh cluster; whatever they get stuck on gets fixed in the guide.
- **Contribution log:** short running notes of who did what, for the contributions table.
- **AWS credits:** develop locally on the dev sample; run assessed EMR work from one agreed account.

---

## 13. Timeline

| Dates | Milestone |
|---|---|
| Oct 29 – Nov 1 | Shared setup (§12); send TT questions; freeze dev profile |
| Nov 2 – 8 | Build every stage on the dev sample: inventory, Streaming jobs, Hive tables, cleaning, split detection, calendar, universe, labels, features; baseline and weekly-evaluation code on stub features |
| Nov 9 – 13 | Full pipeline on EMR; validation comparison of the 8 configs; freeze model; write success rule; single test evaluation; save/reload check |
| Nov 12 – 18 | Timed run sets A, B, C; Task 4 analyses on final outputs |
| Nov 17 – 20 | Report; Appendix A; independent fresh-session rerun |
| Nov 21 – 22 | ZIP checks (relative paths, no credentials or raw data, word count); buffer |

---

## 14. Report plan (word budget ≈ 3,000)

| Section | Words |
|---|---|
| 1. Overview and objectives (+ data-flow diagram) | ~300 |
| 2. Corpus | ~450 |
| 3. Pipeline | ~600 |
| 4. Model | ~600 |
| 5. Analytics | ~550 |
| 6. Performance | ~350 |
| 7. Conclusions | ~150 |

Language rules: "source price ratio", "source-price-ratio difference", "hypothetical selection output", "signal diagnostics". Never "returns", "profit", "alpha generated" or investability claims.

---

## 15. Known limitations
- Likely survivorship bias (check per §5).
- No trading costs; results are not returns.
- One test year, ~50 weekly cross-sections: low certainty, quantified by week-level intervals.
- GDELT covers only Jul 2015 – Jun 2017, is market-wide only and not linked to individual stocks.
- Split detection may miss or misclassify events; dividends are unadjusted.
- 2017's calm regime may make labels noisier than in training years.
- The grid is small and pre-declared, but eight configurations plus baselines still involve some multiple testing.

---

## 16. Open questions for the teaching team `[ASK TT]`
1. What format the "output weights" should take, and what the common assessed workload is.
2. Confirmed EMR release, permitted configuration adjustments and final timing policy (60-minute limit).
3. Whether market-wide GDELT processing counts as meaningful pipeline use.
4. Whether a full GDELT run means all 731 archives, or a documented subset is acceptable.
5. Where and how to acquire the price snapshot, and its version identifier.
6. Whether external data (e.g. sector labels) is allowed. Current plan: no.
7. Appendix lettering: the brief lists AI use as Appendix C and contributions as Appendix D, but the submission checklist calls the contribution table Appendix C.

---

## 17. Suggested repo layout
```
HANDOVER.md
docs/brief/                  # official brief PDF + SEC inventory xlsx (not raw data)
docs/decisions/              # Final Draft export + decision log
notebooks/                   # the EMR notebook (export the final .ipynb from EMR)
streaming/                   # Hadoop Streaming mappers/reducers + run scripts
hive/                        # DDL and query .sql files, numbered by run order
spark/                       # stage modules called by the notebook
evaluation/                  # weekly AUC, bootstrap, baselines
config/                      # project config, spark settings, paths.template.yaml, split_events.csv
manifests/                   # corpus inventory: sources, sizes, SHA-256, retrieval times
evidence/                    # timings.csv, job IDs, verification records, rerun logs
models/                      # saved PipelineModel for the ZIP
report/                      # drafts, figures, final report.pdf
tests/                       # local checks on small samples
```

---

## 18. Working rules (for Claude Code and the team)
- Never fill gaps with future observations. No random shuffles across splits. Test set touched once, after the success rule is written.
- No pandas on full data; only small aggregated results (e.g. the weekly evaluation table) leave Spark.
- Raw data is immutable; every run writes to new `run_id` output locations.
- Fix and record random seeds and software versions.
- No credentials, tokens or machine-specific paths in committed files; use `config/paths.template.yaml` with placeholders.
- Every report claim must point to evidence: file path, notebook cell, or job/query ID.
- Local runs are development only; assessed evidence must come from EMR.
- Keep a running log of AI assistance (this planning chat + Claude Code sessions: tool, purpose, affected work).

---

## 19. Immediate next steps
1. Scaffold the repo (§17), the project config and `config/paths.template.yaml`.
2. Write a local profiling script for a sample of the price CSV: schema, counts, per-ticker date ranges, OHLC violations, duplicates, survivorship counts, overnight-jump scan.
3. Draft the Hadoop Streaming mapper/reducer for the price audit; test locally with `cat sample.csv | mapper.py | sort | reducer.py`.
4. Draft Hive DDL for the §9 contract tables.
5. Implement the calendar, universe and label logic in PySpark on the sample; print week lengths, eligible counts per week and excluded counts.
6. Implement features and within-week ranks; publish a stub `features.weekly`.
7. Build the evaluation module: baselines B1–B3, weekly AUC with tie-aware ranks, week-level bootstrap.
8. Prototype the GDELT mapper on 2–3 archives (field count, malformed lines, daily aggregates).
9. Write the corpus manifest script (source reference, size, SHA-256, retrieval timestamp).

---

## 20. Parked ideas
- **Volatility forecasting (v1 framing):** predict next-week realized volatility with HAR and GBT models, output inverse-volatility weights. Stronger signal and a more natural model role for GDELT; not chosen. Most of the pipeline above would carry over if the team revisits it.
- **Volume forecasting for trade execution:** runner-up framing, not chosen.
