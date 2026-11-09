# Task 3: model training and evaluation

Code: [`modeling/run_model.py`](../../modeling/run_model.py) (validate / test / reload), [`evaluation/metrics.py`](../../evaluation/metrics.py), [`modeling/run_task3.sh`](../../modeling/run_task3.sh) (EMR), [`modeling/model_tables.sql`](../../modeling/model_tables.sql) (Hive). EMR steps: [aws_task3.md](aws_task3.md). Rules: HANDOVER section 8; Gate 1 and Gate 2 in [decision-log.md](../decisions/decision-log.md).

## Question, target and output
- Predict whether a stock finishes the next trading week above the median eligible stock (label from `features.weekly`, source price ratio open-to-open, Friday to Friday).
- Output weights: each test week the top 10% of eligible stocks by model score get weight 1/k, the rest 0 (`model.weights`). A hypothetical selection output, not evidence of attainable returns.

## Baselines (same rows as the model)
| ID | Score | Fixed on |
|---|---|---|
| B0 | constant 0.5 (majority class; AUC 0.5 by construction) | none |
| B1 | `mom_4w_rank` (4-week momentum) | none |
| B2 | `1 - rev_1w_rank` (1-week reversal) | none |
| B3 | best single F2 indicator rank, flipped if its training mean weekly AUC < 0.5 | 2013-15 train only |

## Models (Gate 1 grid, locked 2026-11-05 before any validation)
Imputer (median) → VectorAssembler → StandardScaler → LogisticRegression or RandomForest, on the within-week `*_rank` features, fitted on train rows only, seed 2026. Eight configs: LR on F1/F2 × regParam {0.01, 0}; RF (200 trees) on F1/F2 × maxDepth {4, 8}. Selection on 2016 mean weekly AUC; the simplest config within one standard error of the best is frozen.

## Evaluation
- Weeks are the unit: per-week AUC (rank-sum, average ranks for ties), rank IC (Spearman with the ratio) and decile spread of ln(ratio); then the mean over weeks with a 10,000-draw week bootstrap interval (seed 2026), and the paired weekly difference against B3.
- Also: balanced accuracy, precision, recall (predict 1 above the week's median score), pooled AUC, top-10% turnover.
- Success rule (Gate 2): signal only if the test mean weekly AUC interval is above 0.5 **and** the paired difference vs B3 interval is above 0.

## Safeguards built into the code
- `validate` filters the test split out on read and records `"test_split_read": false`.
- `test` refuses to run unless Gate 2 names the same frozen config and is locked, and refuses if a test evaluation was recorded before (marker written in HDFS and in `evidence/model/` **before** any test row is scored). The test set is evaluated once.
- `reload` is a separate spark-submit (fresh session): loads the saved PipelineModel and re-predicts the test rows without retraining; pass if max |difference| ≤ 1e-9.
- Tested end to end on synthetic data with a planted signal (`tests/model/test_run_model.py`): unlocked gate refused, single run allowed, second run refused, reload exact.

## EMR results (run `20261106-1400-full-ref`, assessed evidence)

Evidence: [`evidence/model/`](../../evidence/model/) (validation, test and reload JSON, weekly tables, logs, job IDs); saved model [`models/20261106-1400-full-ref/pipeline_model`](../../models/20261106-1400-full-ref/pipeline_model).

**Order of events (from the logs):** validation ran on commit `956c3bf` (2026-11-08 09:36 UTC, application `application_1794129037311_0015`); Gate 2 was locked in commit `b495d83`; the single test run started on commit `b495d83` at 09:54 UTC (`application_1794129037311_0017`), after the marker was written; reload ran in a fresh session (`application_1794129037311_0018`).

**Validation, 2016 (52 weeks, 25,720 rows; test split not read):**
- B3 = `vol_ratio`, reversed (training mean weekly AUC 0.4904, the weakest raw indicator, so 1 - 0.4904 makes it the strongest once flipped).
- All eight configs: mean weekly AUC 0.4936 to 0.4989; every 95% interval includes 0.5. Week-to-week standard deviation 0.039 to 0.082.
- Best mean `rf_f2_d8` (0.4989); one SE = 0.0070; the simplest config within it is **`lr_f1_r001`** (0.4946), frozen in Gate 2.

**Test, 2017 (51 weeks, 25,531 rows; evaluated once):**

| | Mean weekly AUC [95%] | Paired vs B3 [95%] | Rank IC [95%] |
|---|---|---|---|
| Model `lr_f1_r001` | 0.5133 [0.4970, 0.5298] | +0.0143 [−0.0084, +0.0364] | 0.0286 [−0.0018, 0.0589] |
| B0 constant | 0.5000 | | |
| B1 momentum | 0.4998 [0.4814, 0.5179] | | 0.0008 |
| B2 reversal | 0.5086 [0.4884, 0.5289] | | 0.0170 |
| B3 `vol_ratio` reversed | 0.4991 [0.4852, 0.5122] | | 0.0046 |

- Balanced accuracy 0.5095, precision 0.509, recall 0.509 (predict 1 above each week's median score); pooled AUC 0.5132.
- Decile spread of ln(source price ratio), top minus bottom decile: 0.0029 [0.0003, 0.0055] per week. A source-price-ratio difference, not a return.
- Top-10% selection turnover: 49.8% of names change each week.
- **Success rule: not met.** The AUC interval includes 0.5 and the paired interval includes 0, so the finding is "no evidence that the model adds signal beyond simple rules". The model is slightly ahead in every point estimate, but by less than 51 noisy weeks can distinguish from chance.
- **Reload:** the saved PipelineModel, reloaded in a fresh session without retraining, reproduced all 25,531 test predictions exactly (max |difference| 0.0; tolerance 1e-9).

## Development validation (local, train and validation only; EMR rerun is the evidence)
[dev_validation_2026-11-05](dev_validation_2026-11-05/validation_local-dev.json): 65,086 train and 25,720 validation rows, 52 validation weeks.
- B3 = `vol_ratio`, flipped (training mean weekly AUC 0.4904, the strongest single indicator once its direction is reversed).
- All eight configs: mean weekly AUC 0.494 to 0.500 on 2016, every 95% interval spans 0.5; paired differences vs B3 between -0.002 and +0.005, every interval spans 0. Weekly AUC varies a lot from week to week (standard deviation 0.04 to 0.08).
- Best mean: `rf_f2_d8` (0.5004); within one standard error (0.0063) the simplest is `lr_f1_r001`, the recommended frozen config.
- Reading: no indicator set shows a clear edge on 2016, consistent with HANDOVER's expectation that weekly beat-the-median labels are mostly noise. A null result on test would be a valid finding.
