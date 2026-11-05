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

## Development validation (local, train and validation only; EMR rerun is the evidence)
[dev_validation_2026-11-05](dev_validation_2026-11-05/validation_local-dev.json): 65,086 train and 25,720 validation rows, 52 validation weeks.
- B3 = `vol_ratio`, flipped (training mean weekly AUC 0.4904, the strongest single indicator once its direction is reversed).
- All eight configs: mean weekly AUC 0.494 to 0.500 on 2016, every 95% interval spans 0.5; paired differences vs B3 between -0.002 and +0.005, every interval spans 0. Weekly AUC varies a lot from week to week (standard deviation 0.04 to 0.08).
- Best mean: `rf_f2_d8` (0.5004); within one standard error (0.0063) the simplest is `lr_f1_r001`, the recommended frozen config.
- Reading: no indicator set shows a clear edge on 2016, consistent with HANDOVER's expectation that weekly beat-the-median labels are mostly noise. A null result on test would be a valid finding.
