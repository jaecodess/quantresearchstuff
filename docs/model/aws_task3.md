# Task 3 on AWS: validation, Gate 2, single test, reload

Needs `features.weekly` from Task 2. Two ways to get it:
- **Same session as Task 2** (simplest): the Hive table already exists. Leave the features argument out.
- **New cluster**: if you saved the features to S3 in Task 2 step C5, pass that path, e.g.
  `s3://<bucket>/inf2006/features/<task2_run_id>/weekly`. Otherwise rerun Task 2 C2 and C3 first.

Use a new run_id, e.g. `20261106-1400-full-ref`, for both phases. Start a cluster and connect as in Task 1 A3 to A5, then:
```bash
cd ~/quantresearchstuff && git switch main && git pull
tmux new -s task3
```

## D1. Phase 1: validation (about 10 to 20 minutes)
```bash
bash modeling/run_task3.sh validate 20261106-1400-full-ref s3://<bucket>/inf2006/features/<task2_run_id>/weekly
```
(leave the last argument out if `features.weekly` exists in this session). It fits the 8 Gate-1 configs on 2013-15, scores 2016, and ends with:
```
RECOMMENDED FROZEN CONFIG: <config_id> (best <config_id>, 1 SE = ...)
```
Nothing here reads 2017.

## D2. Lock Gate 2 (before the test set is touched)
Send the file `evidence/model/validation_20261106-1400-full-ref.json` (or just the RECOMMENDED line) to whoever edits the repo. Gate 2 in `docs/decisions/decision-log.md` is filled with the frozen config, the date and your handle, committed and pushed. Then on the cluster:
```bash
git pull
grep -A3 "Frozen configuration" docs/decisions/decision-log.md
```
Keep the cluster running meanwhile (about USD 0.80 per hour), or terminate and use the S3 features path in a new session.

## D3. Phase 2: the single test evaluation and reload (about 10 minutes)
```bash
bash modeling/run_task3.sh test 20261106-1400-full-ref <frozen_config> s3://<bucket>/inf2006/features/<task2_run_id>/weekly
```
It refuses with `REFUSED: Gate 2 ...` if the gate is not locked for that config, and with `evaluated ONCE` if a test run was recorded before. **Do not try to work around either message.** It then evaluates 2017 once, saves the PipelineModel, reloads it in a fresh Spark session and compares predictions (must print `"passed": true`), registers `model.predictions`, `model.weekly_eval`, `model.weights` in Hive, and copies the model to `models/<run_id>/`.

## D4. Bring everything back and terminate
```bash
tar czf ~/task3_evidence_20261106-1400-full-ref.tar.gz evidence/model models
```
Download with `scp` from a new PowerShell window (as in Task 1 B7), upload the `.tar.gz` here, then terminate the cluster. To let Task 4 use the predictions in a later session, first copy them to S3:
```bash
s3-dist-cp --src hdfs:///inf2006/predictions/20261106-1400-full-ref --dest s3://<bucket>/inf2006/predictions/20261106-1400-full-ref
s3-dist-cp --src hdfs:///inf2006/eval/20261106-1400-full-ref --dest s3://<bucket>/inf2006/eval/20261106-1400-full-ref
```
