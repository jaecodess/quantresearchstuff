# Task 4 on AWS: analytics (Q1, Q2, Q3)

Needs, saved in S3 from earlier tasks:
- Task 2 features: `s3://<bucket>/inf2006/features/<task2_run_id>/weekly` (and `weekly_news` for Q3)
- Task 3 predictions: `s3://<bucket>/inf2006/predictions/<task3_run_id>/predictions` (copied in Task 3 D4)

Nothing is retrained or reselected: Task 4 reads the stored predictions of the frozen model. Use a new run_id for this run, e.g. `20261110-1000-full-ref` (date and time you start). Below: `<task2_run_id>` = `20261105-1430-full-ref`, `<task3_run_id>` = `20261106-1400-full-ref`.

## E0. Cluster and code
Start a cluster and connect as in Task 1 A3 and A4 (same settings as Task 3). At the `[hadoop@ip-... ~]$` prompt:
```bash
sudo yum install -y git tmux
git clone https://github.com/jaecodess/quantresearchstuff.git   # skip if the folder already exists
cd ~/quantresearchstuff
git fetch origin
git switch -c ws/anly/task4 --track origin/ws/anly/task4
ls analytics/run_task4.sh
```
`ls` must print `analytics/run_task4.sh`. Then start tmux (detach Ctrl-b then d; reattach `tmux attach -t task4`):
```bash
tmux new -s task4
cd ~/quantresearchstuff
```
No numpy is needed for Task 4.

## E1. Check the inputs in S3
```bash
aws s3 ls s3://<bucket>/inf2006/features/20261105-1430-full-ref/
aws s3 ls s3://<bucket>/inf2006/predictions/20261106-1400-full-ref/
```
- The first must list `PRE weekly/`. If it also lists `PRE weekly_news/`, Q3 will run. If `weekly_news/` is missing, see E4.
- The second must list `PRE predictions/` and `PRE weights/`.

## E2. Run Task 4 (about 10 to 20 minutes)
```bash
bash analytics/run_task4.sh 20261110-1000-full-ref s3://<bucket>/inf2006 20261105-1430-full-ref 20261106-1400-full-ref
```
It registers the S3 data as Hive tables (nothing is copied), runs Q1 and Q3a in Hive, then Q2, Q3b and the bootstrap intervals in PySpark. The end of the output shows lines like:
```
Q2 test       all       weeks=51 paired model-B3 = 0.0143 [-0.0084, 0.0364]
```
The `test all` line must match Task 3 (+0.0143 [-0.0084, 0.0364]); that confirms the right predictions were read. If Q3 was skipped it prints `Q3 SKIPPED`.

## E3. Bring everything back and terminate
```bash
tar czf ~/task4_evidence_20261110-1000-full-ref.tar.gz evidence/analytics
```
Download with `scp` from a new PowerShell window (as in Task 1 B7), upload the `.tar.gz` here, then terminate the cluster. The charts are drawn from these aggregated files after the upload (no cluster needed).

## E4. Only if `weekly_news/` is missing in E1
Q3 needs the GDELT weekly news table, which only a Task 2 run **with** GDELT writes. In the same tmux window, rebuild it in this session (about 1.5 hours; this run is also the Task 2 evidence run):
```bash
bash corpus/stage_to_hdfs.sh s3://<bucket>/inf2006/raw 20261110-1000-full-ref
bash spark/run_task2.sh 20261110-1000-full-ref /inf2006/raw/prices/plotly-datasets-0c447c4/acquired=2026-11-03
```
Then run E2 with the features in HDFS and the predictions still in S3 (5th argument):
```bash
bash analytics/run_task4.sh 20261110-1000-full-ref /inf2006 20261110-1000-full-ref 20261106-1400-full-ref s3://<bucket>/inf2006
```
and include the Task 2 evidence in E3:
```bash
tar czf ~/task4_evidence_20261110-1000-full-ref.tar.gz evidence/analytics evidence/pipeline
```
