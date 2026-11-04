# Task 2 on AWS: running the pipeline on EMR

Continues from the Task 1 guide ([../corpus/aws_guide.md](../corpus/aws_guide.md)). Do this in one EMR session; keep the same `<run_id>` for every command. Commands below are filled in for a run on 5 Nov 2026: replace `20261105-1400-full-ref` with your own `YYYYMMDD-HHMM-full-ref`, and the bucket name with yours.

## C1. Start a cluster and connect
Same as Task 1 A3 and A4 (same settings, 100 GiB gp3 per core node). Then on the cluster:
```bash
sudo yum install -y git tmux
git clone https://github.com/jaecodess/quantresearchstuff.git
cd quantresearchstuff
git switch main          # after the Task 2 PR is merged; before that: git switch ws/pipe/task2-pipeline
```

## C2. Stage the raw corpus into HDFS
HDFS is empty on a new cluster. Copy from S3 and verify (Task 1 B1). Staging time is reported separately; pipeline timing starts after it.
```bash
bash corpus/stage_to_hdfs.sh s3://<bucket>/inf2006/raw 20261105-1400-full-ref
hdfs dfs -ls /inf2006/raw/prices/plotly-datasets-0c447c4/
```
Note the `acquired=YYYY-MM-DD` folder name in the last output; you need it in C3.

## C3. Run the whole pipeline
Run inside `tmux` so a dropped connection does not stop it (`tmux new -s task2`; detach Ctrl-b d; reattach `tmux attach -t task2`):
```bash
bash spark/run_task2.sh 20261105-1400-full-ref /inf2006/raw/prices/plotly-datasets-0c447c4/acquired=2026-11-03
```
Stages, in order: Hive databases, price audit (Hadoop Streaming), cleaning (PySpark), price tables (Hive), calendar (Hive SQL), split screening (PySpark), split events (Hive), weekly features (PySpark), features table (Hive), GDELT daily job (Hadoop Streaming, 731 mappers), GDELT tables (Hive), weekly news (Hive SQL), reconciliation (PySpark). The GDELT job is the longest stage. To run prices only first, prefix `WITH_GDELT=0`.

Expected checks at the end (same as the development run, because the input bytes are identical):
- `evidence/pipeline/reconciliation_<run_id>.json`: prices `"reconciles": true`, 605,887 clean rows; eligible train 65,086, validation 25,720, test 25,531; GDELT `"reconciles": true`.
- `evidence/pipeline/stage_times_<run_id>.csv`: seconds per stage; the total is printed last.
- `evidence/pipeline/ids_<run_id>.txt`: YARN application IDs, Hadoop job IDs and Hive query IDs (execution evidence).

If a stage fails, the script stops there. Fix the cause and rerun with a **new** run_id (outputs are never overwritten).

## C4. Quick look in Hive (optional)
```bash
hive -e "SELECT split, COUNT(*), SUM(CASE WHEN eligible THEN 1 ELSE 0 END) FROM features.weekly GROUP BY split;"
```

## C5. Bring the evidence back and terminate
Same as Task 1 B7 and B8, with the pipeline folder included:
```bash
cd ~/quantresearchstuff
tar czf ~/task2_evidence_20261105-1400-full-ref.tar.gz evidence
```
Then from your laptop:
```powershell
cd $HOME\Downloads
scp -i <key>.pem hadoop@<primary-dns>:task2_evidence_20261105-1400-full-ref.tar.gz .
```
Then terminate the cluster. HDFS outputs are lost with the cluster; the evidence files and the code are what the report cites. If MODEL or ANLY need `features.weekly` in a later session, they rerun C2 and C3 (or copy `/inf2006/features/<run_id>/` to S3 before terminating: `s3-dist-cp --src hdfs:///inf2006/features/<run_id> --dest s3://<bucket>/inf2006/features/<run_id>`).
