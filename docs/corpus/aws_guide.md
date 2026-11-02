# Task 1 on AWS: step-by-step guide

Builds the raw corpus on AWS, then produces the Task 1 evidence on EMR. Do Part A once. Do Part B in every EMR session (HDFS is wiped when the cluster is terminated; S3 keeps the raw files).

Placeholders: `<bucket>` your S3 bucket, `<key>.pem` your EC2 key pair, `<primary-dns>` the cluster's primary node address, `<run_id>` = `YYYYMMDD-HHMM-<profile>-<config>`, for example `20261103-1400-full-ref`. Never commit real bucket names, keys or addresses.

Rough time: Part A about 1 to 1.5 hours (mostly the GDELT download running by itself). Part B about 30 to 45 minutes.

---

## Part A: one-time setup and acquisition

### A1. Create the S3 bucket
1. AWS console → **S3** → **Create bucket**.
2. Name: something unique, e.g. `inf2006-team-<code>-raw`. Region: the one you will run EMR in (e.g. `us-east-1`). Keep "Block all public access" on.
3. Create. Inside it, the scripts will create `inf2006/raw/...` themselves.

### A2. Create an EC2 key pair (skip if you have one)
1. Console → **EC2** → **Key pairs** → **Create key pair**, type RSA, format `.pem`.
2. Save the file. On Mac/Linux: `chmod 400 <key>.pem`.

### A3. Launch the EMR cluster
Console → **EMR** → **Create cluster**:

| Setting | Value |
|---|---|
| Name | `inf2006-<run_id>` |
| Release | **emr-7.10.0** |
| Applications | **Hadoop, Hive, Spark, JupyterEnterpriseGateway** (Livy if you want notebooks) |
| Instance groups | Primary: 1 × **m5.xlarge**. Core: 2 × **m5.xlarge**. **Remove the task group** |
| Core EBS | 1 volume, **gp3, 200 GiB**, 3,000 IOPS, 125 MiB/s |
| Scaling | **No** managed scaling / autoscaling |
| Auto-termination | On, idle 1 hour (saves credits if you forget) |
| Software settings | Paste the JSON below |
| Security | Your EC2 key pair. Roles: default EMR roles (in AWS Academy use `EMR_DefaultRole` / `EMR_EC2_DefaultRole` or `LabRole` as offered) |

```json
[
  {"Classification": "hdfs-site", "Properties": {"dfs.replication": "2"}},
  {"Classification": "spark-defaults", "Properties": {
    "spark.dynamicAllocation.enabled": "false",
    "spark.executor.instances": "2",
    "spark.executor.cores": "2",
    "spark.executor.memory": "6g",
    "spark.executor.memoryOverhead": "2g",
    "spark.driver.memory": "2g"}}
]
```
(The reference configuration is owned by PERF-04; if they publish `config/` files, use those instead.)

Wait until the status is **Waiting** (about 10 minutes).

### A4. Connect to the primary node
1. Cluster page → **Security and access** → primary node's **EC2 security group** → **Edit inbound rules** → add **SSH, port 22, source My IP**.
2. Copy the **Primary node public DNS** from the cluster summary.
3. From your laptop:
   ```bash
   ssh -i <key>.pem hadoop@<primary-dns>
   ```

### A5. Get the code onto the cluster
```bash
sudo yum install -y git tmux
git clone https://github.com/jaecodess/quantresearchstuff.git
cd quantresearchstuff
git switch ws/corp/raw-corpus     # until it is merged; afterwards stay on main
python3 --version; spark-submit --version 2>&1 | grep version; hadoop version | head -1
```
Write the three versions down (Appendix A needs them).

### A6. Acquire the price CSV into S3
```bash
python3 corpus/acquire.py prices --s3-raw s3://<bucket>/inf2006/raw
```
Expected: `files ok: 1/1  bytes ok: 29580549`. If it says `sha256_mismatch`, the file at the URL changed: stop and tell the team (the script refuses to store it).

### A7. Acquire the GDELT archives into S3
731 files, about 9.6 GB. Run it inside `tmux` so it survives a dropped SSH connection:
```bash
tmux new -s gdelt
python3 corpus/acquire.py gdelt --s3-raw s3://<bucket>/inf2006/raw --workers 4
# detach: Ctrl-b then d      re-attach later: tmux attach -t gdelt
```
Expected at the end: `files ok: 731/731`. Any `NOT OK` line names the file and the reason (`unavailable_404`, `md5_mismatch`, ...). Rerun only if failures were network errors; a 404 is a real finding for the quality report. Do not swap in other files silently.

If the team decides on a documented subset instead of all 731 days (HANDOVER section 16 Q4), add `--start YYYYMMDD --end YYYYMMDD` and record the decision in the decision log.

### A8. Save the manifests
The two manifests are now in `manifests/` on the cluster and in `s3://<bucket>/inf2006/raw/_manifests/`. Copy them to your laptop (step B7) and commit them. They are the acquisition record.

---

## Part B: every EMR session (staging and evidence)

Start a cluster as in A3 to A5 if you terminated the previous one. Pick a new `<run_id>`.

### B1. Stage S3 → HDFS and verify
```bash
cd ~/quantresearchstuff
bash corpus/stage_to_hdfs.sh s3://<bucket>/inf2006/raw <run_id>
```
This copies with `s3-dist-cp`, makes the raw folders read-only, prints counts, sizes and an `fsck` summary, then re-hashes every HDFS file against the manifests. Expected last lines: `"files_ok": 732, "files_checked": 732` (1 price file + 731 archives). Outputs: `evidence/corpus/stage_<run_id>.log`, `hdfs_verify_<run_id>.csv`, `inventory_<run_id>.json`.

### B2. Check the HDFS layout
```bash
hdfs dfs -ls -R /inf2006/raw | head -20
hdfs dfs -du -s -h /inf2006/raw/prices /inf2006/raw/gdelt
hdfs dfsadmin -report | grep -E "Configured Capacity|DFS Used|DFS Remaining"
```
Copy the output into `evidence/corpus/hdfs_layout_<run_id>.txt` (`... > evidence/corpus/hdfs_layout_<run_id>.txt`).

### B3. Profile the price file on EMR
```bash
spark-submit --deploy-mode client corpus/profile_prices.py \
  --input "/inf2006/raw/prices/plotly-datasets-0c447c4/acquired=*/all_stocks_5yr.csv" \
  --out evidence/corpus/prices_profile_<run_id> --run-id <run_id> \
  2>&1 | tee evidence/corpus/prices_profile_<run_id>.log
grep -o "application_[0-9_]*" evidence/corpus/prices_profile_<run_id>.log | sort -u
```
The application ID is your execution evidence. Check the result matches the development profile:
```bash
python3 - <<'EOF'
import json, glob
dev = json.load(open("docs/corpus/dev_profile_2026-11-02/summary.json"))
emr = json.load(open(sorted(glob.glob("evidence/corpus/prices_profile_*/summary.json"))[-1]))
skip = {"run_id", "input", "profiled_at_utc", "spark_version"}
diff = {k: (dev[k], emr.get(k)) for k in dev if k not in skip and dev[k] != emr.get(k)}
print("identical to dev profile" if not diff else diff)
EOF
```
Expected: `identical to dev profile` (key numbers: 619,040 rows, 505 series, 23 hard-problem rows, 605,887 accepted in the study window).

### B4. GDELT quality audit (needs the PIPE-03 branch `ws/pipe/gdelt-streaming-prototype` merged)
```bash
bash streaming/gdelt_daily/run.sh <run_id>
hive --hivevar run_id=<run_id> -f hive/10_gdelt_daily.sql 2>&1 | tee evidence/corpus/gdelt_audit_<run_id>.txt
```
The Hive output gives the totals for `quality_findings.md` section 2: lines, events, malformed, bad values, duplicates, missing dates, overlapping ID ranges. The job ID is in `evidence/gdelt_daily_<run_id>.log`.

### B5. Record job IDs and versions
```bash
yarn application -list -appStates FINISHED,FAILED,KILLED > evidence/corpus/yarn_apps_<run_id>.txt
{ echo "emr_release=emr-7.10.0"; hadoop version | head -1; spark-submit --version 2>&1 | grep -m1 version; python3 --version; hive --version 2>/dev/null | head -1; } > evidence/corpus/versions_<run_id>.txt
```

### B6. Optional: Hive table over the raw prices (lets Spark and Hive read the raw file)
Owned by PIPE-01; skip if they have done it.

### B7. Bring the evidence back to your laptop
From your **laptop** (not the cluster; keeps GitHub credentials off AWS):
```bash
cd quantresearchstuff
scp -i <key>.pem -r "hadoop@<primary-dns>:quantresearchstuff/evidence/corpus" evidence/
scp -i <key>.pem "hadoop@<primary-dns>:quantresearchstuff/manifests/*.csv" manifests/
```
Check nothing secret is inside (`make hygiene`), then commit on your branch.

### B8. Terminate the cluster
EMR console → cluster → **Terminate**. S3 keeps the raw files and manifests. The next session starts again at B1.

---

## After the EMR run: finish the paperwork

1. Replace every *dev* / `…` figure in `docs/corpus/quality_findings.md`, `inventory.md` and `section2_draft.md` with the EMR numbers, and link the evidence files.
2. Update `docs/progress/corpus.md`: tick tasks with `evidence:` paths, add a progress-log line.
3. `make status && make check`, push, open the PR, ask the CORP owner and the PIPE cross-reviewer to review.

## Troubleshooting

| Problem | Fix |
|---|---|
| SSH times out | Inbound rule for port 22 missing or your IP changed (A4) |
| `AccessDenied` on `aws s3 cp` | Cluster role lacks S3 access to `<bucket>`; check the EC2 instance profile role |
| GDELT download very slow | Normal range is 20 to 40 minutes; try `--workers 8`, keep `tmux` running |
| `raw corpus already in HDFS` | Stage was already done this session; nothing is overwritten |
| `files_ok` lower than expected in B1 | Open `hdfs_verify_<run_id>.csv`; `missing_in_hdfs` means the copy failed, a hash mismatch means a corrupted copy: delete that one file in HDFS and rerun B1 |
| Spark job stuck at 0 executors | Another job holds the 2 executors; `yarn application -list`, then `yarn application -kill <id>` |
