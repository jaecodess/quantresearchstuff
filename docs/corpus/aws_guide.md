# Task 1 on AWS: step-by-step guide

Builds the raw corpus on AWS, then produces the Task 1 evidence on EMR. Do Part A once. Do Part B in every EMR session (HDFS is wiped when the cluster is terminated; S3 keeps the raw files).

Placeholders: `<bucket>` your S3 bucket, `<key>.pem` your EC2 key pair, `<primary-dns>` the cluster's primary node address, `<run_id>` = `YYYYMMDD-HHMM-<profile>-<config>`, for example `20261103-1400-full-ref`. Never commit real bucket names, keys or addresses.

Rough time: Part A about 1 to 1.5 hours (mostly the GDELT download running by itself). Part B about 30 to 45 minutes.

---

## Part A: one-time setup and acquisition

### A0. Before you start
- **Region:** pick one and use it for everything (S3, key pair, EMR). `us-east-1` (N. Virginia) is the usual choice; AWS Academy Learner Labs normally allow only `us-east-1` or `us-west-2`. Select it in the region menu (top right of the console) before each step.
- **Account:** agree with the team which AWS account runs the assessed work, and that the bucket lives there.
- **Cost:** a running cluster is about USD 0.80 per hour (3 × m5.xlarge plus EMR fee, disks and public IPs). S3 storage for about 10 GB is about USD 0.25 per month.

### A1. Create the S3 bucket
Console → **S3** → **Buckets** → **Create bucket**. Fill in each section as follows:

| Section | Setting | Value | Why |
|---|---|---|---|
| General configuration | Bucket type | **General purpose** | |
| | Bucket name | `inf2006-<team-code>-raw-<random 4 digits>` (lowercase, no spaces, globally unique) | |
| | AWS Region | **Same region as EMR** (e.g. `us-east-1`) | Same-region transfer to EMR is free and faster |
| Object Ownership | | **ACLs disabled (recommended)** | Access is controlled by IAM only |
| Block Public Access | Block all public access | **On** (all 4 boxes ticked) | Raw data and evidence stay private |
| Bucket Versioning | | **Enable** | An overwritten or deleted raw file can be recovered; supports "raw data is immutable" |
| Tags | | optional, e.g. `project = inf2006` | Easier to find costs in Billing |
| Default encryption | Encryption type | **SSE-S3** (Amazon S3 managed keys) | Free, no key management |
| | Bucket Key | **Enable** | |
| Advanced settings | Object Lock | **Disable** | Not needed; versioning is enough |

Click **Create bucket**. Then open the bucket and create the folders. The paths contain a nested folder, so you actually create four folders. The S3 console makes one folder at a time, and you type only the name, without the slash.

**At the top level of the bucket**

1. Click **Create folder**, type `inf2006`, then **Create folder**.
2. Click **Create folder** again, type `emr-logs`, then **Create folder**.

**Inside `inf2006`** (click it to open it first)

3. Click **Create folder**, type `raw`, then **Create folder**.
4. Click **Create folder**, type `evidence`, then **Create folder**.

The finished layout:

```
<bucket>/
  emr-logs/
  inf2006/
    raw/
    evidence/
```

Leave **Server-side encryption** on the create-folder screen at its default, which uses the bucket's SSE-S3 setting.

Leave `raw/` empty. `acquire.py` creates the `prices/` and `gdelt/` folders inside it when it runs. When you set up EMR in A3, the log location is `s3://<bucket>/emr-logs/`.

### A2. EC2 settings: key pair and network
You do not launch EC2 instances yourself; EMR does. You only need a key pair and to check the network.

**Key pair** (skip if you already have one in this region): Console → **EC2** → **Network & Security** → **Key pairs** → **Create key pair**.

| Setting | Value |
|---|---|
| Name | `inf2006-<yourname>` |
| Key pair type | **RSA** |
| Private key file format | **.pem** (Mac/Linux/Windows OpenSSH) or **.ppk** (PuTTY) |

The file downloads once. Keep it outside the repo. On Mac/Linux: `chmod 400 ~/Downloads/inf2006-<yourname>.pem`.

**Network** (check once): Console → **VPC** → **Your VPCs**: a **default VPC** should exist. Under **Subnets**, pick one subnet in it and check **Auto-assign public IPv4 address = Yes**. Write down its subnet ID (`subnet-...`). A public subnet gives the cluster internet access for the downloads without a NAT gateway (a NAT gateway costs extra; do not create one).

**Security groups:** EMR creates two on first launch, `ElasticMapReduce-master` (primary) and `ElasticMapReduce-slave` (core). The only change you make is the SSH rule in A4.

### A3. Launch the EMR cluster
Console → **EMR** → **EMR on EC2: Clusters** → **Create cluster**. Go through the page top to bottom:

| Section | Setting | Value |
|---|---|---|
| Name and applications | Name | `inf2006-<run_id>` |
| | Amazon EMR release | **emr-7.10.0** |
| | Application bundle | **Custom**: tick **Hadoop, Hive, Spark, JupyterEnterpriseGateway, Livy**; untick everything else |
| | AWS Glue Data Catalog settings | **Untick both** (use the cluster's own Hive metastore; DDL is recreated from `hive/*.sql`, D-013) |
| | Operating system | **Amazon Linux release** (default); **Apply latest updates** ticked |
| Cluster configuration | Configuration type | **Instance groups** |
| | Primary | Instance type **m5.xlarge**; EBS root volume default; no extra EBS volume |
| | Core | Instance type **m5.xlarge**; **Add EBS volume**: **gp3, 100 GiB, IOPS 3000, Throughput 125 MiB/s**, 1 volume per instance (brief update: 100 GiB per core node for Learner Lab limits; the root volume is separate, leave it at the default) |
| | Task 1 of 1 | **Remove instance group** |
| Cluster scaling and provisioning | Choose an option | **Set cluster size manually** |
| | Core instance count | **2** (Task: 0) |
| | Provisioning | **On-Demand** (not Spot: a lost Spot node could fail a timed run) |
| Networking | VPC / Subnet | default VPC, the **public subnet** you noted in A2 |
| | EC2 security groups | leave the EMR-managed defaults |
| Cluster termination and node replacement | Termination option | **Automatically terminate cluster after idle time**, idle time **1 hour** |
| | Use termination protection | **Off** |
| | Unhealthy node replacement | On (default) |
| Bootstrap actions | | none |
| Cluster logs | Publish cluster-specific logs to Amazon S3 | **On**, location `s3://<bucket>/emr-logs/` (job logs become evidence; they survive termination) |
| Tags | | optional `project = inf2006` |
| Software settings | Enter configuration | paste the JSON below |
| Security configuration and EC2 key pair | Security configuration | none |
| | Amazon EC2 key pair for SSH | your key from A2 |
| Identity and Access Management (IAM) roles | Amazon EMR service role | **Choose an existing service role** → `EMR_DefaultRole` (or **Create a service role** if none exists) |
| | EC2 instance profile | **Choose an existing instance profile** → `EMR_EC2_DefaultRole` (AWS Academy: use the role the lab provides, often `EMR_EC2_DefaultRole` or `LabInstanceProfile`). It must allow read/write on `<bucket>` |

Software settings JSON (same as the reference configuration; PERF-04 owns it, so use theirs if they publish one in `config/`):

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

Click **Create cluster**. Wait until the status is **Waiting** (about 10 minutes). On the cluster summary, check the release, 1 primary + 2 core, and that the configuration tab shows the two classifications.

**Same cluster from the AWS CLI** (optional, repeatable, good for Appendix A). Save the two JSON files, then run from your laptop with the AWS CLI configured:

`emr-instance-groups.json`
```json
[
  {"Name": "Primary", "InstanceGroupType": "MASTER", "InstanceType": "m5.xlarge", "InstanceCount": 1},
  {"Name": "Core", "InstanceGroupType": "CORE", "InstanceType": "m5.xlarge", "InstanceCount": 2,
   "EbsConfiguration": {"EbsOptimized": true, "EbsBlockDeviceConfigs": [
     {"VolumeSpecification": {"VolumeType": "gp3", "SizeInGB": 100, "Iops": 3000, "Throughput": 125},
      "VolumesPerInstance": 1}]}}
]
```
`emr-configurations.json`: the software settings JSON above.

```bash
aws emr create-cluster \
  --region us-east-1 \
  --name "inf2006-<run_id>" \
  --release-label emr-7.10.0 \
  --applications Name=Hadoop Name=Hive Name=Spark Name=JupyterEnterpriseGateway Name=Livy \
  --instance-groups file://emr-instance-groups.json \
  --configurations file://emr-configurations.json \
  --ec2-attributes KeyName=inf2006-<yourname>,SubnetId=<subnet-id>,InstanceProfile=EMR_EC2_DefaultRole \
  --service-role EMR_DefaultRole \
  --auto-termination-policy IdleTimeout=3600 \
  --log-uri s3://<bucket>/emr-logs/
```
It prints a cluster ID (`j-...`); record it in `evidence/corpus/` as part of the run record.

### A4. Connect to the primary node
1. Cluster page → **Properties** / **Network and security** → **EC2 security groups (firewall)** → click the **Primary node** group (`ElasticMapReduce-master`).
2. **Inbound rules** → **Edit inbound rules** → **Add rule**:

   | Type | Protocol | Port | Source | Description |
   |---|---|---|---|---|
   | SSH | TCP | 22 | **My IP** | `ssh from <yourname>` |

   Never use `0.0.0.0/0` (open to the whole internet). If your IP changes (new Wi-Fi), edit the rule again.
3. Copy the **Primary node public DNS** from the cluster summary.
4. From your laptop:
   ```bash
   ssh -i <key>.pem hadoop@<primary-dns>
   ```
   Windows: same command in PowerShell (OpenSSH is built in), or PuTTY with the `.ppk` file.

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
Storage budget: 2 core nodes × 100 GiB = 200 GiB physical, which is about **100 GiB logical** at replication 2, shared with YARN shuffle and temp space. The raw corpus uses about 19.2 GB physical (GDELT ZIPs 9.61 GB × 2) plus about 60 MB for prices. Never expand GDELT in HDFS: 54.71 GB × 2 ≈ 109 GB would exceed the whole cluster. Record `DFS Remaining` before and after each run.
Copy the output into `evidence/corpus/hdfs_layout_<run_id>.txt` (`... > evidence/corpus/hdfs_layout_<run_id>.txt`).

### B3. Profile the price file on EMR
```bash
cd ~/quantresearchstuff && spark-submit --deploy-mode client corpus/profile_prices.py --input "/inf2006/raw/prices/plotly-datasets-0c447c4/acquired=*/all_stocks_5yr.csv" --out evidence/corpus/prices_profile_20261103-2250-full-ref --run-id 20261103-2250-full-ref 2>&1 | tee evidence/corpus/prices_profile_20261103-2250-full-ref.log
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
The evidence files the cluster created exist only on the cluster, and **they are deleted when you terminate it**. Without them in the repo there is no proof of the EMR run, so do this before B8. The simplest way is to bundle everything into one file and download it.

**1. In the cluster window, bundle the evidence:**
```bash
cd ~/quantresearchstuff
tar czf ~/task1_evidence_<run_id>.tar.gz evidence manifests
ls -lh ~/task1_evidence_<run_id>.tar.gz
```

**2. In a NEW PowerShell window on your laptop** (leave the cluster window open), from the folder with your key:
```powershell
cd $HOME\Downloads
scp -i <key>.pem hadoop@<primary-dns>:task1_evidence_<run_id>.tar.gz .
```
`scp` works like `ssh` but copies a file. The `.` at the end means "into this folder"; the file lands in Downloads.

**3. Get it into the repo.** Unpack it into your laptop clone of the repo on your branch (`tar xzf task1_evidence_<run_id>.tar.gz`), or hand the file to whoever is committing. Run `make hygiene` to check nothing secret is inside, then commit `evidence/` and `manifests/`, replace the *dev* numbers in `docs/corpus/` with the EMR numbers, and update the task file.

**Backup** in case `scp` gives you trouble: copy it to S3 from the cluster, then download it from the S3 console.
```bash
aws s3 cp ~/task1_evidence_<run_id>.tar.gz s3://<bucket>/inf2006/evidence/
```

After you have the file safely on your laptop, do **B8: terminate the cluster**.

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
| Cluster fails with "no default VPC" or cannot download | Use a public subnet with auto-assign public IPv4 (A2) |
| `EMR_DefaultRole` not found | In A3 choose **Create a service role**, or in AWS Academy pick the lab role |
| `AccessDenied` on `aws s3 cp` | Cluster role lacks S3 access to `<bucket>`; check the EC2 instance profile role |
| GDELT download very slow | Normal range is 20 to 40 minutes; try `--workers 8`, keep `tmux` running |
| `raw corpus already in HDFS` | Stage was already done this session; nothing is overwritten |
| `files_ok` lower than expected in B1 | Open `hdfs_verify_<run_id>.csv`; `missing_in_hdfs` means the copy failed, a hash mismatch means a corrupted copy: delete that one file in HDFS and rerun B1 |
| Spark job stuck at 0 executors | Another job holds the 2 executors; `yarn application -list`, then `yarn application -kill <id>` |
