# Development-profile timing runs (run set A): step-by-step guide

You will start one Amazon EMR cluster, run the project's pipeline **three times on the development profile** (a small, fixed slice of the data), put the results on GitHub and shut the cluster down. You do not need to know the project or edit any code. Follow every step in order.

**Why:** section 6 of our report must compare the development profile with the full profile, three timed repetitions each. The three full-profile runs are done. These three dev runs are the other half.

- **Time:** about 1 hour 30 minutes in total (cluster start about 15 minutes, three runs of about 10 to 15 minutes each, GitHub about 15 minutes).
- **Cost:** about USD 1. **Always do step 11 (terminate)** at the end, even if something went wrong.
- **What you need:**
  - An AWS account whose S3 bucket already holds the project data under `inf2006/` (folders `raw/`, `predictions/`, `eval/`). That is Hsy's account, or a teammate account that copied the data with `teammate_full_rerun_guide` step 3 and has not deleted it.
  - A Windows laptop with PowerShell, or a Mac with Terminal.
  - A GitHub account with write access to `jaecodess/quantresearchstuff` and a token (step 9.1).
  - The bucket name. Wherever this guide says `<bucket>`, type it instead, without the `< >`.

## What a run does
Each run is one command: `perf/run_pipeline.sh --profile dev`. It times, in order:
- **Task 2** on the dev slice: about 50 of the 505 stocks (fixed rule: `crc32(ticker) % 10 == 0`) and the 31 GDELT archives of January 2016 (instead of 731). Price audit (Hadoop Streaming), cleaning and features (PySpark), Hive tables, GDELT parse (Hadoop Streaming), reconciliation.
- **Task 3 validation:** the 8 model configurations fitted on 2013 to 2015 and scored on 2016. **2017 is never scored**: the test set was evaluated once, in Task 3 (decision D-025).
- **Task 4:** the analyses, on the dev stocks only, using the stored model predictions.

It also samples memory and disk use on every node every 15 seconds. Each run gets a new run name and new output folders; nothing is reused.

**Do not:** change any cluster setting, run any other script (especially anything with `test` in the command), or delete anything in S3.

---

## Step 1. Sign in to AWS
1. Sign in to the account that holds the data.
   - **AWS Academy Learner Lab:** open the course, **Modules**, **Launch AWS Academy Learner Lab**, **Start Lab**, wait for the green dot next to **AWS**, click **AWS**.
   - **Normal AWS account:** https://console.aws.amazon.com
2. Top-right corner: region **N. Virginia** (`us-east-1`). Everything below happens in this region.
3. Check the data is there: open **S3**, click the bucket, open `inf2006/`. You must see `raw/`, `predictions/` and `eval/`.

## Step 2. Key pair
If you already have a `.pem` key file for this account (for example from an earlier guide), skip to step 3 and use it. Otherwise:
1. Open **EC2**, left menu **Key Pairs**, **Create key pair**. Name `inf2006-<yourname>`, type **RSA**, format **.pem**, **Create key pair**. The file downloads to **Downloads**. Wherever this guide says `<key>`, type the file name without `.pem`.
2. **Windows only** (PowerShell):
   ```powershell
   cd $HOME\Downloads
   icacls .\<key>.pem /inheritance:r
   icacls .\<key>.pem /grant:r "$($env:USERNAME):(R)"
   ```
   **Mac only:** `chmod 400 ~/Downloads/<key>.pem`

## Step 3. Note a public subnet
**VPC**, **Subnets**, click a subnet of the default VPC whose **Auto-assign public IPv4 address** is **Yes**, copy its **Subnet ID**.

## Step 4. Create the cluster
**EMR**, **Clusters**, **Create cluster**. Set exactly these values; leave everything else as it is:

| Section | Setting | Value |
|---|---|---|
| Name and applications | Name | `inf2006-dev-<yourname>` |
| | Amazon EMR release | **emr-7.10.0** |
| | Application bundle | **Custom**: tick only **Hadoop**, **Hive**, **Spark**, **JupyterEnterpriseGateway**, **Livy** |
| | AWS Glue Data Catalog settings | **untick both boxes** |
| Cluster configuration | Configuration type | **Instance groups** |
| | Primary | **m5.xlarge** |
| | Core | **m5.xlarge**; **Add EBS volume**: **gp3**, **100** GiB, IOPS **3000**, Throughput **125** |
| | Task 1 of 1 | **Remove instance group** |
| Cluster scaling and provisioning | Choose an option | **Set cluster size manually**, Core instance count **2**, **On-Demand** |
| Networking | VPC / Subnet | default VPC / the subnet from step 3 |
| Cluster termination | Termination option | **Automatically terminate cluster after idle time**, **1 hour** |
| Software settings | Enter configuration | the box below |
| Security configuration and EC2 key pair | Amazon EC2 key pair for SSH | your key pair |
| IAM roles | Service role | `EMR_DefaultRole` if listed, otherwise **Create a service role** |
| | EC2 instance profile | `EMR_EC2_DefaultRole` (Learner Lab: or `LabInstanceProfile`) if listed, otherwise **Create an instance profile** with **All S3 buckets in this account with read and write access** |

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
These must match exactly: it is the same reference configuration as the full-profile runs, so the times are comparable. Click **Create cluster** and wait until the status is **Waiting** (10 to 15 minutes).

## Step 5. Connect
1. Cluster page, **Properties**, **Network and security**, the **Primary node** security group link, the group ID, **Inbound rules**, **Edit inbound rules**, **Add rule**: Type **SSH**, Source **My IP**, **Save rules**.
2. **Summary** tab: copy the **Primary node public DNS**.
3. PowerShell (or Terminal):
   ```powershell
   cd $HOME\Downloads
   ssh -i <key>.pem hadoop@<primary-public-dns>
   ```
   Type `yes` the first time. You are in when you see the EMR logo and `[hadoop@ip-... ~]$`.

## Step 6. Prepare the cluster and stage the data (about 5 minutes, not timed)
Paste one line at a time. Put your bucket name in the last line:
```bash
sudo yum install -y git tmux python3-numpy
git clone https://github.com/jaecodess/quantresearchstuff.git
cd ~/quantresearchstuff
ls perf/run_pipeline.sh perf/make_dev_input.py
tmux new -s dev
```
Inside tmux (green bar at the bottom), copy the raw data from S3 into HDFS and verify it (about 2 minutes). Put your bucket name in:
```bash
cd ~/quantresearchstuff
bash corpus/stage_to_hdfs.sh s3://<bucket>/inf2006/raw $(date -u +%Y%m%d-%H%M)-dev-stage
```
The end of the output is a summary per source: every `"problems"` line must say `0`, and `"files_checked"` must be `1047`. If not, stop and message Hsy.

## Step 7. Run the three repetitions (about 30 to 45 minutes)
Still inside tmux. Put your bucket name in the second line, then paste the rest:
```bash
RAW=/inf2006/raw/prices/plotly-datasets-0c447c4/acquired=2026-11-03
S3=s3://<bucket>/inf2006
for rep in 1 2 3; do bash perf/run_pipeline.sh --profile dev --config ref --rep $rep --raw-dir "$RAW" --s3-root "$S3"; sleep 60; done
```
- The first run also copies the stored Task 3 predictions from S3 into HDFS and builds the dev price file. Both happen before timing starts.
- Each run prints `=== task2 started`, `=== task3 started`, `=== task4 started` and ends with a line `--- <run name> status=ok`.
- You can detach (**Ctrl+b**, then **d**) and come back later with `tmux attach -t dev` after reconnecting (step 5.3).
- If a run ends with `status=failed`, let the loop carry on. A failed run still writes its evidence, and section 6 reports failures. Tell Hsy which one.

## Step 8. Check the results
```bash
cd ~/quantresearchstuff
python3 perf/timings.py summary
grep -h "^status=\|^check=" evidence/perf/*-dev-ref-r*/meta.txt
cat evidence/perf/*-dev-ref-r*/resources_peak.csv
```
- The summary line `dev/ref: 3 ok of 3 runs, median ... s (range ...)` is the main result. Copy it into your notepad.
- `status=ok` three times. Runs 2 and 3 should say `check=same results as <run 1>`. If one says `DIFFERENT`, keep it: it is evidence, not something to fix. The random-forest scores can differ slightly between runs.
- `resources_peak.csv` shows peak memory and disk per node (node names are hidden).

## Step 9. Put the results on GitHub (about 15 minutes)
**9.1** You need a GitHub account with write access to `jaecodess/quantresearchstuff` and a **classic token** with the **repo** scope: GitHub, profile picture, **Settings**, **Developer settings**, **Personal access tokens**, **Tokens (classic)**, **Generate new token (classic)**, expiry 7 days, tick **repo**, **Generate token**, copy it (starts with `ghp_`). Never put it in a file.

**9.2 Backup in S3** (bucket name in the last line):
```bash
cd ~/quantresearchstuff
tar czf ~/dev_runs.tar.gz evidence/perf $(find evidence -name '*dev-stage*')
aws s3 cp ~/dev_runs.tar.gz s3://<bucket>/inf2006/evidence/
```

**9.3 Hide the bucket name and server addresses** (bucket name in the first line; leave `<bucket>` and `<primary-dns>` in the `sed` line as written):
```bash
cd ~/quantresearchstuff
BUCKET=<bucket>
grep -rlE "$BUCKET|compute-1\.amazonaws\.com" evidence | xargs -r sed -i -E "s#$BUCKET#<bucket>#g; s#ec2-[0-9-]+\.compute-1\.amazonaws\.com#<primary-dns>#g"
python3 scripts/check_hygiene.py
git status --short
```
Hygiene must print `hygiene: 0 problem(s)`. `git status` must show only new files (`??`) under `evidence/`. The `raw/` folders inside `evidence/perf/` are ignored on purpose (they contain server names).

**9.4 Progress note** (change the GitHub username and the summary numbers):
```bash
echo "- $(date -u +%F) @<your-github-username>: Run set A on EMR: development profile (D-024), reference config, 3 repetitions with perf/run_pipeline.sh (Task 2, Task 3 validation, Task 4; 2017 never scored, D-025); dev/ref median <...> s, range <...> s; memory and disk peaks per node. Evidence: evidence/perf/timings.csv, evidence/perf/summary.csv (PERF-06)" >> docs/progress/performance.md
python3 scripts/progress.py status
python3 scripts/progress.py check
```
The last line must print `0 errors`.

**9.5 Commit and push** (change the placeholders):
```bash
git config user.name "<Your Name>"
git config user.email "<your GitHub email>"
git switch -c ws/perf/dev-runs-<yourname>
git add evidence docs/progress
git commit -m "PERF: run set A, development profile x3 (EMR evidence)"
git push -u origin ws/perf/dev-runs-<yourname>
```
Username: your GitHub username. Password: the token from 9.1 (nothing shows while pasting). Open the link it prints, check **base** is `main`, title `PERF: run set A, development profile x3 (EMR evidence)`, paste the summary line from step 8 under **Evidence**, **Create pull request**, and send Hsy the link.

## Step 10. If something goes wrong
| What you see | What to do |
|---|---|
| `REFUSED: tracked files have uncommitted changes` | Run `git status`. Undo the change it lists with `git checkout -- <file>`, then run step 7 again. |
| `no .../predictions/20261106-1400-full-ref in HDFS; pass --s3-root` | The `S3=` line was missing or wrong. Fix it and run step 7 again. |
| `could not copy s3://...` | The bucket name is wrong, or the bucket has no `predictions/` folder (step 1.3). |
| `outputs for ... already exist` | Two runs started in the same minute. Wait one minute and run that repetition again: `bash perf/run_pipeline.sh --profile dev --config ref --rep <n> --raw-dir "$RAW" --s3-root "$S3"`. |
| `status=failed` | Keep it. Look at `tail -50 evidence/perf/<run name>/run.log` and send it to Hsy. You can add one more repetition with `--rep 4`. |
| Connection dropped | Reconnect (step 5.3), then `tmux attach -t dev`. Nothing is lost. |
| `git push` fails | `Authentication failed`: use the token, not your password. `403` / `Permission denied`: ask jaecodess to add you as a collaborator. |

Never run `modeling/run_task3.sh test` or `modeling/run_model.py test`.

## Step 11. Terminate the cluster (do not skip)
**EMR**, **Clusters**, tick `inf2006-dev-<yourname>`, **Terminate**, confirm. Wait for **Terminated**.
