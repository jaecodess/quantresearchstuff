# Appendix A: EMR setup and fresh-session rerun guide

This guide reruns the whole project on a **new** EMR cluster that starts from the raw files in S3 only. It executes the project notebook [`perf/inf2006_pipeline.ipynb`](../../perf/inf2006_pipeline.ipynb) on the cluster and saves the executed copy (with outputs) in `notebooks/`. That copy is the `.ipynb` the brief asks for. Each stage ends with verification checks against the committed evidence of the original runs; a check row records the check, command, expected value, actual value, evidence location and result.

What it does, in order (one notebook section each, every section calls the owned scripts):

| Section | Script | Checks against |
|---|---|---|
| 0 Environment | versions, git commit, YARN nodes | (recorded only) |
| 1 Task 1 staging | `corpus/stage_to_hdfs.sh` | 1,047 files match their manifests, 0 problems |
| 2 Task 2 pipeline | `spark/run_task2.sh` | 605,887 clean rows, eligible rows per split, 144,207,180 GDELT events, 97 news weeks |
| 3a Task 3 validation | `modeling/run_task3.sh validate` | same frozen config `lr_f1_r001`, same validation AUCs, 2017 not read |
| 3b Task 3 reload | `modeling/run_model.py reload` | saved model reproduces all 25,531 stored test predictions, no retraining |
| 4 Task 4 analytics | `analytics/run_task4.sh` | Q1, Q2 and Q3a results equal the original run |
| 5 Summary | | writes `evidence/rerun/` |

The 2017 test set is **not** evaluated again (it was evaluated once, Task 3). Time: about 75 minutes; cost about USD 1.20.

## A1. Prerequisites and cluster configuration
- S3 bucket with the raw corpus under `s3://<bucket>/inf2006/raw/` (Task 1, [aws_guide.md](../corpus/aws_guide.md) and [aws_aux_sources.md](../corpus/aws_aux_sources.md)), the Task 3 predictions under `s3://<bucket>/inf2006/predictions/20261106-1400-full-ref/` and evaluation under `.../eval/20261106-1400-full-ref/` (Task 3 D4).
- An EC2 key pair (`.pem`) and the reference cluster: EMR 7.10.0 with Hadoop, Hive, Spark; 1 primary + 2 core m5.xlarge; gp3 100 GiB per core node; HDFS replication 2; Spark 2 executors x 2 cores x 6 GiB, dynamic allocation off. Create and connect exactly as in Task 1 A3 and A4.

## A2. Environment preparation (on the primary node)
At the `[hadoop@ip-... ~]$` prompt:
```bash
sudo yum install -y git tmux python3-numpy
python3 -m pip install --user nbconvert==7.16.4 nbclient==0.10.0 ipykernel==6.29.5 jupyter_client==8.6.3 jupyter_core==5.7.2
python3 -m nbconvert --version
git clone https://github.com/jaecodess/quantresearchstuff.git
cd ~/quantresearchstuff
ls perf/inf2006_pipeline.ipynb
```
`nbconvert --version` prints `7.16.4`; `ls` prints the notebook path. If `ls` says "No such file", the notebook is not on `main` yet: run `git fetch origin && git switch -c ws/perf/notebook-rerun --track origin/ws/perf/notebook-rerun`.

## A3. Data acquisition and staging
Nothing to download: the raw files are already in S3 (acquired once, with manifests in `manifests/`). The notebook's section 1 copies them into HDFS and verifies every file. To re-acquire from the original publishers instead, follow Task 1 B1 to B2 and the SEC and Wikinews guide first.

## A4. Execute the notebook
Start tmux so the run survives a dropped connection (detach Ctrl-b then d; reattach `tmux attach -t rerun`):
```bash
tmux new -s rerun
cd ~/quantresearchstuff
export RERUN_ID=20261116-1000-rerun
export S3_ROOT=s3://<bucket>/inf2006
bash perf/run_notebook.sh
```
Use the date and time you start as `RERUN_ID`. The terminal stays quiet while the notebook runs. To watch progress, open a second tmux window (Ctrl-b then c) and run:
```bash
cd ~/quantresearchstuff && tail -f evidence/rerun/live_20261116-1000-rerun.log
```
Switch between windows with Ctrl-b then n. The GDELT stage takes about 52 minutes on its own.

When it ends, the first window prints:
```
RESULT: 19 of 19 checks passed
```
Any check that did not pass is listed under that line. Report it honestly; do not rerun until it passes.

## A5. Reload the saved model and verify
Done inside the notebook (section 3b): the PipelineModel in `models/20261106-1400-full-ref/pipeline_model` is copied to HDFS, loaded in a new Spark session and used to predict the test rows again, without retraining. Result: `evidence/rerun/reload_20261106-1400-full-ref.json` (`"passed": true`, `"max_abs_diff"` 0.0).

## A6. Bring the evidence back
```bash
tar czf ~/rerun_20261116-1000-rerun.tar.gz notebooks evidence/rerun $(find evidence -name '*20261116-1000-rerun*' -not -path 'evidence/rerun/*')
```
From a new PowerShell window in your Downloads folder (note the space and dot at the end):
```powershell
scp -i <key>.pem hadoop@<primary-public-dns>:~/rerun_20261116-1000-rerun.tar.gz .
```
Upload the `.tar.gz`, then terminate the cluster (EMR console, Terminate). S3 keeps the raw data.

## A7. Troubleshooting
| Symptom | Fix |
|---|---|
| `No module named nbconvert` | Rerun the `pip install --user` line in A2; always call it as `python3 -m nbconvert`. |
| `No module named numpy` in section 3 | `sudo yum install -y python3-numpy` (fallback `sudo python3 -m pip install numpy==1.26.4`), then rerun with a new `RERUN_ID`. |
| A section fails | The notebook stops and names the stage. Read the end of `evidence/rerun/live_<RERUN_ID>.log`, fix the cause, then rerun with a new `RERUN_ID` (every run writes to new folders). |
| A check shows `FAIL` | Keep it. The record is evidence; report it with the reason in the report. |
| Connection dropped | `ssh` again, then `tmux attach -t rerun`. |
| `s3-dist-cp` cannot find predictions | Copy them first as in Task 3 D4 (`s3://<bucket>/inf2006/predictions/20261106-1400-full-ref`). |

Never run `modeling/run_task3.sh test` or `run_model.py test`: the 2017 test set is evaluated once.

## A8. Cleanup
Terminate the cluster after downloading. HDFS is deleted with it; S3 keeps the raw corpus, features, predictions and evaluation tables. Delete S3 data only after the project is marked.

## Recorded for the report (Appendix A)
| Item | Value |
|---|---|
| Data versions | Prices: plotly/datasets commit 0c447c4, acquired 2026-11-03; GDELT 1.0 daily, acquired 2026-11-03; SEC EDGAR and Wikinews 20261001, acquired 2026-11-10 |
| Training cutoff | 31 December 2015 (train 2013-2015, validation 2016, test 2017) |
| Software | EMR 7.10.0: Hadoop 3.4.1-amzn-2, Spark 3.5.5-amzn-1, Hive 3.1.3-amzn-19, Python 3.9.25 |
| Random seed | 2026 (model fitting and all bootstraps) |
| Saved model | `models/20261106-1400-full-ref/pipeline_model` (frozen config `lr_f1_r001`) |
| Fresh-session rerun | `evidence/rerun/rerun_<RERUN_ID>.json`, `checks_<RERUN_ID>.csv`, executed notebook `notebooks/inf2006_pipeline_<RERUN_ID>.ipynb` |
