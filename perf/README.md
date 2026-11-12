# PERF: timed runs (HANDOVER section 11)

Everything runs on the EMR **primary node**, from the repo root, inside `tmux`. Local runs are not evidence.

| File | What it does |
|---|---|
| `run_pipeline.sh` | One timed run: Task 2 → Task 3 (frozen Gate 2 model) → Task 4, fresh `run_id`, evidence in `evidence/perf/<run_id>/` (PERF-02) |
| `timings.py` | Merges stage times into `evidence/perf/timings.csv`, per-node memory/disk peaks, YARN app stats, median/range summary, output checks (PERF-01) |
| `sample_resources.sh` | Background sampler started by `run_pipeline.sh` (YARN node memory, HDFS per-datanode usage) |
| `make_dev_input.py` | Derives the dev-profile price file (PERF-03, rule in D-024) |
| `configs/ref.env`, `configs/alt.env` | Reference Spark settings; the one alternative change for run set C (PERF-08) |

Timing scope and the test-once guard are explained in decision **D-023**; the dev profile in **D-024** (both PROPOSED until the team accepts them).

## Before the first run (each new cluster)

1. Cluster exactly as `docs/corpus/aws_guide.md` §A3 (emr-7.10.0, 1 + 2 m5.xlarge, 100 GiB gp3 per core node, D-015). Same cluster settings for every run set.
2. Code on the cluster and up to date (`git pull`), no uncommitted changes to tracked files (the script refuses otherwise).
3. Stage the raw corpus (untimed): `bash corpus/stage_to_hdfs.sh s3://<bucket>/inf2006/raw <run_id>`.

## Run sets

```bash
RAW=/inf2006/raw/prices/plotly-datasets-0c447c4/acquired=2026-11-03   # your staged price folder
```

```bash
for rep in 1 2 3; do bash perf/run_pipeline.sh --profile dev --config ref --rep $rep --raw-dir "$RAW"; done    # A (PERF-06)
```

```bash
for rep in 1 2 3; do bash perf/run_pipeline.sh --profile full --config ref --rep $rep --raw-dir "$RAW"; done   # B (PERF-07)
```

Then choose ONE change from run-set-B evidence (`evidence/perf/summary.csv`, the slowest stages), fill `configs/alt.env`, record it in the decision log, commit, `git pull` on the cluster (PERF-08):

```bash
for rep in 1 2 3; do bash perf/run_pipeline.sh --profile full --config alt --rep $rep --raw-dir "$RAW"; done    # C (PERF-09)
```

A failed run still writes its evidence and counts as a failure in the summary (the brief asks for failures to be reported). Do not delete it; rerun with the same `--rep` a minute later if needed and say so in section 6.

## After every run set

```bash
python3 perf/timings.py summary      # evidence/perf/summary.csv: median, min, max per profile/config and per stage
```

Bring `evidence/perf/` back to your laptop (`aws_guide.md` §B7), commit it, and terminate the cluster. `raw/` snapshots stay on the cluster (git-ignored: they contain hostnames). Check each run's `meta.txt` for `check=same results as ...`; a `DIFFERENT` line is a finding for section 6, not something to hide.

## What lands in `evidence/perf/<run_id>/`

`meta.txt` (profile, config, Spark settings, frozen config, git commit, versions, status, output check), `run.log`, `wrapper_times.csv` (task totals), `pipeline/`, `model/`, `analytics/` (each task's own evidence and stage times), `resources_peak.csv` (per node: peak YARN allocation and node memory, peak HDFS used, min remaining), `apps.csv` (YARN MB-seconds and vcore-seconds per application), `application_ids.txt`, `check.txt`.
