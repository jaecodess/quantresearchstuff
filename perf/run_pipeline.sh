#!/usr/bin/env bash
# One timed PERF run of the whole pipeline (PERF-02): Task 2 -> Task 3 (validation training) -> Task 4.
# Run on the EMR primary node from the repo root, AFTER the raw corpus is staged in HDFS
# (corpus/stage_to_hdfs.sh). Timing starts here (brief): staging, downloads and cluster start-up are not timed.
#
#   bash perf/run_pipeline.sh --profile dev|full --config ref|alt --rep N --raw-dir <hdfs raw prices dir> \
#        [--s3-root s3://<bucket>/inf2006]
#   e.g. bash perf/run_pipeline.sh --profile dev --config ref --rep 1 \
#          --raw-dir /inf2006/raw/prices/plotly-datasets-0c447c4/acquired=2026-11-03 --s3-root s3://<bucket>/inf2006
#   --s3-root is needed once per cluster, to copy the stored Task 3 predictions into HDFS (untimed).
#
# Run sets (HANDOVER section 11): A = dev/ref x3, B = full/ref x3, C = full/alt x3. Run each inside tmux.
#
# Profiles (PERF-03; proposed in D-024, frozen by TEAM-06):
#   full  all staged prices and all staged GDELT archives
#   dev   tickers with crc32(ticker) % DEV_TICKER_MOD == 0 (default 10, about 50 of 505; the same rule as
#         clean_prices.py --ticker-mod) and one GDELT month DEV_GDELT_MONTH (default 201601). The dev price
#         file is derived once into <root>/profiles/ (untimed, like staging); raw data is never changed.
# Configs: perf/configs/ref.env (reference) and perf/configs/alt.env (one change, PERF-08).
#
# What is timed: the Task 2 runner (Streaming, Hive, PySpark cleaning, splits, features, GDELT, reconciliation),
# Task 3 validation (modeling/run_model.py validate: the 8 Gate 1 configs fitted on 2013-15 train rows and
# scored on 2016 validation rows, with baselines and the selection rule) and the Task 4 runner. This is the same
# timed workload as the three full-profile reruns (perf/inf2006_pipeline.ipynb), so dev and full are comparable.
#
# Test-once rule (D-025, replaces D-023): no model is fitted on, scores or evaluates 2017 rows here, and the
# Task 3 test mode is never called. Task 4 reads the stored frozen-model predictions of PRED_RUN (the single
# Task 3 test run), copied into HDFS before timing like the raw corpus; Task 4 joins them to this run's features,
# so a dev run analyses only the dev tickers and weeks.
#
# Environment options: HDFS_ROOT (default /inf2006), PRED_RUN (default 20261106-1400-full-ref), DEV_TICKER_MOD, DEV_GDELT_MONTH, SAMPLE_INTERVAL (s, 15),
#   ALLOW_DIRTY=1 (run with uncommitted changes to tracked files; not for assessed runs).
#
# Evidence: evidence/perf/<run_id>/ (meta.txt, run.log, wrapper_times.csv, pipeline/, model/, analytics/,
#   resources_peak.csv, apps.csv, check.txt; raw/ snapshots are git-ignored) and evidence/perf/timings.csv,
#   evidence/perf/summary.csv. Commit evidence/perf/ after every run set.
set -euo pipefail

usage() { sed -n '2,8p' "$0" >&2; exit 2; }
PROFILE="" CONFIG="" REP="" RAW_DIR="" S3_ROOT=""
while [ $# -gt 0 ]; do
  case "$1" in
    --profile) PROFILE="${2:-}"; shift 2 ;;
    --config) CONFIG="${2:-}"; shift 2 ;;
    --rep) REP="${2:-}"; shift 2 ;;
    --raw-dir) RAW_DIR="${2:-}"; shift 2 ;;
    --s3-root) S3_ROOT="${2:-}"; S3_ROOT="${S3_ROOT%/}"; shift 2 ;;
    -h|--help) usage ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done
[[ "$PROFILE" =~ ^(dev|full)$ ]] || { echo "--profile must be dev or full" >&2; usage; }
[[ "$CONFIG" =~ ^(ref|alt)$ ]] || { echo "--config must be ref or alt" >&2; usage; }
[[ "$REP" =~ ^[1-9]$ ]] || { echo "--rep must be 1-9" >&2; usage; }
[ -n "$RAW_DIR" ] || { echo "--raw-dir is required" >&2; usage; }

ROOT="${HDFS_ROOT:-/inf2006}"
DEV_TICKER_MOD="${DEV_TICKER_MOD:-10}"
DEV_GDELT_MONTH="${DEV_GDELT_MONTH:-201601}"
SAMPLE_INTERVAL="${SAMPLE_INTERVAL:-15}"
RUN_ID="$(date -u +%Y%m%d-%H%M)-${PROFILE}-${CONFIG}-r${REP}"
RUN_DIR="evidence/perf/$RUN_ID"
PRED_RUN="${PRED_RUN:-20261106-1400-full-ref}"
now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

# ---------------------------------------------------------------- configuration
# shellcheck source=perf/configs/ref.env
source perf/configs/ref.env
if [ "$CONFIG" = "alt" ]; then
  # shellcheck source=perf/configs/alt.env
  source perf/configs/alt.env
  [ -n "$CONFIG_NOTE" ] || { echo "REFUSED: perf/configs/alt.env has no change yet (PERF-08)." >&2; exit 2; }
fi
export SPARK_OPTS


# ---------------------------------------------------------------- preflight (untimed)
if [ "${ALLOW_DIRTY:-0}" != "1" ] && ! git diff --quiet HEAD --; then
  echo "REFUSED: tracked files have uncommitted changes; commit or stash them (or ALLOW_DIRTY=1 for a trial)." >&2
  exit 2
fi
hdfs dfs -test -e "$RAW_DIR/all_stocks_5yr.csv" || { echo "no $RAW_DIR/all_stocks_5yr.csv in HDFS" >&2; exit 2; }
for out in "$ROOT/staging/streaming/$RUN_ID" "$ROOT/features/$RUN_ID"; do
  if hdfs dfs -test -e "$out" || [ -e "$RUN_DIR" ]; then
    echo "outputs for $RUN_ID already exist; wait a minute and rerun (run ids are per minute)" >&2; exit 2
  fi
done
# Stored Task 3 predictions for Task 4 (untimed, like staging; copied once per cluster)
for d in predictions eval; do
  if ! hdfs dfs -test -d "$ROOT/$d/$PRED_RUN"; then
    [ -n "$S3_ROOT" ] || { echo "no $ROOT/$d/$PRED_RUN in HDFS; pass --s3-root s3://<bucket>/inf2006 to copy it" >&2; exit 2; }
    s3-dist-cp --src "$S3_ROOT/$d/$PRED_RUN" --dest "hdfs://$ROOT/$d/$PRED_RUN" >/dev/null \
      || { echo "could not copy $S3_ROOT/$d/$PRED_RUN" >&2; exit 2; }
  fi
done
mkdir -p "$RUN_DIR/model" "$RUN_DIR/raw"
LOG="$RUN_DIR/run.log"
WT="$RUN_DIR/wrapper_times.csv"
echo "task,stage,start_utc,end_utc,seconds,exit_code" > "$WT"

if [ "$PROFILE" = "dev" ]; then
  INPUT_DIR="$ROOT/profiles/dev-mod${DEV_TICKER_MOD}/prices/$(basename "$(dirname "$RAW_DIR")")/$(basename "$RAW_DIR")"
  export GDELT_GLOB="$ROOT/raw/gdelt/v1-daily/acquired=*/${DEV_GDELT_MONTH}*.export.CSV.zip"
  if ! hdfs dfs -test -e "$INPUT_DIR/all_stocks_5yr.csv"; then
    t0=$(now); s0=$(date +%s)
    hdfs dfs -mkdir -p "$INPUT_DIR"
    hdfs dfs -cat "$RAW_DIR/all_stocks_5yr.csv" | python3 perf/make_dev_input.py --ticker-mod "$DEV_TICKER_MOD" \
      | hdfs dfs -put - "$INPUT_DIR/all_stocks_5yr.csv"
    echo "prep,dev_input,$t0,$(now),$(( $(date +%s) - s0 )),0" >> "$WT"
  fi
else
  INPUT_DIR="$RAW_DIR"
  unset GDELT_GLOB
fi

{
  echo "run_id=$RUN_ID"
  echo "profile=$PROFILE"
  echo "config=$CONFIG"
  echo "rep=$REP"
  echo "status=running"
  echo "config_note=$CONFIG_NOTE"
  echo "spark_opts=$SPARK_OPTS"
  echo "task3=validate (8 Gate 1 configs, train 2013-15, scored 2016; 2017 never read)"
  echo "task4_predictions=$PRED_RUN (stored, untimed copy)"
  echo "input_dir=$INPUT_DIR"
  echo "gdelt_glob=${GDELT_GLOB:-all staged archives}"
  echo "dev_ticker_mod=$([ "$PROFILE" = dev ] && echo "$DEV_TICKER_MOD" || echo none)"
  echo "git_commit=$(git rev-parse HEAD)"
  echo "started_utc=$(now)"
  echo "dfs_replication=$(hdfs getconf -confKey dfs.replication 2>/dev/null)"
  echo "yarn_nm_memory_mb=$(grep -A1 'yarn.nodemanager.resource.memory-mb' /etc/hadoop/conf/yarn-site.xml 2>/dev/null \
    | grep -oE '[0-9]+' | head -1 || true)"
  echo "spark_version=$(spark-submit --version 2>&1 | grep -oE 'version [0-9.]+' | head -1)"
  echo "hadoop_version=$(hadoop version 2>/dev/null | head -1)"
  echo "hive_version=$(hive --version 2>/dev/null | head -1)"
  # EMR writes cluster facts here; best effort, the cluster config itself is fixed by PERF-04.
  echo "emr_release=$(cat /mnt/var/lib/info/*.json 2>/dev/null | grep -oE 'emr-[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true)"
  echo "instance_types=$(cat /mnt/var/lib/info/*.json 2>/dev/null | grep -oE '[a-z][0-9][a-z]*\.[0-9]*x?large' | sort -u | tr '\n' ' ' || true)"
} > "$RUN_DIR/meta.txt"
tee -a "$LOG" < "$RUN_DIR/meta.txt"

bash perf/sample_resources.sh "$RUN_DIR" "$SAMPLE_INTERVAL" &
SAMPLER=$!
trap 'kill "$SAMPLER" 2>/dev/null || true' EXIT

# ---------------------------------------------------------------- timed run
STATUS=ok
FAILED_AT=""
timed() {  # timed <task> <command...>: one task-level row (stage _total) per task
  local task="$1"; shift
  [ "$STATUS" = ok ] || return 0
  local t0 s0 rc; t0=$(now); s0=$(date +%s)
  echo "=== $task started $t0" | tee -a "$LOG"
  set +e
  "$@" 2>&1 | tee -a "$LOG"
  rc=${PIPESTATUS[0]}
  set -e
  echo "$task,_total,$t0,$(now),$(( $(date +%s) - s0 )),$rc" >> "$WT"
  echo "=== $task finished $(now) exit=$rc" | tee -a "$LOG"
  if [ "$rc" -ne 0 ]; then STATUS=failed; FAILED_AT="$task"; fi
}

T3_TIMES="$RUN_DIR/model/stage_times_${RUN_ID}.csv"
step3() {  # stage rows for Task 3, in the same format as the Task 2 and Task 4 runners
  local name="$1"; shift
  local t0 s0; t0=$(now); s0=$(date +%s)
  "$@" || return 1
  echo "$name,$t0,$(now),$(( $(date +%s) - s0 ))" >> "$T3_TIMES"
}
task3() {
  echo "stage,start_utc,end_utc,seconds" > "$T3_TIMES"
  # shellcheck disable=SC2086
  step3 validate spark-submit $SPARK_OPTS modeling/run_model.py validate --run-id "$RUN_ID" --root "$ROOT" \
    --out "$RUN_DIR/model" || return 1
}

timed task2 bash spark/run_task2.sh "$RUN_ID" "$INPUT_DIR"
timed task3 task3
timed task4 bash analytics/run_task4.sh "$RUN_ID" "$ROOT" "$RUN_ID" "$PRED_RUN"

# ---------------------------------------------------------------- evidence (untimed)
kill "$SAMPLER" 2>/dev/null || true
trap - EXIT
shopt -s nullglob
for sub in pipeline analytics; do
  files=(evidence/"$sub"/*"$RUN_ID"*)
  if [ ${#files[@]} -gt 0 ]; then mkdir -p "$RUN_DIR/$sub"; mv "${files[@]}" "$RUN_DIR/$sub/"; fi
done
if [ -f "evidence/gdelt_daily_${RUN_ID}.log" ]; then
  mkdir -p "$RUN_DIR/pipeline"; mv "evidence/gdelt_daily_${RUN_ID}.log" "$RUN_DIR/pipeline/"
fi

grep -rhoE "application_[0-9]+_[0-9]+" "$RUN_DIR" --include='*.log' --include='*.txt' --include='*.json' \
  | sort -u > "$RUN_DIR/application_ids.txt" || true
while read -r app; do
  yarn application -status "$app" >> "$RUN_DIR/raw/yarn_apps.txt" 2>/dev/null || true
done < "$RUN_DIR/application_ids.txt"

sed -i "s/^status=running$/status=$STATUS/" "$RUN_DIR/meta.txt"
{ echo "finished_utc=$(now)"; echo "failed_at=$FAILED_AT"; } >> "$RUN_DIR/meta.txt"
python3 perf/timings.py collect --run-dir "$RUN_DIR"
python3 perf/timings.py resources --run-dir "$RUN_DIR"
python3 perf/timings.py summary

# Output checks: same results as the earliest successful reference run of this profile (row counts, reconciliation, metrics).
if [ "$STATUS" = ok ]; then
  REF_DIR=""  # the earliest successful reference run of this profile
  for d in $(find evidence/perf -maxdepth 1 -type d -name "*-${PROFILE}-ref-r*" ! -name "$RUN_ID" | sort); do
    if grep -qx 'status=ok' "$d/meta.txt" 2>/dev/null; then REF_DIR="$d"; break; fi
  done
  if [ -n "$REF_DIR" ]; then
    python3 perf/timings.py check --run-dir "$RUN_DIR" --against "$REF_DIR" > "$RUN_DIR/check.txt" \
      && echo "check=same results as $(basename "$REF_DIR")" >> "$RUN_DIR/meta.txt" \
      || echo "check=DIFFERENT from $(basename "$REF_DIR"); see check.txt" >> "$RUN_DIR/meta.txt"
  else
    echo "check=first successful $PROFILE reference run; later runs are compared with this one" >> "$RUN_DIR/meta.txt"
  fi
fi

echo "--- $RUN_ID status=$STATUS ${FAILED_AT:+failed_at=$FAILED_AT}" | tee -a "$LOG"
grep -E '^check=' "$RUN_DIR/meta.txt" | tee -a "$LOG" || true
echo "Next: commit evidence/perf/ (raw/ is git-ignored), then the next repetition."
[ "$STATUS" = ok ]
