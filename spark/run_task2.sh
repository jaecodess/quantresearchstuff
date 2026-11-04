#!/usr/bin/env bash
# Task 2 pipeline on EMR, all stages in order (PIPE-13). Run on the primary node from the repo root,
# after the raw corpus is staged in HDFS (corpus/stage_to_hdfs.sh). Timing starts here (brief).
#
#   bash spark/run_task2.sh <run_id> <raw_prices_hdfs_dir>
#   e.g. bash spark/run_task2.sh 20261105-1400-full-ref /inf2006/raw/prices/plotly-datasets-0c447c4/acquired=2026-11-03
#
# Environment options:
#   WITH_GDELT=0          skip the GDELT Streaming job and weekly news (default 1 = include)
#   GDELT_GLOB=...        archives for the GDELT job (default: all staged archives)
#   SPLIT_EVENTS=path     reviewed split table (default config/split_events.csv)
#   SPARK_OPTS="..."      extra spark-submit options (for the PERF alternative configuration)
#
# Evidence written to evidence/pipeline/:
#   task2_<run_id>.log          every command with YARN application IDs, Hadoop job IDs, Hive query IDs
#   stage_times_<run_id>.csv    stage, start, end, seconds (input to PERF timing tables)
#   clean_prices_<run_id>.json, split_screen_<run_id>.csv, features_<run_id>.json,
#   reconciliation_<run_id>.json, eligible_per_week_<run_id>.csv, price_audit_<run_id>.log
set -euo pipefail

RUN_ID="${1:?usage: run_task2.sh <run_id> <raw_prices_hdfs_dir>}"
RAW_DIR="${2:?usage: run_task2.sh <run_id> <raw_prices_hdfs_dir>}"
ROOT="${HDFS_ROOT:-/inf2006}"
WITH_GDELT="${WITH_GDELT:-1}"
SPLIT_EVENTS="${SPLIT_EVENTS:-config/split_events.csv}"
SPARK_OPTS="${SPARK_OPTS:-}"
EV=evidence/pipeline
mkdir -p "$EV"
LOG="$EV/task2_${RUN_ID}.log"
TIMES="$EV/stage_times_${RUN_ID}.csv"
echo "stage,start_utc,end_utc,seconds" > "$TIMES"
HV=(--hivevar "root=$ROOT" --hivevar "run_id=$RUN_ID")

stage() {
  local name="$1"; shift
  local t0 t1 s0 s1
  t0=$(date -u +%Y-%m-%dT%H:%M:%SZ); s0=$(date +%s)
  echo "=== stage $name started $t0" | tee -a "$LOG"
  "$@" 2>&1 | tee -a "$LOG"
  t1=$(date -u +%Y-%m-%dT%H:%M:%SZ); s1=$(date +%s)
  echo "$name,$t0,$t1,$((s1 - s0))" >> "$TIMES"
  echo "=== stage $name finished $t1 ($((s1 - s0)) s)" | tee -a "$LOG"
}

echo "run_id=$RUN_ID root=$ROOT raw=$RAW_DIR with_gdelt=$WITH_GDELT git=$(git rev-parse --short HEAD)" | tee "$LOG"

stage hive_databases hive -f hive/00_create_databases.sql
stage price_audit_streaming bash streaming/price_audit/run.sh "$RUN_ID" "$RAW_DIR"
# shellcheck disable=SC2086
stage clean_prices spark-submit $SPARK_OPTS spark/clean_prices.py --input "$RAW_DIR/all_stocks_5yr.csv" \
  --root "$ROOT" --run-id "$RUN_ID" --summary "$EV/clean_prices_${RUN_ID}.json"
stage hive_prices hive "${HV[@]}" --hivevar "raw_prices_dir=$RAW_DIR" -f hive/20_prices.sql
stage hive_calendar hive "${HV[@]}" -f hive/30_calendar_weeks.sql
# shellcheck disable=SC2086
stage split_screen spark-submit $SPARK_OPTS spark/split_detection.py --out "$EV/split_screen_${RUN_ID}.csv"
hdfs dfs -mkdir -p "$ROOT/config/split_events/$RUN_ID"
hdfs dfs -put -f "$SPLIT_EVENTS" "$ROOT/config/split_events/$RUN_ID/split_events.csv"
stage hive_split_events hive "${HV[@]}" -f hive/31_split_events.sql
# shellcheck disable=SC2086
stage weekly_features spark-submit $SPARK_OPTS spark/weekly_features.py --root "$ROOT" --run-id "$RUN_ID" \
  --summary "$EV/features_${RUN_ID}.json"
stage hive_features_weekly hive "${HV[@]}" -f hive/40_features_weekly.sql

if [ "$WITH_GDELT" = "1" ]; then
  stage gdelt_streaming bash streaming/gdelt_daily/run.sh "$RUN_ID" ${GDELT_GLOB:+"$GDELT_GLOB"}
  stage hive_gdelt_daily hive --hivevar "run_id=$RUN_ID" -f hive/10_gdelt_daily.sql
  stage hive_weekly_news hive "${HV[@]}" -f hive/41_weekly_news.sql
fi

# shellcheck disable=SC2086
stage reconcile spark-submit $SPARK_OPTS spark/reconcile.py --run-id "$RUN_ID" --out "$EV"

echo "--- application / job / query IDs" | tee -a "$LOG"
grep -oE "application_[0-9]+_[0-9]+|job_[0-9]+_[0-9]+|Query ID = [^ ]+" "$LOG" | sort -u | tee "$EV/ids_${RUN_ID}.txt"
echo "--- stage times"; cat "$TIMES"
awk -F, 'NR > 1 {s += $4} END {print "total pipeline seconds: " s}' "$TIMES" | tee -a "$LOG"
