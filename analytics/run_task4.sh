#!/usr/bin/env bash
# Task 4 analytics on EMR (ANLY-01 to ANLY-04). Run on the primary node from the repo root.
#
#   bash analytics/run_task4.sh <task4_run_id> <in_root> <task2_run_id> <task3_run_id> [pred_root]
#
#   in_root       where the Task 2 features and Task 3 predictions are saved, e.g. s3://<bucket>/inf2006
#                 (or /inf2006 in the same session as Tasks 2 and 3)
#   task2_run_id  folder of features.weekly and weekly_news, e.g. 20261105-1430-full-ref
#   task3_run_id  folder of the stored frozen-model predictions, e.g. 20261106-1400-full-ref
#   pred_root     where the predictions are, if not under in_root (e.g. features rebuilt in HDFS this session,
#                 predictions still in S3)
#
# Steps: register the inputs as Hive external tables (nothing is copied or recomputed), Q1 and Q3a in Hive,
# Q2, Q3b and the week bootstraps in PySpark. Outputs: Hive tables analytics.q1_weekly / analytics.q3a_weekly
# in HDFS, and evidence/analytics/ (aggregated CSV + JSON, log, application and query IDs, timings).
# Q3 runs only if <in_root>/features/<task2_run_id>/weekly_news exists (Task 2 run with GDELT).
set -euo pipefail
RUN_ID="${1:?usage: run_task4.sh <task4_run_id> <in_root> <task2_run_id> <task3_run_id>}"
IN_ROOT="${2:?in_root}"
FEAT_RUN="${3:?task2_run_id}"
MODEL_RUN="${4:?task3_run_id}"
PRED_ROOT="${5:-$IN_ROOT}"
ROOT="${HDFS_ROOT:-/inf2006}"
EV=evidence/analytics
mkdir -p "$EV"
LOG="$EV/task4_${RUN_ID}.log"
TIMES="$EV/timings_task4_${RUN_ID}.csv"
SPARK_OPTS="${SPARK_OPTS:-}"
echo "stage,start_utc,end_utc,seconds" > "$TIMES"

stage() {
  local name="$1"; shift
  local t0 s0; t0=$(date -u +%Y-%m-%dT%H:%M:%SZ); s0=$(date +%s)
  echo "=== $name" | tee -a "$LOG"
  "$@" 2>&1 | tee -a "$LOG"
  echo "$name,$t0,$(date -u +%Y-%m-%dT%H:%M:%SZ),$(( $(date +%s) - s0 ))" >> "$TIMES"
}

echo "run_id=$RUN_ID in_root=$IN_ROOT features=$FEAT_RUN predictions=$PRED_ROOT/$MODEL_RUN git=$(git rev-parse --short HEAD)" \
  "started=$(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee "$LOG"

stage databases hive -f hive/00_create_databases.sql
stage register_features hive --hivevar "root=$IN_ROOT" --hivevar "run_id=$FEAT_RUN" -f hive/40_features_weekly.sql
stage register_model hive --hivevar "root=$PRED_ROOT" --hivevar "run_id=$MODEL_RUN" -f modeling/model_tables.sql
stage q1_hive hive --hivevar "root=$ROOT" --hivevar "run_id=$RUN_ID" -f analytics/20_q1_volume_quintiles.sql

if hadoop fs -test -d "$IN_ROOT/features/$FEAT_RUN/weekly_news"; then
  stage register_news hive --hivevar "in_root=$IN_ROOT" --hivevar "features_run_id=$FEAT_RUN" \
    -f analytics/10_register_news.sql
  stage q3a_hive hive --hivevar "root=$ROOT" --hivevar "run_id=$RUN_ID" -f analytics/30_q3a_news_market.sql
else
  echo "NOTE: $IN_ROOT/features/$FEAT_RUN/weekly_news not found; Q3 is skipped (needs the Task 2 GDELT run)." \
    | tee -a "$LOG"
  hive -e "DROP TABLE IF EXISTS features.weekly_news; DROP TABLE IF EXISTS analytics.q3a_weekly;" 2>&1 | tee -a "$LOG"
fi

# shellcheck disable=SC2086
stage q2_q3b_spark spark-submit $SPARK_OPTS analytics/run_analytics.py --run-id "$RUN_ID" --out "$EV"

grep -oE "application_[0-9]+_[0-9]+|Query ID = [^ ]+" "$LOG" | sort -u > "$EV/ids_task4_${RUN_ID}.txt"
echo "--- stage times"; cat "$TIMES"
echo "finished=$(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$LOG"
