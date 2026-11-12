#!/usr/bin/env bash
# DEVELOPMENT ONLY: Task 4 in local Spark, with the Hive SQL run through spark/run_hive_sql.py.
# Not assessed evidence (that comes from analytics/run_task4.sh on EMR).
#
#   bash analytics/run_task4_local.sh <work_dir> <in_root> <task2_run_id> <task3_run_id> [out_dir]
# work_dir holds the Derby metastore and the analytics output; in_root is a local folder laid out like
# /inf2006 (features/<task2_run_id>/weekly[,weekly_news], predictions/<task3_run_id>/predictions).
set -euo pipefail
WORK="$(mkdir -p "$1" && cd "$1" && pwd)"
IN_ROOT="$(cd "$2" && pwd)"
FEAT_RUN="$3"
MODEL_RUN="$4"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${5:-$WORK/evidence}"
RUN_ID="${RUN_ID:-local-dev}"
cd "$WORK"
PY="python3"
SQL="$REPO/spark/run_hive_sql.py"

$PY "$SQL" "$REPO/hive/40_features_weekly.sql" "root=$IN_ROOT" "run_id=$FEAT_RUN"
$PY "$SQL" "$REPO/modeling/model_tables.sql" "root=$IN_ROOT" "run_id=$MODEL_RUN"
$PY "$SQL" "$REPO/analytics/20_q1_volume_quintiles.sql" "root=$WORK/inf2006" "run_id=$RUN_ID"
if [ -d "$IN_ROOT/features/$FEAT_RUN/weekly_news" ]; then
  $PY "$SQL" "$REPO/analytics/10_register_news.sql" "in_root=$IN_ROOT" "features_run_id=$FEAT_RUN"
  $PY "$SQL" "$REPO/analytics/30_q3a_news_market.sql" "root=$WORK/inf2006" "run_id=$RUN_ID"
fi
$PY "$REPO/analytics/run_analytics.py" --run-id "$RUN_ID" --out "$OUT"
