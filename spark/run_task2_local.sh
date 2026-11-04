#!/usr/bin/env bash
# DEVELOPMENT ONLY: the Task 2 price pipeline in local Spark, with Hive SQL run through spark/run_hive_sql.py.
# Not assessed evidence (that comes from spark/run_task2.sh on EMR). No Hadoop Streaming or GDELT here.
#
#   bash spark/run_task2_local.sh <work_dir> <raw_prices_csv> [ticker_mod]
# work_dir holds the local "HDFS" root, the Derby metastore and the evidence files.
set -euo pipefail
WORK="$(mkdir -p "$1" && cd "$1" && pwd)"
RAW_CSV="$(cd "$(dirname "$2")" && pwd)/$(basename "$2")"
MOD="${3:-}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
RUN_ID="${RUN_ID:-local-dev}"
SPLIT_EVENTS="${SPLIT_EVENTS:-$REPO/config/split_events.csv}"
ROOT="$WORK/inf2006"
EV="$WORK/evidence"
mkdir -p "$EV" "$WORK/rawprices" "$ROOT/config/split_events/$RUN_ID"
cp "$RAW_CSV" "$WORK/rawprices/"
cp "$SPLIT_EVENTS" "$ROOT/config/split_events/$RUN_ID/split_events.csv"
cd "$WORK"   # Derby metastore_db lives here, shared by every step
PY="python3"
HV=("root=$ROOT" "run_id=$RUN_ID")

$PY "$REPO/spark/clean_prices.py" --input "$WORK/rawprices/$(basename "$RAW_CSV")" --root "$ROOT" --run-id "$RUN_ID" \
  --summary "$EV/clean_prices_${RUN_ID}.json" ${MOD:+--ticker-mod "$MOD"}
$PY "$REPO/spark/run_hive_sql.py" "$REPO/hive/20_prices.sql" "${HV[@]}" "raw_prices_dir=$WORK/rawprices"
$PY "$REPO/spark/run_hive_sql.py" "$REPO/hive/30_calendar_weeks.sql" "${HV[@]}"
$PY "$REPO/spark/split_detection.py" --out "$EV/split_screen_${RUN_ID}.csv"
$PY "$REPO/spark/run_hive_sql.py" "$REPO/hive/31_split_events.sql" "${HV[@]}"
$PY "$REPO/spark/weekly_features.py" --root "$ROOT" --run-id "$RUN_ID" --summary "$EV/features_${RUN_ID}.json"
$PY "$REPO/spark/run_hive_sql.py" "$REPO/hive/40_features_weekly.sql" "${HV[@]}"
$PY "$REPO/spark/reconcile.py" --run-id "$RUN_ID" --out "$EV"
