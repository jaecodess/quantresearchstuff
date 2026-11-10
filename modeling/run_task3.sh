#!/usr/bin/env bash
# Task 3 on EMR in two phases, with the Gate 2 lock in between (MODEL-05 to MODEL-09).
# Run on the primary node from the repo root.
#
#   Phase 1:  bash modeling/run_task3.sh validate <run_id> [features]
#             -> evidence/model/validation_<run_id>.json, prints RECOMMENDED FROZEN CONFIG.
#             Then lock Gate 2 in docs/decisions/decision-log.md (frozen config + success rule), push,
#             and `git pull` here. Nothing in phase 1 reads the 2017 test split.
#   Phase 2:  bash modeling/run_task3.sh test <run_id> <frozen_config> [features]
#             -> the single test evaluation (refused if Gate 2 is not locked or the test was already run),
#                then a fresh-session reload check, Hive model tables, and a local copy of the PipelineModel.
#
# features: Hive table features.weekly by default (same session as Task 2), or a Parquet path such as
#           s3://<bucket>/inf2006/features/<task2_run_id>/weekly (saved in Task 2 step C5).
set -euo pipefail
MODE="${1:?usage: run_task3.sh validate|test <run_id> [frozen] [features]}"
RUN_ID="${2:?run_id}"
ROOT="${HDFS_ROOT:-/inf2006}"
EV=evidence/model
mkdir -p "$EV"
LOG="$EV/task3_${MODE}_${RUN_ID}.log"
SPARK_OPTS="${SPARK_OPTS:-}"

if [ "$MODE" = "validate" ]; then
  FEATURES="${3:-}"
elif [ "$MODE" = "test" ]; then
  FROZEN="${3:?test mode needs the frozen config id from Gate 2}"
  FEATURES="${4:-}"
else
  echo "mode must be validate or test" >&2; exit 2
fi
FEAT_ARGS=()
[ -n "$FEATURES" ] && FEAT_ARGS=(--features-path "$FEATURES")

echo "mode=$MODE run_id=$RUN_ID git=$(git rev-parse --short HEAD) started=$(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee "$LOG"
hive -f hive/00_create_databases.sql 2>&1 | tee -a "$LOG"

if [ "$MODE" = "validate" ]; then
  # shellcheck disable=SC2086
  spark-submit $SPARK_OPTS modeling/run_model.py validate --run-id "$RUN_ID" --root "$ROOT" "${FEAT_ARGS[@]}" 2>&1 | tee -a "$LOG"
  echo "Next: lock Gate 2 in docs/decisions/decision-log.md with the recommended config, push, git pull here, then run phase 2." | tee -a "$LOG"
  exit 0
fi

# Phase 2: single test evaluation, then reload in a fresh session (a separate spark-submit).
# shellcheck disable=SC2086
spark-submit $SPARK_OPTS modeling/run_model.py test --run-id "$RUN_ID" --root "$ROOT" --frozen "$FROZEN" "${FEAT_ARGS[@]}" 2>&1 | tee -a "$LOG"
# shellcheck disable=SC2086
spark-submit $SPARK_OPTS modeling/run_model.py reload --run-id "$RUN_ID" --root "$ROOT" "${FEAT_ARGS[@]}" 2>&1 | tee -a "$LOG"
hive --hivevar "root=$ROOT" --hivevar "run_id=$RUN_ID" -f modeling/model_tables.sql 2>&1 | tee -a "$LOG"

# Copy the saved PipelineModel into the repo (it goes into the submission ZIP under models/).
rm -rf "models/$RUN_ID"
mkdir -p "models/$RUN_ID"
hdfs dfs -get "$ROOT/models/$RUN_ID/pipeline_model" "models/$RUN_ID/" 2>&1 | tee -a "$LOG"
du -sh "models/$RUN_ID" | tee -a "$LOG"

grep -oE "application_[0-9]+_[0-9]+|Query ID = [^ ]+" "$LOG" | sort -u > "$EV/ids_task3_${RUN_ID}.txt"
echo "finished=$(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee -a "$LOG"
