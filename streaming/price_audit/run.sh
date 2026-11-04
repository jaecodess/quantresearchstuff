#!/usr/bin/env bash
# Run the price audit Hadoop Streaming job on EMR (PIPE-02).
# Usage (primary node, repo root):  bash streaming/price_audit/run.sh <run_id> <raw_prices_hdfs_dir>
# Example raw dir: /inf2006/raw/prices/plotly-datasets-0c447c4/acquired=2026-11-03
# Writes /inf2006/staging/streaming/<run_id>/price_audit/ (one row per ticker) and
# evidence/pipeline/price_audit_<run_id>.log (command, job ID, counters, totals).
set -euo pipefail
RUN_ID="${1:?usage: run.sh <run_id> <raw_prices_hdfs_dir>}"
RAW_DIR="${2:?usage: run.sh <run_id> <raw_prices_hdfs_dir>}"
ROOT="${HDFS_ROOT:-/inf2006}"
JOB_DIR="$(cd "$(dirname "$0")" && pwd)"
OUT="$ROOT/staging/streaming/$RUN_ID/price_audit"
STREAMING_JAR="${STREAMING_JAR:-/usr/lib/hadoop-mapreduce/hadoop-streaming.jar}"
mkdir -p evidence/pipeline
LOG="evidence/pipeline/price_audit_${RUN_ID}.log"
if hdfs dfs -test -e "$OUT"; then echo "output $OUT already exists; use a new run_id" >&2; exit 1; fi
{
  echo "run_id=$RUN_ID started=$(date -u +%Y-%m-%dT%H:%M:%SZ) input=$RAW_DIR"
  set -x
  hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="price_audit_${RUN_ID}" \
    -D mapreduce.job.reduces=2 \
    -files "$JOB_DIR/mapper.py,$JOB_DIR/reducer.py" \
    -input "$RAW_DIR" \
    -output "$OUT" \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py"
  set +x
  echo "finished=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  hdfs dfs -cat "$OUT/part-*" | awk -F'\t' '$1 ~ /^_/ {print} $1 !~ /^_/ {t++; r+=$2; m+=$5; b+=$6; o+=$7; z+=$8; d+=$9; j+=$10}
    END {print "tickers="t, "rows="r, "missing="m, "bad_value="b, "ohlc_invalid="o, "zero_volume="z, "dup_dates="d, "overnight_jumps="j}'
} 2>&1 | tee "$LOG"
