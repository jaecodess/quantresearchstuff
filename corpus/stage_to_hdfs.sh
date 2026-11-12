#!/usr/bin/env bash
# Copy the raw corpus from S3 into HDFS unchanged, then verify it against the manifests (CORP-06, CORP-07).
# Run once per EMR session, on the primary node, from the repo root:
#   bash corpus/stage_to_hdfs.sh s3://<bucket>/inf2006/raw <run_id>
# Staging time is reported separately from pipeline timing (brief: timing starts after inputs are in HDFS).
set -euo pipefail

S3_RAW="${1:?usage: stage_to_hdfs.sh s3://<bucket>/<prefix>/raw <run_id>}"
RUN_ID="${2:?usage: stage_to_hdfs.sh s3://<bucket>/<prefix>/raw <run_id>}"
HDFS_RAW="${HDFS_RAW:-/inf2006/raw}"
OUT_DIR="evidence/corpus"
LOG="$OUT_DIR/stage_${RUN_ID}.log"
mkdir -p "$OUT_DIR"

{
  echo "run_id=$RUN_ID started=$(date -u +%Y-%m-%dT%H:%M:%SZ) src=<s3_raw> dest=$HDFS_RAW"
  if hdfs dfs -test -e "$HDFS_RAW/prices" || hdfs dfs -test -e "$HDFS_RAW/gdelt"; then
    echo "raw corpus already in HDFS; skipping copy (raw data is never overwritten)"
  else
    hdfs dfs -mkdir -p "$HDFS_RAW"
    s3-dist-cp --src "${S3_RAW%/}/" --dest "hdfs://$HDFS_RAW/"
  fi
  # Raw files are read-only from here on.
  hdfs dfs -chmod -R a-w "$HDFS_RAW/prices" "$HDFS_RAW/gdelt" "$HDFS_RAW/sec" "$HDFS_RAW/wikinews" 2>/dev/null || true
  echo "copy finished=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  hdfs dfs -count -h "$HDFS_RAW/prices" "$HDFS_RAW/gdelt" "$HDFS_RAW/sec" "$HDFS_RAW/wikinews" 2>/dev/null || true
  hdfs dfs -du -s -h "$HDFS_RAW/prices" "$HDFS_RAW/gdelt" "$HDFS_RAW/sec" "$HDFS_RAW/wikinews" 2>/dev/null || true
  hdfs fsck "$HDFS_RAW" | tail -n 25 || true
} 2>&1 | tee "$LOG"

# Use the committed manifests; on a fresh clone before they are committed, use the copies from S3.
shopt -s nullglob
manifests=(manifests/*_acquired=*.csv)
if [ ${#manifests[@]} -eq 0 ]; then
  mkdir -p manifests
  hdfs dfs -get "$HDFS_RAW/_manifests/"*.csv manifests/
  manifests=(manifests/*_acquired=*.csv)
fi
echo "verify started=$(date -u +%Y-%m-%dT%H:%M:%SZ) (sizes for all files, SHA-256 for the price CSV)" | tee -a "$LOG"
python3 corpus/verify_hdfs.py --manifests "${manifests[@]}" --run-id "$RUN_ID" --out "$OUT_DIR" 2>&1 | tee -a "$LOG"
