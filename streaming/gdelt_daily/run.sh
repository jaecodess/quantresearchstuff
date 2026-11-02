#!/usr/bin/env bash
# Run the gdelt_daily Hadoop Streaming job on EMR (PIPE-03 prototype, PIPE-11 full run).
# Usage (on the EMR primary node, from the repo root):
#   bash streaming/gdelt_daily/run.sh <run_id> [archive_glob]
# Example dev run on 3 archives:
#   bash streaming/gdelt_daily/run.sh 20261103-1500-dev-ref "/inf2006/raw/gdelt/v1-daily/acquired=*/2015070[1-3].export.CSV.zip"
# Writes:
#   /inf2006/manifests/gdelt_daily/<run_id>/archives.txt     one archive path per line (the job input)
#   /inf2006/staging/streaming/<run_id>/gdelt_daily/          one aggregate row per archive date
#   evidence/gdelt_daily_<run_id>.log                         command, job ID and counters (commit this)
set -euo pipefail

RUN_ID="${1:?usage: run.sh <run_id> [archive_glob]}"
GLOB="${2:-/inf2006/raw/gdelt/v1-daily/acquired=*/*.export.CSV.zip}"
ROOT="${HDFS_ROOT:-/inf2006}"
JOB_DIR="$(cd "$(dirname "$0")" && pwd)"
MANIFEST_DIR="$ROOT/manifests/gdelt_daily/$RUN_ID"
OUT="$ROOT/staging/streaming/$RUN_ID/gdelt_daily"
STREAMING_JAR="${STREAMING_JAR:-/usr/lib/hadoop-mapreduce/hadoop-streaming.jar}"
LOG="evidence/gdelt_daily_${RUN_ID}.log"
mkdir -p evidence

# 1. Manifest: one archive per line, so NLineInputFormat gives one archive per mapper.
tmp_manifest="$(mktemp)"
hdfs dfs -ls -C "$GLOB" | grep -E '\.zip$' | sort > "$tmp_manifest"
n_archives=$(wc -l < "$tmp_manifest")
[ "$n_archives" -gt 0 ] || { echo "no archives match $GLOB" >&2; exit 1; }
hdfs dfs -mkdir -p "$MANIFEST_DIR"
hdfs dfs -put -f "$tmp_manifest" "$MANIFEST_DIR/archives.txt"
rm -f "$tmp_manifest"

# 2. Fresh output location per run (HANDOVER section 18): refuse to overwrite.
if hdfs dfs -test -e "$OUT"; then
  echo "output $OUT already exists; use a new run_id" >&2
  exit 1
fi

{
  echo "run_id=$RUN_ID started=$(date -u +%Y-%m-%dT%H:%M:%SZ) archives=$n_archives glob=$GLOB"
  set -x
  hadoop jar "$STREAMING_JAR" \
    -D mapreduce.job.name="gdelt_daily_${RUN_ID}" \
    -D mapreduce.input.lineinputformat.linespermap=1 \
    -D mapreduce.map.speculative=false \
    -D mapreduce.task.timeout=1800000 \
    -D mapreduce.job.reduces=1 \
    -files "$JOB_DIR/mapper.py,$JOB_DIR/reducer.py" \
    -inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \
    -input "$MANIFEST_DIR/archives.txt" \
    -output "$OUT" \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py"
  set +x
  echo "finished=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "output rows: $(hdfs dfs -cat "$OUT/part-*" | wc -l) (expected $n_archives unless dates repeat)"
} 2>&1 | tee "$LOG"
