#!/usr/bin/env bash
# Background sampler for PERF runs: YARN node memory and HDFS per-datanode usage every <interval> seconds.
#   bash perf/sample_resources.sh <run_dir> [interval_s]      (stopped by run_pipeline.sh with kill)
# Raw snapshots go to <run_dir>/raw/ (git-ignored: they contain hostnames); `perf/timings.py resources`
# turns them into resources_peak.csv with nodes renamed node1, node2, ...
set -u
RAW="$1/raw"
INTERVAL="${2:-15}"
mkdir -p "$RAW"
while true; do
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  { echo "### sample $ts"; yarn node -list -showDetails 2>/dev/null; } >> "$RAW/yarn_nodes.txt"
  { echo "### sample $ts"; hdfs dfsadmin -report 2>/dev/null; } >> "$RAW/dfs_report.txt"
  { echo "### sample $ts"; df -B1 / /mnt 2>/dev/null; } >> "$RAW/df_primary.txt"
  sleep "$INTERVAL"
done
