#!/usr/bin/env bash
# Section 6 timing runs (PERF-06 to PERF-09): the reference configuration and ONE alternative, three times each.
#
#   bash perf/perf_runs.sh <base_id> s3://<bucket>/inf2006
#   e.g. bash perf/perf_runs.sh 20261118-1000-perf s3://<bucket>/inf2006
#
# Options (environment variables): REPS=3, ALT_PARTITIONS=16
#
# Per repetition and configuration (order interleaved: default r1, alt r1, default r2, ...):
#   task2_price_stages  spark/run_task2.sh with WITH_GDELT=0 (price audit, cleaning, calendar, splits, features)
#   task3_validation    modeling/run_task3.sh validate (8 configs on 2013-15, scored on 2016; 2017 never read)
#   task4_analytics     analytics/run_task4.sh (Hive Q1/Q3a, PySpark Q2/Q3b on the stored predictions)
# The alternative changes ONE setting for every Spark job: spark.sql.shuffle.partitions = ALT_PARTITIONS
# (the cluster default is 200). Hive and Streaming steps are unaffected by it. The GDELT parse is not repeated.
#
# Output: evidence/perf/timings_<base_id>.csv, summary_<base_id>.csv/.json, perf_<base_id>.log (full output),
# env_<base_id>.txt, storage_<base_id>.txt. A failed task is recorded with its exit code and the script carries on with the next one.
set -uo pipefail
BASE="${1:?usage: perf_runs.sh <base_id> s3://<bucket>/inf2006}"
S3_ROOT="${2:?usage: perf_runs.sh <base_id> s3://<bucket>/inf2006}"
S3_ROOT="${S3_ROOT%/}"
REPS="${REPS:-3}"
ALT="${ALT_PARTITIONS:-16}"
PRICES=/inf2006/raw/prices/plotly-datasets-0c447c4/acquired=2026-11-03
FEAT_REF=20261105-1430-full-ref     # Task 2 run whose weekly_news table is reused (GDELT is not re-parsed)
MODEL_REF=20261106-1400-full-ref    # Task 3 run whose stored predictions Task 4 reads
cd "$(git rev-parse --show-toplevel)"
EV=evidence/perf
mkdir -p "$EV"
T="$EV/timings_${BASE}.csv"
LOG="$EV/perf_${BASE}.log"
[ -f "$T" ] || echo "config,rep,task,run_id,spark_opts,start_utc,end_utc,seconds,exit" > "$T"

now() { date -u +%Y-%m-%dT%H:%M:%SZ; }
say() { echo "[$(now)] $*" | tee -a "$LOG"; }

# ---- checks before anything is timed
python3 -c "import numpy" 2>/dev/null || { echo "STOP: numpy missing. Run: sudo yum install -y python3-numpy"; exit 1; }
aws s3 ls "$S3_ROOT/features/$FEAT_REF/weekly_news/" >/dev/null || { echo "STOP: cannot read $S3_ROOT/features/$FEAT_REF/weekly_news/ (check the bucket name)"; exit 1; }
aws s3 ls "$S3_ROOT/predictions/$MODEL_REF/" >/dev/null || { echo "STOP: cannot read $S3_ROOT/predictions/$MODEL_REF/"; exit 1; }

# ---- environment record
{
  echo "base_id=$BASE started=$(now) git=$(git rev-parse --short HEAD) reps=$REPS alt_shuffle_partitions=$ALT"
  grep -io 'emr-[0-9.]*' /mnt/var/lib/info/extraInstanceData.json 2>/dev/null | head -1
  spark-submit --version 2>&1 | grep -m1 version
  echo "--- spark-defaults (executors, memory, shuffle, adaptive)"
  grep -E "executor|driver.memory|dynamicAllocation|shuffle.partitions|adaptive" /etc/spark/conf/spark-defaults.conf 2>/dev/null
  echo "--- YARN nodes"; yarn node -list 2>/dev/null | tail -n +2
} > "$EV/env_${BASE}.txt" 2>&1
say "environment recorded in $EV/env_${BASE}.txt"

# ---- raw prices in HDFS (a new cluster has an empty HDFS)
if ! hdfs dfs -test -e "$PRICES/all_stocks_5yr.csv"; then
  say "staging the raw corpus from S3 into HDFS (about 2 minutes, not timed)"
  bash corpus/stage_to_hdfs.sh "$S3_ROOT/raw" "${BASE}-stage" >> "$LOG" 2>&1 || { say "STOP: staging failed, see $LOG"; exit 1; }
fi

timed() {  # timed <config> <rep> <task> <run_id> <command...>
  local cfg="$1" rep="$2" task="$3" rid="$4"; shift 4
  local t0 s0 code; t0=$(now); s0=$(date +%s)
  say "START $cfg rep $rep $task ($rid)"
  echo "===== $cfg rep $rep $task $rid $t0" >> "$LOG"
  "$@" >> "$LOG" 2>&1; code=$?
  local secs=$(( $(date +%s) - s0 ))
  echo "$cfg,$rep,$task,$rid,\"${SPARK_OPTS:-}\",$t0,$(now),$secs,$code" >> "$T"
  say "END   $cfg rep $rep $task: $((secs / 60)) min $((secs % 60)) s, exit $code"
  return 0
}

for rep in $(seq 1 "$REPS"); do
  for cfg in default "partitions${ALT}"; do
    if [ "$cfg" = default ]; then export SPARK_OPTS=""; else export SPARK_OPTS="--conf spark.sql.shuffle.partitions=$ALT"; fi
    RID="${BASE}-${cfg}-r${rep}"
    timed "$cfg" "$rep" task2_price_stages "$RID" env WITH_GDELT=0 bash spark/run_task2.sh "$RID" "$PRICES"
    timed "$cfg" "$rep" task3_validation "$RID" bash modeling/run_task3.sh validate "$RID"
    # Task 4 needs a weekly_news table beside this run's features: copy the saved one (tiny, not timed)
    hdfs dfs -mkdir -p "/inf2006/features/$RID" >> "$LOG" 2>&1
    s3-dist-cp --src "$S3_ROOT/features/$FEAT_REF/weekly_news" --dest "hdfs:///inf2006/features/$RID/weekly_news" >> "$LOG" 2>&1
    timed "$cfg" "$rep" task4_analytics "$RID" bash analytics/run_task4.sh "$RID" /inf2006 "$RID" "$MODEL_REF" "$S3_ROOT"
  done
done
export SPARK_OPTS=""

# storage after all runs (HDFS used per node and per folder) for the Section 6 storage budget
{ echo "measured=$(now)"; hdfs dfsadmin -report 2>/dev/null | grep -E "^Name:|^DFS Used:|^DFS Remaining:|^Configured Capacity:"
  echo "--- HDFS folders"; hdfs dfs -du -s -h /inf2006/* 2>/dev/null; } > "$EV/storage_${BASE}.txt" 2>&1
python3 perf/summarise_perf.py "$T" "$EV/summary_${BASE}" | tee -a "$LOG"
say "finished. Bundle with: tar czf ~/perf_${BASE}.tar.gz evidence/perf \$(find evidence -name '*${BASE}*' -not -path 'evidence/perf/*')"
