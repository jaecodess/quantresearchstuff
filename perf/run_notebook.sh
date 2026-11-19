#!/usr/bin/env bash
# Execute the rerun notebook headless on the EMR primary node and save the executed copy (Appendix A).
#
#   RERUN_ID=<new run id> S3_ROOT=s3://<bucket>/inf2006 [WITH_GDELT=1] bash perf/run_notebook.sh
#
# Writes notebooks/inf2006_pipeline_<RERUN_ID>.ipynb (the executed notebook, with outputs) and
# evidence/rerun/ (verification checks, stage times, the full command log live_<RERUN_ID>.log).
# Follow progress from a second window: tail -f evidence/rerun/live_<RERUN_ID>.log
set -euo pipefail
: "${RERUN_ID:?set RERUN_ID, e.g. 20261116-1000-rerun}"
: "${S3_ROOT:?set S3_ROOT, e.g. s3://<bucket>/inf2006}"
export RERUN_ID S3_ROOT WITH_GDELT="${WITH_GDELT:-1}"
cd "$(git rev-parse --show-toplevel)"
mkdir -p notebooks evidence/rerun
OUT="inf2006_pipeline_${RERUN_ID}.ipynb"
echo "rerun $RERUN_ID started $(date -u +%Y-%m-%dT%H:%M:%SZ), WITH_GDELT=$WITH_GDELT"
echo "progress: tail -f evidence/rerun/live_${RERUN_ID}.log"
python3 -m nbconvert --to notebook --execute perf/inf2006_pipeline.ipynb \
  --output-dir notebooks --output "$OUT" \
  --ExecutePreprocessor.timeout=-1 --ExecutePreprocessor.kernel_name=python3 \
  2>&1 | tee "evidence/rerun/nbconvert_${RERUN_ID}.log"
echo "executed notebook: notebooks/$OUT"
python3 - "$RERUN_ID" <<'EOF'
import json, sys
s = json.load(open(f"evidence/rerun/rerun_{sys.argv[1]}.json"))
print(f"RESULT: {s['checks_passed']} of {s['checks_total']} checks passed")
for c in s["checks"]:
    if c["result"] != "pass":
        print(f"  {c['result']}: {c['check']} (expected {c['expected']}, actual {c['actual']})")
EOF
