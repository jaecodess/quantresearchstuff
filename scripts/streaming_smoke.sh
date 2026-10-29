#!/usr/bin/env bash
# Local smoke test for every Hadoop Streaming job: cat sample | mapper | sort | reducer, diffed against expected output.
# Convention: streaming/<job>/{mapper.py,reducer.py,tests/sample.in,tests/expected.out}
set -euo pipefail
cd "$(dirname "$0")/.."

status=0
ran=0
for dir in streaming/*/; do
  [ -f "$dir/mapper.py" ] || continue
  if [ ! -f "$dir/tests/sample.in" ] || [ ! -f "$dir/tests/expected.out" ]; then
    echo "::error::$dir needs tests/sample.in and tests/expected.out"
    status=1
    continue
  fi
  ran=$((ran + 1))
  if [ -f "$dir/reducer.py" ]; then
    actual=$(python3 "$dir/mapper.py" < "$dir/tests/sample.in" | LC_ALL=C sort | python3 "$dir/reducer.py")
  else
    actual=$(python3 "$dir/mapper.py" < "$dir/tests/sample.in" | LC_ALL=C sort)
  fi
  if ! diff <(printf '%s\n' "$actual") "$dir/tests/expected.out" > /dev/null; then
    echo "::error::$dir output differs from tests/expected.out"
    diff <(printf '%s\n' "$actual") "$dir/tests/expected.out" || true
    status=1
  else
    echo "ok: $dir"
  fi
done
echo "streaming smoke: $ran job(s) run"
exit $status
