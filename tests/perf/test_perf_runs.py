"""Section 6 timing script: shell syntax, safety (never the test mode) and the summary statistics."""

import csv
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "perf"))

import summarise_perf as S  # noqa: E402

SCRIPT = ROOT / "perf" / "perf_runs.sh"


def test_script_syntax_and_safety():
    subprocess.run(["bash", "-n", str(SCRIPT)], check=True)
    text = SCRIPT.read_text()
    assert "run_task3.sh test" not in text and "run_model.py test" not in text
    assert "run_task3.sh validate" in text and "WITH_GDELT=0" in text
    assert "spark.sql.shuffle.partitions" in text
    assert "s3://inf2006" not in text and "915843949100" not in text


def test_summary(tmp_path):
    rows = [
        ("default", 1, "task3_validation", 200, 0),
        ("default", 2, "task3_validation", 220, 0),
        ("default", 3, "task3_validation", 210, 0),
        ("partitions16", 1, "task3_validation", 100, 0),
        ("partitions16", 2, "task3_validation", 105, 0),
        ("partitions16", 3, "task3_validation", 999, 1),
    ]
    src = tmp_path / "t.csv"
    with open(src, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["config", "rep", "task", "run_id", "spark_opts", "start_utc", "end_utc", "seconds", "exit"])
        for cfg, rep, task, secs, code in rows:
            w.writerow([cfg, rep, task, f"x-{cfg}-r{rep}", "", "", "", secs, code])
    out = S.main([str(src), str(tmp_path / "s")])
    by = {(o["task"], o["config"]): o for o in out}
    d, a = by[("task3_validation", "default")], by[("task3_validation", "partitions16")]
    assert d["median_s"] == 210 and d["min_s"] == 200 and d["max_s"] == 220
    assert a["runs_ok"] == 2 and a["runs_failed"] == 1 and a["median_s"] == 102.5
    assert a["speedup_vs_default"] == round(210 / 102.5, 3)
    assert json.loads((tmp_path / "s.json").read_text())["summary"]
