"""The rerun notebook (perf/inf2006_pipeline.ipynb) and its helpers.

The helper checks run everywhere. The notebook itself is executed in DRY_RUN mode (commands printed, not run)
when nbconvert is installed, so every cell must at least run end to end. Real evidence only comes from EMR.
"""

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "perf"))

import rerun_lib as R  # noqa: E402

NB = ROOT / "perf" / "inf2006_pipeline.ipynb"


def test_notebook_is_thin_and_never_runs_the_test_mode():
    nb = json.loads(NB.read_text())
    code = "".join("".join(c["source"]) for c in nb["cells"] if c["cell_type"] == "code")
    assert nb["nbformat"] == 4
    assert "run_model.py test" not in code and "run_task3.sh test" not in code
    for script in (
        "corpus/stage_to_hdfs.sh",
        "spark/run_task2.sh",
        "modeling/run_task3.sh validate",
        "run_model.py reload",
        "analytics/run_task4.sh",
    ):
        assert script in code, script
    assert "s3://" not in code.replace("s3-dist-cp", "")  # bucket comes from S3_ROOT, never committed
    assert all(not c.get("outputs") for c in nb["cells"] if c["cell_type"] == "code")


def test_checks_and_lookups(tmp_path):
    rr = R.Rerun("t", out_dir=str(tmp_path))
    assert rr.check("eq", "cmd", 5, 5, "ev")
    assert not rr.check("ne", "cmd", 5, 6, "ev")
    assert rr.check("tol", "cmd", 0.0, 1e-12, "ev", tol=1e-9)
    assert not rr.check("missing", "cmd", 1, None, "ev")
    s = rr.save()
    assert s["checks_passed"] == 2 and s["checks_total"] == 4
    assert (tmp_path / "checks_t.csv").exists()
    out = rr.sh("echo hello", "echo")
    assert "hello" in out and rr.timings[-1]["exit"] == 0
    with pytest.raises(RuntimeError):
        rr.sh("exit 3", "fails")
    assert R.get({"a": {"b": 1}}, "a", "b") == 1 and R.get({"a": 1}, "a", "b") is None
    t4 = json.loads((ROOT / "evidence/analytics/task4_20261110-1000-full-ref.json").read_text())
    assert R.q2_paired(t4) == pytest.approx(0.014260489, abs=1e-8)
    assert R.q1_paired(t4) is not None and R.q3a_dispersion(t4) is not None
    v = json.loads((ROOT / "evidence/model/validation_20261106-1400-full-ref.json").read_text())
    assert R.max_auc_diff(v, v) == 0.0
    inv = json.loads((ROOT / "evidence/corpus/inventory_20261110-1300-aux.json").read_text())
    assert R.problems_total(inv) == 0


@pytest.mark.skipif(importlib.util.find_spec("nbconvert") is None, reason="nbconvert not installed")
def test_notebook_executes_in_dry_run(tmp_path):
    env = {**os.environ, "RERUN_ID": "dry", "S3_ROOT": "s3://example/inf2006", "DRY_RUN": "1"}
    out = tmp_path / "nb"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "nbconvert",
            "--to",
            "notebook",
            "--execute",
            str(NB),
            "--output-dir",
            str(out),
            "--output",
            "dry.ipynb",
            "--ExecutePreprocessor.timeout=120",
        ],
        check=True,
        env=env,
        cwd=ROOT / "perf",
    )
    rec = json.loads((ROOT / "evidence/rerun/rerun_dry.json").read_text())
    try:
        assert rec["dry_run"] is True and rec["checks_total"] >= 15
        assert {c["result"] for c in rec["checks"]} == {"dry"}
    finally:
        for f in (ROOT / "evidence/rerun").glob("*_dry.*"):
            f.unlink()
