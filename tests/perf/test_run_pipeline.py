"""perf/run_pipeline.sh: shell syntax and the test-once rule (D-025): Task 3 is validation only."""

import subprocess
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "perf" / "run_pipeline.sh"


def test_syntax():
    subprocess.run(["bash", "-n", str(SCRIPT)], check=True)


def test_never_scores_the_test_set():
    code = "\n".join(line for line in SCRIPT.read_text().splitlines() if not line.lstrip().startswith("#"))
    assert "run_model.py validate" in code
    assert "run_model.py test" not in code and "run_model.py reload" not in code
    assert "--marker-dir" not in code


def test_task4_reads_stored_predictions():
    code = SCRIPT.read_text()
    assert 'PRED_RUN="${PRED_RUN:-20261106-1400-full-ref}"' in code
    assert 'analytics/run_task4.sh "$RUN_ID" "$ROOT" "$RUN_ID" "$PRED_RUN"' in code
