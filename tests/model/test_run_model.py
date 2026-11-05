"""Spark test: validate -> guarded single test -> reload, on SYNTHETIC features with a planted signal.

Never touches the real 2017 test split: everything here is generated data in a temp folder.
"""

import json
import random
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark

ROOT = Path(__file__).resolve().parents[2]
RANKS = [
    "rev_1w",
    "mom_4w",
    "mom_12w",
    "mom_12w_skip",
    "vol_20d",
    "vol_ratio",
    "max_move_20d",
    "rel_volume",
    "dollar_volume",
]


def make_features(spark, path):
    rnd = random.Random(3)
    rows = []
    for split, year, n_weeks in (("train", 2015, 30), ("validation", 2016, 12), ("test", 2017, 12)):
        for w in range(n_weeks):
            wk = date(year, 1, 9) + timedelta(days=7 * w)
            vals = []
            for t in range(60):
                x = {f: rnd.random() for f in RANKS}
                # planted signal in TWO F1 indicators, so a model can beat the best single-indicator rule (B3)
                latent = 0.08 * (x["mom_4w"] - 0.5) + 0.08 * (x["vol_20d"] - 0.5) + rnd.gauss(0, 0.02)
                vals.append((f"T{t:02d}", x, 1 + latent))
            med = sorted(v[2] for v in vals)[len(vals) // 2]
            for tk, x, ratio in vals:
                rows.append((tk, wk, split, True, ratio, int(ratio > med), *[x[f] for f in RANKS]))
    schema = "ticker string, start_date date, split string, eligible boolean, ratio double, label int, " + ", ".join(
        f"{f}_rank double" for f in RANKS
    )
    spark.createDataFrame(rows, schema).write.parquet(str(path))


def run(mode, tmp, *extra):
    return subprocess.run(
        [
            sys.executable,
            str(ROOT / "modeling" / "run_model.py"),
            mode,
            "--run-id",
            "t",
            "--root",
            str(tmp / "root"),
            "--out",
            str(tmp / "ev"),
            "--features-path",
            str(tmp / "features"),
            "--marker-dir",
            str(tmp / "marker"),
            *extra,
        ],
        capture_output=True,
        text=True,
    )


def test_validate_guarded_test_and_reload(tmp_path):
    from pyspark.sql import SparkSession

    (tmp_path / "root").mkdir()
    spark = SparkSession.builder.master("local[2]").config("spark.ui.enabled", "false").getOrCreate()
    try:
        make_features(spark, tmp_path / "features")
    finally:
        spark.stop()

    v = run("validate", tmp_path)
    assert v.returncode == 0, v.stderr[-3000:]
    val = json.loads((tmp_path / "ev" / "validation_t.json").read_text())
    assert val["test_split_read"] is False
    assert val["rows"] == {"train": 1800, "validation": 720}
    assert val["b3_choice"]["indicator"] in ("mom_4w", "vol_20d") and not val["b3_choice"]["flipped"]
    chosen = val["selection"]["recommended_frozen_config"]

    unlocked = tmp_path / "log_unlocked.md"
    unlocked.write_text("## Gate 2: frozen\n\nFrozen configuration: ____\n\nLocked on: ____ by: ____\n")
    r = run("test", tmp_path, "--frozen", chosen, "--decision-log", str(unlocked))
    assert r.returncode != 0 and "REFUSED: Gate 2" in (r.stderr + r.stdout)
    assert not (tmp_path / "root" / "models" / "TEST_EVALUATED.json").exists()

    locked = tmp_path / "log_locked.md"
    locked.write_text(f"## Gate 2: frozen\n\nFrozen configuration: `{chosen}`\n\nLocked on: 2026-11-05 by: @t\n")
    t = run("test", tmp_path, "--frozen", chosen, "--decision-log", str(locked))
    assert t.returncode == 0, t.stderr[-3000:]
    res = json.loads((tmp_path / "ev" / "test_t.json").read_text())
    assert res["test_weeks"] == 12 and res["success_rule"]["evidence_of_signal"] is True, res["success_rule"]
    assert (
        res["test_summary"]["model"]["weekly_auc"]["mean"] > res["test_summary"]["B3_best_single"]["weekly_auc"]["mean"]
    )
    assert (tmp_path / "root" / "models" / "TEST_EVALUATED.json").exists()

    again = run("test", tmp_path, "--frozen", chosen, "--decision-log", str(locked))
    assert again.returncode != 0 and "evaluated ONCE" in (again.stderr + again.stdout)

    rl = run("reload", tmp_path)
    assert rl.returncode == 0, rl.stderr[-3000:]
    rel = json.loads((tmp_path / "ev" / "reload_t.json").read_text())
    assert rel["passed"] and rel["max_abs_diff"] <= 1e-9 and rel["rows_compared"] == 720
