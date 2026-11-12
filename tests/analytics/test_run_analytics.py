"""Task 4 end to end on SYNTHETIC data with planted patterns (Hive SQL run through Spark SQL, then PySpark).

Planted: (1) busier-than-usual stocks (high rel_volume) have higher next-week ratios, (2) the model score carries
signal only among high-volatility stocks, (3) weeks with high news_abn have higher cross-sectional dispersion.
The test checks each analysis finds its pattern, and the helpers on hand-computed cases.
"""

import csv
import json
import os
import random
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from analytics import run_analytics as RA  # noqa: E402

RANKS = ["rev_1w", "mom_4w", "mom_12w", "mom_12w_skip", "vol_20d", "vol_ratio", "max_move_20d", "rel_volume"]


def test_helpers():
    assert RA.tercile_of({"a": 3.0, "b": 1.0, "c": 2.0, "d": 6.0, "e": 5.0, "f": 4.0}) == {
        "b": 1,
        "c": 1,
        "a": 2,
        "f": 2,
        "e": 3,
        "d": 3,
    }
    assert RA.spearman([1, 2, 3, 4], [10, 20, 30, 40]) == pytest.approx(1.0)
    assert RA.spearman([1, 2, 3, 4], [4, 3, 2, 1]) == pytest.approx(-1.0)
    assert RA.spearman([1, 2, 2, 3], [1, 2, 2, 3]) == pytest.approx(1.0)
    d = RA.diff_ci([1.0, 1.0, 1.0], [0.0, 0.0], n_boot=200)
    assert d["mean"] == 1.0 and d["lo"] == 1.0 and d["hi"] == 1.0
    assert RA.quarter(date(2017, 5, 5)) == "Q2"


def build_inputs(spark, in_root):
    rnd = random.Random(11)
    feats, preds, news = [], [], []
    for split, year, n_weeks in (("train", 2015, 26), ("validation", 2016, 26), ("test", 2017, 26)):
        for w in range(n_weeks):
            wk = date(year, 1, 8) + timedelta(days=14 * w)
            busy_news = w % 3 == 0
            news.append((wk, 1.0 if busy_news else rnd.uniform(-0.5, 0.5), -2.0, 0.2, rnd.uniform(-1, 1), 5, 60, 7))
            noise = 0.06 if busy_news else 0.02
            rows = []
            for t in range(90):
                x = {f: rnd.random() for f in RANKS}
                sig = rnd.random()
                planted = 0.03 * (x["rel_volume"] - 0.5) + (0.05 * (sig - 0.5) if x["vol_20d"] > 2 / 3 else 0)
                ratio = 1 + planted + rnd.gauss(0, noise)
                rows.append((f"T{t:02d}", x, sig, ratio))
            med = sorted(r[3] for r in rows)[len(rows) // 2]
            for tk, x, sig, ratio in rows:
                label = int(ratio > med)
                raw = [x[f] for f in RANKS]  # raw values carry the same ordering as their ranks here
                feats.append((tk, wk, True, ratio, label, *raw, *raw, 0.01 * x["vol_20d"], split))
                if split != "train":
                    preds.append((tk, wk, "model", sig, None, float(label), ratio, "syn", split))
                    preds.append((tk, wk, "B3_best_single", rnd.random(), None, float(label), ratio, "syn", split))
    fcols = ["ticker string", "start_date date", "eligible boolean", "ratio double", "label int"]
    fcols += [f"{f} double" for f in RANKS] + [f"{f}_rank double" for f in RANKS]
    fcols += ["dollar_volume double", "split string"]
    fdf = spark.createDataFrame(feats, ", ".join(fcols))
    fdf.write.partitionBy("split").parquet(str(in_root / "features" / "syn" / "weekly"))
    spark.createDataFrame(
        news,
        "start_date date, news_abn double, tone_mean double, conflict_share double, news_abn_same_week double, "
        "n_days_pre int, n_days_base int, n_days_same_week int",
    ).write.parquet(str(in_root / "features" / "syn" / "weekly_news"))
    spark.createDataFrame(
        preds,
        "ticker string, start_date date, model_id string, score double, pred int, label double, ratio double, "
        "run_id string, split string",
    ).write.partitionBy("split").parquet(str(in_root / "predictions" / "syn" / "predictions"))
    return news


def read(path):
    with open(path) as fh:
        return list(csv.DictReader(fh))


@pytest.mark.spark
def test_task4_end_to_end(tmp_path):
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[2]").appName("t4_inputs").getOrCreate()
    in_root = tmp_path / "in"
    build_inputs(spark, in_root)
    spark.stop()

    out = tmp_path / "ev"
    cmd = ["bash", str(ROOT / "analytics" / "run_task4_local.sh"), str(tmp_path / "work"), str(in_root), "syn", "syn"]
    subprocess.run([*cmd, str(out)], check=True, env={**os.environ, "RUN_ID": "t"})
    res = json.loads((out / "task4_t.json").read_text())

    # Q1: busiest quintile above the quietest on mean excess ln ratio, in every split
    for split in ("train", "validation", "test"):
        p = next(r for r in res["q1"] if r["split"] == split and r["quintile"] == "Q5-Q1 mean_excess")
        assert p["paired_lo"] > 0, p

    # Q2: model beats B3 among high-volatility stocks, not among low-volatility ones, in both years
    for split in ("validation", "test"):
        hi = next(r for r in res["q2"] if r["split"] == split and r["group"] == "vol_high")
        lo = next(r for r in res["q2"] if r["split"] == split and r["group"] == "vol_low")
        assert hi["paired_model_minus_b3"]["lo"] > 0, hi
        assert lo["paired_model_minus_b3"]["lo"] < 0 < lo["paired_model_minus_b3"]["hi"], lo
        assert hi["same_sign_2016_2017"] is True
        assert hi["n_rows"] > 0 and hi["n_weeks"] == 26
    quarters = [r for r in res["q2"] if r["split"] == "test" and r["dimension"] == "quarter"]
    assert sum(r["n_weeks"] for r in quarters) == 26

    # Q3a: high pre-start news tercile has higher dispersion than the low tercile
    d = next(r for r in res["q3a"] if r["timing"] == "pre_start" and r["tercile"] == "high-low")
    assert d["dispersion_lo"] > 0, d
    # Q3b: every out-of-sample week with news is in exactly one tercile
    assert sum(res["q3b_weeks_by_tercile"].values()) == 52
    assert {r["scope"] for r in res["q3b"]} == {"2016+2017", "2016 validation", "2017 test (Jan-Jun)"}

    # outputs are aggregated only: no ticker column in any CSV
    for f in out.glob("*.csv"):
        assert "ticker" not in read(f)[0], f.name
