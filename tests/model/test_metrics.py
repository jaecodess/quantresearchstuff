"""Tests for evaluation/metrics.py: weekly AUC against brute-force pairwise AUC (with ties), bootstrap, weights."""

import random
import sys
from datetime import date
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from evaluation import metrics  # noqa: E402


def brute_auc(scores, labels):
    pos = [s for s, y in zip(scores, labels) if y == 1]
    neg = [s for s, y in zip(scores, labels) if y == 0]
    wins = sum(1.0 if p > n else 0.5 if p == n else 0.0 for p in pos for n in neg)
    return wins / (len(pos) * len(neg))


def test_bootstrap_is_reproducible_and_brackets_mean():
    vals = [0.5 + 0.01 * ((i * 7) % 5 - 2) for i in range(50)]
    a = metrics.bootstrap_mean(vals, n_boot=2000)
    b = metrics.bootstrap_mean(vals, n_boot=2000)
    assert a == b
    assert a["lo"] <= a["mean"] <= a["hi"]
    assert metrics.bootstrap_mean([])["n_weeks"] == 0


def test_turnover():
    rows = [
        {"ticker": t, "start_date": w, "weight": wt}
        for w, tops in ((1, "AB"), (2, "BC"), (3, "BC"))
        for t, wt in (
            ("A", 0.5 if "A" in tops else 0),
            ("B", 0.5 if "B" in tops else 0),
            ("C", 0.5 if "C" in tops else 0),
        )
    ]
    assert metrics.turnover(rows) == pytest.approx((0.5 + 0.0) / 2)


@pytest.mark.spark
def test_weekly_auc_matches_bruteforce_with_ties():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    try:
        rnd = random.Random(1)
        rows, truth = [], {}
        for wk in (date(2016, 1, 8), date(2016, 1, 15)):
            scores = [round(rnd.random(), 1) for _ in range(40)]  # rounding forces many ties
            labels = [int(rnd.random() < 0.5) for _ in range(40)]
            ratios = [1 + rnd.uniform(-0.05, 0.05) for _ in range(40)]
            truth[wk] = brute_auc(scores, labels)
            rows += [(f"T{i}", wk, "validation", labels[i], ratios[i], scores[i]) for i in range(40)]
        df = spark.createDataFrame(
            rows, "ticker string, start_date date, split string, label int, ratio double, s double"
        )
        wk = metrics.weekly_metrics(metrics.long_scores(df, ["s"])).collect()
        for r in wk:
            assert r["auc"] == pytest.approx(truth[r["start_date"]], abs=1e-12)
            assert -1 <= r["rank_ic"] <= 1
        w = metrics.top_weights(df, "s").collect()
        for d in truth:
            ws = [r["weight"] for r in w if r["start_date"] == d]
            assert sum(ws) == pytest.approx(1.0) and sum(1 for x in ws if x > 0) == 4
        out, _ = metrics.pooled_classification(metrics.long_scores(df, ["s"]))
        assert 0 <= out["s"]["balanced_accuracy"] <= 1
    finally:
        spark.stop()
