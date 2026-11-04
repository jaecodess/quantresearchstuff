"""Spark test: the local Task 2 price pipeline on synthetic data with planted cases.

Tickers (weekdays from 2013-02-08, 120 market days):
  AAA  clean random walk                       -> eligible once it has 61 closes
  BBB  confirmed 2-for-1 split on day 100      -> weeks whose lookback/outcome crosses it: split_in_window
  CCC  starts on day 30                        -> eligible 61 market days later than AAA
  DDD  missing day 80, plus one bad row        -> weeks whose lookback includes day 80: insufficient_history
Plus a row after the study end (rejected outside_study_period).
"""

import csv
import json
import random
import subprocess
from datetime import date, timedelta
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark

ROOT = Path(__file__).resolve().parents[2]
N_DAYS = 120
SPLIT_DAY = 100


def market_days(n):
    d, out = date(2013, 2, 8), []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d)
        d += timedelta(days=1)
    return out


def make_prices(path):
    rnd = random.Random(7)
    days = market_days(N_DAYS)
    rows = []
    for tk in ("AAA", "BBB", "CCC", "DDD"):
        px = 50.0
        for i, d in enumerate(days):
            if tk == "CCC" and i < 30:
                continue
            if tk == "DDD" and i == 80:
                continue
            px *= 1 + rnd.uniform(-0.02, 0.02)
            o = px
            if tk == "BBB" and i >= SPLIT_DAY:
                o = px / 2
            h, lo, c = o * 1.01, o * 0.99, o * (1 + rnd.uniform(-0.005, 0.005))
            vol = 1000 * (2 if tk == "BBB" and i >= SPLIT_DAY else 1)
            rows.append([d.isoformat(), f"{o:.4f}", f"{h:.4f}", f"{lo:.4f}", f"{c:.4f}", str(vol), tk])
    rows.append(["2013-03-01", "", "", "", "50", "100", "DDD"])  # duplicate date for DDD but invalid: rejected
    rows.append(["2018-01-02", "50", "51", "49", "50", "100", "AAA"])  # after study end
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["date", "open", "high", "low", "close", "volume", "Name"])
        w.writerows(rows)
    return days


def test_local_pipeline_on_synthetic_prices(tmp_path):
    raw = tmp_path / "all_stocks_5yr.csv"
    days = make_prices(raw)
    splits = tmp_path / "split_events.csv"
    splits.write_text(
        "ticker,date,ratio_est,k,decision,evidence\n"
        f"BBB,{days[SPLIT_DAY].isoformat()},0.5,2,confirmed,planted test split\n"
    )
    work = tmp_path / "work"
    subprocess.run(
        ["bash", str(ROOT / "spark" / "run_task2_local.sh"), str(work), str(raw)],
        check=True,
        capture_output=True,
        env={**__import__("os").environ, "SPLIT_EVENTS": str(splits), "RUN_ID": "t"},
    )
    rec = json.loads((work / "evidence" / "reconciliation_t.json").read_text())
    p = rec["prices"]
    assert p["reconciles"] and p["rejected_rows"] == 2
    assert p["rejected_by_reason"] == {"missing_open": 1, "outside_study_period": 1}

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[1]").getOrCreate()
    try:
        w = spark.read.parquet(str(work / "inf2006" / "features" / "t" / "weekly"))
        rows = {(r["ticker"], str(r["start_date"])): r.asDict() for r in w.collect()}
    finally:
        spark.stop()
    idx = {d.isoformat(): i for i, d in enumerate(days)}

    for (tk, start), r in rows.items():
        if r["cutoff_date"] is None:  # first week: no market day before it
            assert not r["eligible"] and r["ineligible_reason"] == "no_bar_at_cutoff"
            continue
        cut = idx[str(r["cutoff_date"])]
        nxt = idx[str(r["next_start_date"])]
        st = idx[start]
        assert r["cutoff_date"] < r["start_date"]
        if not r["eligible"]:
            assert r["label"] is None and r["mom_4w"] is None
        if tk == "AAA":
            assert r["eligible"] == (cut >= 60), (start, r)
        if tk == "CCC":
            assert r["eligible"] == (cut >= 90), (start, r)
        if tk == "DDD" and cut >= 60 and not (cut - 60 <= 80 <= cut) and nxt != 80 and st != 80:
            assert r["eligible"], (start, r)
        if tk == "DDD" and cut - 60 <= 80 <= cut:
            assert r["ineligible_reason"] in ("insufficient_history", "no_bar_at_cutoff"), (start, r)
        if tk == "BBB" and cut >= 60:
            crosses = cut - 60 < SPLIT_DAY <= nxt and st != SPLIT_DAY
            assert (r["ineligible_reason"] == "split_in_window") == crosses, (start, r)
    # labels: per week, eligible rows split around the median
    by_week = {}
    for (_tk, start), r in rows.items():
        if r["eligible"]:
            by_week.setdefault(start, []).append(r["label"])
    assert by_week, "no eligible weeks"
    for labels in by_week.values():
        assert sum(labels) <= len(labels) // 2
