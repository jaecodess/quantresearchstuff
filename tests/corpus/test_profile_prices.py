"""Spark test: the price profile finds every planted problem and reconciles the study window."""

import csv
import json
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytestmark = pytest.mark.spark

ROOT = Path(__file__).resolve().parents[2]


def test_profile_prices_on_fixture(tmp_path):
    out = tmp_path / "prof"
    subprocess.run(
        [
            sys.executable,
            str(ROOT / "corpus" / "profile_prices.py"),
            "--input",
            str(ROOT / "tests" / "corpus" / "fixtures" / "prices_sample.csv"),
            "--out",
            str(out),
        ],
        check=True,
        capture_output=True,
    )
    s = json.loads((out / "summary.json").read_text())
    assert s["header_matches_expected"] is True
    assert s["n_data_rows"] == 16
    assert s["n_series"] == 3
    assert s["reason_counts"] == {
        "close_outside_range": 2,  # CCC 2013-02-11 and BBB 2013-02-11 (high < low)
        "duplicate_exact": 2,  # BBB 2013-02-13 twice
        "high_below_low": 1,  # BBB 2013-02-11
        "missing_high": 1,
        "missing_low": 1,
        "missing_open": 1,  # AAA 2013-02-13
        "non_numeric_volume": 1,  # CCC 2013-02-12
        "open_outside_range": 1,  # BBB 2013-02-11
        "zero_volume": 1,
    }
    assert s["n_duplicate_keys"] == 1
    assert s["n_rows_after_study_period"] == 1  # AAA 2018-01-02
    assert s["n_rows_in_study_period"] == 15
    assert s["n_rows_in_study_period_without_hard_problem"] == 15 - 6
    assert s["n_series_starting_late"] == 1  # CCC starts 2013-02-11
    with open(out / "overnight_jumps.csv") as fh:
        jumps = list(csv.DictReader(fh))
    # BBB 2013-02-11 is invalid, so the gap is measured from 2013-02-08 close 20.5 to open 10.2 (about 1/2).
    assert [(j["ticker"], j["date"], j["split_candidate_price_rule"]) for j in jumps] == [("BBB", "2013-02-12", "True")]
