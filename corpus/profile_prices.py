#!/usr/bin/env python3
"""Raw-quality profile of the daily price CSV (CORP-02, CORP-04, CORP-08, CORP-09).

Reads the raw file exactly as staged (as text, so nothing is silently coerced or dropped) and
writes small aggregated results for the report. Nothing is cleaned or changed here; cleaning is PIPE-04.

Run on EMR (assessed evidence):
  spark-submit corpus/profile_prices.py \
      --input "/inf2006/raw/prices/<snapshot_version>/acquired=<date>/all_stocks_5yr.csv" \
      --out evidence/corpus/prices_profile_<run_id>
Run locally on a copy (development only, not evidence):
  spark-submit corpus/profile_prices.py --input <local csv> --out <local dir>

Outputs in --out (all small, aggregated):
  summary.json       headline counts for report section 2
  per_ticker.csv     one row per series: first/last date, rows, missing calendar days, problems
  problem_rows.csv   every raw row that breaks a check, with the reason(s) (for the quality report)
  overnight_jumps.csv  |ln(open_t / close_t-1)| > 0.25, with the nearest common split ratio
  calendar.csv       one row per date: number of series with a bar
"""

import argparse
import csv
import json
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

EXPECTED_HEADER = ["date", "open", "high", "low", "close", "volume", "Name"]
PRICE_COLS = ["open", "high", "low", "close"]
NUM_COLS = PRICE_COLS + ["volume"]
JUMP_THRESHOLD = 0.25  # HANDOVER section 6.2, about +/-22%
SPLIT_K = [2, 3, 4, 1.5, 5, 10]
CALENDAR_SHARE = 0.5  # a market trading day = a date where at least half of the series have a bar


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--out", required=True)
    p.add_argument("--run-id", default="")
    p.add_argument("--study-start", default="2013-02-08", help="brief's reference window start")
    p.add_argument("--study-end", default="2017-12-29", help="brief's reference window end")
    return p.parse_args()


def write_csv(path, rows, header):
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(header)
        for r in rows:
            w.writerow([r[h] for h in header])


def nearest_split(ratio):
    """Return (k, relative error) of the closest common split ratio 1/k or k (reverse split)."""
    best = None
    for k in SPLIT_K:
        for target in (1.0 / k, float(k)):
            err = abs(ratio / target - 1.0)
            if best is None or err < best[1]:
                best = (k if target < 1 else -k, err)
    return best


def main():
    a = parse_args()
    os.makedirs(a.out, exist_ok=True)
    spark = SparkSession.builder.appName("profile_prices").getOrCreate()
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")

    lines = spark.read.text(a.input).withColumn("source_file", F.input_file_name())
    n_lines = lines.count()
    first_line = lines.first()["value"].strip()
    header = first_line.split(",")

    raw = (
        lines.withColumn("f", F.split("value", ",", -1))
        .withColumn("n_fields", F.size("f"))
        .filter(F.col("value") != first_line)  # drop the header line(s)
    )
    n_header_lines = n_lines - raw.count()

    df = raw.select(
        "value",
        "n_fields",
        *[F.trim(F.col("f")[i]).alias(c + "_s") for i, c in enumerate(EXPECTED_HEADER)],
    ).withColumnRenamed("Name_s", "ticker")
    df = df.withColumn("ticker", F.when(F.col("ticker") == "", None).otherwise(F.col("ticker")))
    df = df.withColumn("date", F.to_date("date_s", "yyyy-MM-dd"))
    for c in NUM_COLS:
        df = df.withColumn(c, F.col(c + "_s").cast("double"))

    def blank(c):
        return F.col(c).isNull() | (F.col(c) == "")

    checks = {
        "wrong_field_count": F.col("n_fields") != 7,
        "missing_ticker": F.col("ticker").isNull(),
        "missing_date": blank("date_s"),
        "bad_date": ~blank("date_s") & F.col("date").isNull(),
    }
    for c in NUM_COLS:
        checks["missing_" + c] = blank(c + "_s")
        checks["non_numeric_" + c] = ~blank(c + "_s") & F.col(c).isNull()
    checks.update(
        {
            "high_below_low": F.col("high") < F.col("low"),
            "open_outside_range": (F.col("open") > F.col("high")) | (F.col("open") < F.col("low")),
            "close_outside_range": (F.col("close") > F.col("high")) | (F.col("close") < F.col("low")),
            "price_not_positive": F.least(*[F.col(c) for c in PRICE_COLS]) <= 0,
            "negative_volume": F.col("volume") < 0,
            "zero_volume": F.col("volume") == 0,
            "volume_not_integer": F.col("volume") != F.floor("volume"),
        }
    )
    reason_cols = [F.when(cond, F.lit(name)) for name, cond in checks.items()]
    # Note: F.array_remove(arr, None) returns NULL in Spark, which would hide every problem; use filter.
    df = df.withColumn("reasons", F.filter(F.array(*reason_cols), lambda x: x.isNotNull()))

    # Duplicates on the natural key (ticker, date): exact copies vs conflicting values.
    key = Window.partitionBy("ticker", "date")
    df = df.withColumn("key_count", F.count(F.lit(1)).over(key)).withColumn(
        "key_versions", F.size(F.collect_set("value").over(key))
    )
    df = df.withColumn(
        "reasons",
        F.when(
            F.col("key_count") > 1,
            F.concat(
                "reasons",
                F.array(F.when(F.col("key_versions") > 1, "duplicate_conflicting").otherwise("duplicate_exact")),
            ),
        ).otherwise(F.col("reasons")),
    )
    df = df.cache()

    # Reasons that make a row unusable for prices (zero volume alone is suspicious, not invalid).
    soft = ["zero_volume"]
    df = df.withColumn("hard_problem", F.size(F.array_except("reasons", F.array(*[F.lit(s) for s in soft]))) > 0)

    exploded = df.select(F.explode("reasons").alias("reason"))
    reason_counts = {r["reason"]: r["n"] for r in exploded.groupBy("reason").agg(F.count("*").alias("n")).collect()}
    n_rows = df.count()
    n_rows_any_problem = df.filter(F.size("reasons") > 0).count()
    n_rows_hard_problem = df.filter("hard_problem").count()
    n_dup_keys = df.filter("key_count > 1").select("ticker", "date").distinct().count()
    n_rows_null_any = df.filter(
        F.exists("reasons", lambda x: x.startswith("missing_") | x.startswith("non_numeric_"))
    ).count()

    # Reconciliation against the brief's reference window and "accepted bars" figure.
    in_period = F.col("date").between(F.lit(a.study_start).cast("date"), F.lit(a.study_end).cast("date"))
    period = df.agg(
        F.sum(in_period.cast("int")).alias("in_period"),
        F.sum((F.col("date") > F.lit(a.study_end).cast("date")).cast("int")).alias("after_period"),
        F.sum((F.col("date") < F.lit(a.study_start).cast("date")).cast("int")).alias("before_period"),
        F.sum((in_period & ~F.col("hard_problem")).cast("int")).alias("in_period_no_hard_problem"),
        F.sum((in_period & ~F.col("hard_problem") & (F.col("volume") > 0)).cast("int")).alias(
            "in_period_no_hard_problem_positive_volume"
        ),
    ).first()

    good = df.filter(F.col("ticker").isNotNull() & F.col("date").isNotNull())

    # Market calendar from the data itself.
    n_series = good.select("ticker").distinct().count()
    cal = good.groupBy("date").agg(F.countDistinct("ticker").alias("n_series")).orderBy("date")
    cal_rows = cal.collect()
    cal_dates = [r["date"] for r in cal_rows if r["n_series"] >= CALENDAR_SHARE * n_series]
    write_csv(
        os.path.join(a.out, "calendar.csv"),
        [{"date": r["date"].isoformat(), "n_series": r["n_series"]} for r in cal_rows],
        ["date", "n_series"],
    )
    cal_df = spark.createDataFrame([(d,) for d in cal_dates], "date date")

    # Per-series coverage: missing market days between the series' own first and last bar.
    span = good.groupBy("ticker").agg(
        F.min("date").alias("first_date"),
        F.max("date").alias("last_date"),
        F.count("*").alias("n_rows"),
        F.sum(F.col("hard_problem").cast("int")).alias("n_problem_rows"),
        F.sum((F.col("volume") == 0).cast("int")).alias("n_zero_volume"),
    )
    expected = (
        span.crossJoin(cal_df)
        .filter((F.col("date") >= F.col("first_date")) & (F.col("date") <= F.col("last_date")))
        .select("ticker", "date")
    )
    missing = expected.join(good.select("ticker", "date").distinct(), ["ticker", "date"], "left_anti")
    miss_per = missing.groupBy("ticker").agg(F.count("*").alias("n_missing_market_days"))
    off_cal = good.join(cal_df, "date", "left_anti").groupBy("ticker").agg(F.count("*").alias("n_off_calendar_rows"))

    # Overnight jumps (split screening, HANDOVER section 6.2).
    w = Window.partitionBy("ticker").orderBy("date")
    jumps = (
        good.filter(~F.col("hard_problem"))
        .withColumn("prev_close", F.lag("close").over(w))
        .withColumn("prev_date", F.lag("date").over(w))
        .withColumn("log_gap", F.log(F.col("open") / F.col("prev_close")))
        .filter(F.abs("log_gap") > JUMP_THRESHOLD)
        .select("ticker", "prev_date", "date", "prev_close", "open", "log_gap")
        .orderBy("ticker", "date")
        .collect()
    )
    jump_rows = []
    for r in jumps:
        ratio = r["open"] / r["prev_close"]
        k, err = nearest_split(ratio)
        jump_rows.append(
            {
                "ticker": r["ticker"],
                "prev_date": r["prev_date"].isoformat(),
                "date": r["date"].isoformat(),
                "prev_close": r["prev_close"],
                "open": r["open"],
                "ratio": round(ratio, 6),
                "log_gap": round(r["log_gap"], 6),
                "nearest_k": k,
                "k_rel_error": round(err, 4),
                "split_candidate_price_rule": err <= 0.03,
            }
        )
    write_csv(os.path.join(a.out, "overnight_jumps.csv"), jump_rows, list(jump_rows[0]) if jump_rows else ["ticker"])
    jump_count = {}
    for j in jump_rows:
        jump_count[j["ticker"]] = jump_count.get(j["ticker"], 0) + 1

    per = (
        span.join(miss_per, "ticker", "left")
        .join(off_cal, "ticker", "left")
        .fillna(0, ["n_missing_market_days", "n_off_calendar_rows"])
        .orderBy("ticker")
        .collect()
    )
    first_cal, last_cal = cal_dates[0], cal_dates[-1]
    per_rows = []
    for r in per:
        per_rows.append(
            {
                "ticker": r["ticker"],
                "first_date": r["first_date"].isoformat(),
                "last_date": r["last_date"].isoformat(),
                "n_rows": r["n_rows"],
                "n_missing_market_days": r["n_missing_market_days"],
                "n_off_calendar_rows": r["n_off_calendar_rows"],
                "n_problem_rows": r["n_problem_rows"],
                "n_zero_volume": r["n_zero_volume"],
                "n_overnight_jumps": jump_count.get(r["ticker"], 0),
                "starts_late": r["first_date"] > first_cal,
                "ends_early": r["last_date"] < last_cal,
            }
        )
    write_csv(os.path.join(a.out, "per_ticker.csv"), per_rows, list(per_rows[0]))

    problems = (
        df.filter(F.size("reasons") > 0)
        .select(F.col("value").alias("raw_line"), F.array_join("reasons", ";").alias("reasons"))
        .orderBy("raw_line")
        .collect()
    )
    write_csv(os.path.join(a.out, "problem_rows.csv"), [r.asDict() for r in problems], ["raw_line", "reasons"])

    total_missing = sum(r["n_missing_market_days"] for r in per_rows)
    full_grid = n_series * len(cal_dates)
    summary = {
        "run_id": a.run_id,
        "input": a.input,
        "profiled_at_utc": started,
        "spark_version": spark.version,
        "header": header,
        "header_matches_expected": header == EXPECTED_HEADER,
        "n_lines": n_lines,
        "n_header_lines": n_header_lines,
        "n_data_rows": n_rows,
        "n_series": n_series,
        "date_min": min(r["first_date"] for r in per_rows),
        "date_max": max(r["last_date"] for r in per_rows),
        "n_market_days": len(cal_dates),
        "n_dates_below_calendar_share": len(cal_rows) - len(cal_dates),
        "full_grid_series_x_days": full_grid,
        "coverage_of_full_grid": round(n_rows / full_grid, 4) if full_grid else None,
        "n_missing_market_days_within_series_span": total_missing,
        "n_series_starting_late": sum(r["starts_late"] for r in per_rows),
        "n_series_ending_early": sum(r["ends_early"] for r in per_rows),
        "n_series_with_gaps": sum(r["n_missing_market_days"] > 0 for r in per_rows),
        "n_rows_any_problem": n_rows_any_problem,
        "n_rows_hard_problem": n_rows_hard_problem,
        "n_rows_with_missing_or_non_numeric": n_rows_null_any,
        "n_rows_without_hard_problem": n_rows - n_rows_hard_problem,
        "n_duplicate_keys": n_dup_keys,
        "reason_counts": dict(sorted(reason_counts.items())),
        "n_overnight_jumps": len(jump_rows),
        "n_split_candidates_price_rule": sum(j["split_candidate_price_rule"] for j in jump_rows),
        "study_period": [a.study_start, a.study_end],
        "n_rows_before_study_period": period["before_period"],
        "n_rows_after_study_period": period["after_period"],
        "n_rows_in_study_period": period["in_period"],
        "n_rows_in_study_period_without_hard_problem": period["in_period_no_hard_problem"],
        "n_rows_in_study_period_without_hard_problem_positive_volume": period[
            "in_period_no_hard_problem_positive_volume"
        ],
        "brief_reference_accepted_bars": 605887,
        "notes": "hard_problem = any reason except zero_volume. Split candidates still need the volume "
        "check and manual review (HANDOVER 6.2).",
    }
    with open(os.path.join(a.out, "summary.json"), "w") as fh:
        json.dump(summary, fh, indent=2, default=str)
    print(json.dumps(summary, indent=2, default=str))
    spark.stop()


if __name__ == "__main__":
    main()
