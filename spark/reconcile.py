#!/usr/bin/env python3
"""Row-count reconciliation and split report for the pipeline (PIPE-10).

Reads the Hive tables and writes small evidence files (aggregates only):
  <out>/reconciliation_<run_id>.json   raw = clean + rejected per source; per-split weeks, stock-weeks,
                                       eligible rows, label balance, exclusions by reason (incl. splits),
                                       date coverage, holiday weeks; GDELT totals when present
  <out>/eligible_per_week_<run_id>.csv one row per week: split, eligible stocks, label share

  spark-submit spark/reconcile.py --run-id <run_id> --out evidence/pipeline
"""

import argparse
import csv
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def one(df):
    return {k: (str(v) if hasattr(v, "isoformat") else v) for k, v in df.first().asDict().items()}


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", required=True)
    p.add_argument("--out", default="evidence/pipeline")
    a = p.parse_args(argv)
    os.makedirs(a.out, exist_ok=True)
    spark = SparkSession.builder.appName(f"reconcile_{a.run_id}").enableHiveSupport().getOrCreate()
    tables = {t.name for db in ("raw", "clean", "features") for t in spark.catalog.listTables(db)}
    rep = {"run_id": a.run_id, "application_id": spark.sparkContext.applicationId}

    # Prices: raw = clean + rejected.
    raw_rows = spark.table("raw.prices_csv").filter("date_s != 'date'").count()
    clean = spark.table("clean.prices_daily")
    rej = spark.table("clean.prices_rejected")
    n_clean, n_rej = clean.count(), rej.count()
    rep["prices"] = {
        "raw_rows": raw_rows,
        "clean_rows": n_clean,
        "rejected_rows": n_rej,
        "reconciles": raw_rows == n_clean + n_rej,
        "rejected_by_reason": {
            r["reject_reason"]: r["n"] for r in rej.groupBy("reject_reason").agg(F.count("*").alias("n")).collect()
        },
        "clean_coverage": one(
            clean.agg(
                F.min("date").alias("first"), F.max("date").alias("last"), F.countDistinct("ticker").alias("series")
            )
        ),
    }

    # Calendar and weeks.
    weeks = spark.table("features.calendar_weeks")
    rep["calendar"] = {
        "market_days": spark.table("features.market_calendar").count(),
        "weeks_by_split": {r["split"]: r["n"] for r in weeks.groupBy("split").agg(F.count("*").alias("n")).collect()},
        "holiday_friday_weeks": [
            str(r["anchor_friday"]) for r in weeks.filter("NOT start_is_friday").orderBy("anchor_friday").collect()
        ],
        "weeks_not_5_days": weeks.filter("n_trading_days != 5").count(),
    }

    # Features: per split.
    w = spark.table("features.weekly")
    per = {}
    for r in (
        w.groupBy("split")
        .agg(
            F.countDistinct("start_date").alias("weeks"),
            F.count("*").alias("ticker_weeks"),
            F.sum(F.col("eligible").cast("int")).alias("eligible"),
            F.avg("label").alias("label_share_1"),
            F.min("start_date").alias("first_start"),
            F.max("start_date").alias("last_start"),
            F.min("cutoff_date").alias("first_cutoff"),
            F.max("next_start_date").alias("last_outcome_end"),
        )
        .collect()
    ):
        per[r["split"]] = {k: (str(v) if hasattr(v, "isoformat") else v) for k, v in r.asDict().items() if k != "split"}
    for r in w.filter("NOT eligible").groupBy("split", "ineligible_reason").agg(F.count("*").alias("n")).collect():
        per[r["split"]].setdefault("excluded_by_reason", {})[r["ineligible_reason"]] = r["n"]
    wk = (
        w.groupBy("split", "start_date")
        .agg(F.sum(F.col("eligible").cast("int")).alias("n_eligible"), F.avg("label").alias("label_share_1"))
        .orderBy("start_date")
        .collect()
    )
    for s in per:
        n = sorted(r["n_eligible"] for r in wk if r["split"] == s and r["n_eligible"] > 0)
        per[s]["eligible_per_week"] = (
            {"weeks_with_eligible": len(n), "min": n[0], "median": n[len(n) // 2], "max": n[-1]} if n else {}
        )
    rep["features_weekly"] = per

    if "gdelt_daily_audit" in tables:
        g = spark.table("raw.gdelt_daily_audit")
        rep["gdelt"] = one(
            g.agg(
                F.count("*").alias("archive_dates"),
                F.sum("n_corrupt_archive").alias("corrupt_archives"),
                F.sum("n_lines").alias("lines"),
                F.sum("n_events").alias("events_kept"),
                F.sum("n_malformed").alias("malformed"),
                F.sum("n_bad_value").alias("bad_value"),
                F.sum("n_dup").alias("duplicates"),
            )
        )
        rep["gdelt"]["reconciles"] = rep["gdelt"]["lines"] == sum(
            rep["gdelt"][k] for k in ("events_kept", "malformed", "bad_value", "duplicates")
        )
    if "weekly_news" in tables:
        rep["weekly_news_weeks_with_news_abn"] = (
            spark.table("features.weekly_news").filter("news_abn IS NOT NULL").count()
        )

    path = os.path.join(a.out, f"reconciliation_{a.run_id}.json")
    with open(path, "w") as fh:
        json.dump(rep, fh, indent=2, default=str)
    with open(os.path.join(a.out, f"eligible_per_week_{a.run_id}.csv"), "w", newline="") as fh:
        wr = csv.writer(fh, lineterminator="\n")
        wr.writerow(["split", "start_date", "n_eligible", "label_share_1"])
        for r in wk:
            wr.writerow(
                [
                    r["split"],
                    r["start_date"],
                    r["n_eligible"],
                    "" if r["label_share_1"] is None else round(r["label_share_1"], 4),
                ]
            )
    print(json.dumps(rep, indent=2, default=str))
    print(f"wrote {path}")
    spark.stop()


if __name__ == "__main__":
    main()
