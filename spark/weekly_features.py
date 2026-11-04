#!/usr/bin/env python3
"""Weekly universe, labels, features and within-week ranks -> features.weekly (PIPE-07, PIPE-08).

Reads Hive tables: clean.prices_daily, features.market_calendar, features.calendar_weeks, clean.split_events.
Writes Parquet partitioned by split to <root>/features/<run_id>/weekly (registered by hive/40_features_weekly.sql).

One row per ticker x week for every train / validation / test week (HANDOVER 7.1 to 7.6):
  eligible  = bar on the cutoff day with 61 consecutive clean market-day closes ending there (no gaps),
              clean open on start_date and on next_start_date,
              no confirmed split inside the lookback window (cutoff-60, cutoff] or the outcome window
              (start, next_start]. A split exactly on start_date is fine: features use pre-split closes,
              the outcome uses two post-split opens.
  ineligible_reason = first failed rule (no_bar_at_cutoff, insufficient_history, missing_start_open,
              missing_next_open, split_in_window); null when eligible.
  ratio     = open(next_start_date) / open(start_date), the source price ratio. NEVER a feature.
  label     = 1 if ratio > median ratio of eligible stocks that week, else 0; null when not eligible.
Features (HANDOVER 7.4), all from closes, highs, lows and volumes up to and including the cutoff day:
  rev_1w ln(C0/C-5); mom_4w ln(C0/C-20); mom_12w ln(C0/C-60); mom_12w_skip ln(C-5/C-60);
  vol_20d std of 20 daily log close ratios; vol_ratio ln(Parkinson vol 5d / 60d);
  max_move_20d max |daily log close ratio| over 20 days; rel_volume ln(mean vol 5d / mean vol 60d);
  dollar_volume ln(mean(close x volume) over 60d).
  F1 = mom_4w, mom_12w, vol_20d, rel_volume.   F2 = rev_1w, mom_4w, mom_12w_skip, vol_20d, vol_ratio,
  max_move_20d, rel_volume, dollar_volume.
Ranks: <feature>_rank = percent_rank among that week's eligible stocks with a non-null value (0..1).
Ranks use same-week information only and fit no parameters, so they are identical in every split.

  spark-submit spark/weekly_features.py --root /inf2006 --run-id <run_id> [--summary evidence/...json]
"""

import argparse
import json
import math

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

FEATURES = [
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
F1 = ["mom_4w", "mom_12w", "vol_20d", "rel_volume"]
F2 = ["rev_1w", "mom_4w", "mom_12w_skip", "vol_20d", "vol_ratio", "max_move_20d", "rel_volume", "dollar_volume"]
MODEL_SPLITS = ["train", "validation", "test"]
LOOKBACK = 60  # 61 closes: cutoff-60 .. cutoff


def safe_ln(x):
    return F.when(x > 0, F.log(x))


def daily_features(bars):
    """Features as of each bar's close. Valid only where hist_ok (61 consecutive market days)."""
    w = Window.partitionBy("ticker").orderBy("day_idx")

    def win(n):
        return w.rowsBetween(-(n - 1), 0)

    b = (
        bars.withColumn("prev_idx", F.lag("day_idx").over(w))
        .withColumn(
            "r", F.when(F.col("prev_idx") == F.col("day_idx") - 1, F.log(F.col("close") / F.lag("close").over(w)))
        )
        .withColumn("park", F.pow(F.log(F.col("high") / F.col("low")), 2) / (4 * math.log(2)))
        .withColumn("c5", F.lag("close", 5).over(w))
        .withColumn("c20", F.lag("close", 20).over(w))
        .withColumn("c60", F.lag("close", LOOKBACK).over(w))
        .withColumn("idx60", F.lag("day_idx", LOOKBACK).over(w))
    )
    p5, p60 = F.avg("park").over(win(5)), F.avg("park").over(win(60))
    v5, v60 = F.avg("volume").over(win(5)), F.avg("volume").over(win(60))
    return b.select(
        "ticker",
        "day_idx",
        (F.col("idx60") == F.col("day_idx") - LOOKBACK).alias("hist_ok"),
        F.log(F.col("close") / F.col("c5")).alias("rev_1w"),
        F.log(F.col("close") / F.col("c20")).alias("mom_4w"),
        F.log(F.col("close") / F.col("c60")).alias("mom_12w"),
        F.log(F.col("c5") / F.col("c60")).alias("mom_12w_skip"),
        F.stddev_samp("r").over(win(20)).alias("vol_20d"),
        F.when((p5 > 0) & (p60 > 0), 0.5 * F.log(p5 / p60)).alias("vol_ratio"),
        F.max(F.abs("r")).over(win(20)).alias("max_move_20d"),
        F.when((v5 > 0) & (v60 > 0), F.log(v5 / v60)).alias("rel_volume"),
        safe_ln(F.avg(F.col("close") * F.col("volume")).over(win(60))).alias("dollar_volume"),
    )


def build(spark):
    cal = spark.table("features.market_calendar").select(F.col("trade_date").alias("date"), "day_idx")
    bars = spark.table("clean.prices_daily").join(cal, "date")
    weeks = spark.table("features.calendar_weeks").filter(F.col("split").isin(MODEL_SPLITS))
    feats = daily_features(bars)

    tickers = bars.select("ticker").distinct()
    grid = tickers.crossJoin(
        weeks.select("start_date", "next_start_date", "cutoff_date", "split", "start_idx", "cutoff_idx", "next_idx")
    )
    at_cut = feats.withColumnRenamed("day_idx", "cutoff_idx").withColumn("has_cutoff_bar", F.lit(True))
    o_start = bars.select("ticker", F.col("day_idx").alias("start_idx"), F.col("open").alias("open_start"))
    o_next = bars.select("ticker", F.col("day_idx").alias("next_idx"), F.col("open").alias("open_next"))
    df = (
        grid.join(at_cut, ["ticker", "cutoff_idx"], "left")
        .join(o_start, ["ticker", "start_idx"], "left")
        .join(o_next, ["ticker", "next_idx"], "left")
    )

    # Confirmed splits inside (cutoff-60, next_start], except exactly on start_date.
    ev = (
        spark.table("clean.split_events")
        .filter(F.col("decision") == "confirmed")
        .join(cal, "date")
        .select("ticker", F.col("day_idx").alias("event_idx"))
    )
    hits = (
        df.select("ticker", "start_date", "cutoff_idx", "start_idx", "next_idx")
        .join(ev, "ticker")
        .filter(
            (F.col("event_idx") > F.col("cutoff_idx") - LOOKBACK)
            & (F.col("event_idx") <= F.col("next_idx"))
            & (F.col("event_idx") != F.col("start_idx"))
        )
        .select("ticker", "start_date")
        .distinct()
        .withColumn("split_hit", F.lit(True))
    )
    df = df.join(hits, ["ticker", "start_date"], "left")

    reason = (
        F.when(F.col("has_cutoff_bar").isNull(), "no_bar_at_cutoff")
        .when(~F.coalesce(F.col("hist_ok"), F.lit(False)), "insufficient_history")
        .when(F.col("open_start").isNull(), "missing_start_open")
        .when(F.col("open_next").isNull(), "missing_next_open")
        .when(F.col("split_hit").isNotNull(), "split_in_window")
    )
    df = df.withColumn("ineligible_reason", reason).withColumn("eligible", F.col("ineligible_reason").isNull())
    df = df.withColumn("ratio", F.col("open_next") / F.col("open_start"))
    for c in FEATURES:  # features only describe eligible stock-weeks
        df = df.withColumn(c, F.when(F.col("eligible"), F.col(c)))

    wk = Window.partitionBy("start_date")
    med = df.filter("eligible").groupBy("start_date").agg(F.expr("percentile(ratio, 0.5)").alias("median_ratio"))
    df = df.join(med, "start_date", "left").withColumn(
        "label", F.when(F.col("eligible"), (F.col("ratio") > F.col("median_ratio")).cast("int"))
    )
    for c in FEATURES:
        rk = Window.partitionBy("start_date", F.col(c).isNull()).orderBy(c)
        df = df.withColumn(c + "_rank", F.when(F.col(c).isNotNull(), F.percent_rank().over(rk)))
    df = df.withColumn("n_eligible_week", F.sum(F.col("eligible").cast("int")).over(wk))

    cols = (
        ["ticker", "start_date", "cutoff_date", "next_start_date", "eligible", "ineligible_reason", "ratio", "label"]
        + FEATURES
        + [c + "_rank" for c in FEATURES]
        + ["n_eligible_week", "split"]
    )
    return df.select(*cols)


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--root", default="/inf2006")
    p.add_argument("--run-id", required=True)
    p.add_argument("--summary", default=None)
    a = p.parse_args(argv)
    spark = SparkSession.builder.appName(f"weekly_features_{a.run_id}").enableHiveSupport().getOrCreate()
    out = f"{a.root}/features/{a.run_id}/weekly"
    df = build(spark).withColumn("run_id", F.lit(a.run_id))
    df.repartition("split").write.mode("errorifexists").partitionBy("split").parquet(out)

    w = spark.read.parquet(out)
    per_split = {}
    for r in (
        w.groupBy("split")
        .agg(
            F.countDistinct("start_date").alias("weeks"),
            F.count("*").alias("ticker_weeks"),
            F.sum(F.col("eligible").cast("int")).alias("eligible"),
            F.avg("label").alias("label_mean"),
            F.min("start_date").alias("first_start"),
            F.max("start_date").alias("last_start"),
            F.min("n_eligible_week").alias("min_eligible_per_week"),
            F.max("n_eligible_week").alias("max_eligible_per_week"),
        )
        .collect()
    ):
        per_split[r["split"]] = {
            k: (str(v) if k.endswith("start") else v) for k, v in r.asDict().items() if k != "split"
        }
    reasons = {
        f"{r['split']}/{r['ineligible_reason']}": r["n"]
        for r in w.filter("NOT eligible").groupBy("split", "ineligible_reason").agg(F.count("*").alias("n")).collect()
    }
    summary = {
        "run_id": a.run_id,
        "application_id": spark.sparkContext.applicationId,
        "output": out,
        "per_split": per_split,
        "ineligible_by_split_and_reason": dict(sorted(reasons.items())),
        "feature_sets": {"F1": F1, "F2": F2},
    }
    print(json.dumps(summary, indent=2, default=str))
    if a.summary:
        with open(a.summary, "w") as fh:
            json.dump(summary, fh, indent=2, default=str)
    spark.stop()


if __name__ == "__main__":
    main()
