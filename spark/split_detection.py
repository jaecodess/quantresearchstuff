#!/usr/bin/env python3
"""Split screening on clean prices (PIPE-05, HANDOVER 6.2). Prices are never back-adjusted here.

1. Overnight moves: |ln(open_t / close_t-1)| > 0.25 between consecutive market days of the same series.
2. Price rule: ratio within +/-3% of 1/k (split) or k (reverse split), k in {1.5, 2, 3, 4, 5, 10}.
3. Volume rule: mean volume of the 20 days from the move vs the 20 days before shifts by roughly k
   (between k/2 and 2k for a split; the inverse for a reverse split).
4. Screening: a move within +/-5% of a split ratio is flagged for_review (wider than the price rule, so a
   split with a slightly noisy open such as DISCA 2014-08-07 at 4.2% is not missed). Neither rule alone
   decides: every flagged move is reviewed by hand and recorded with a decision and its evidence in
   config/split_events.csv, the reviewed table the pipeline uses.

Reads Hive tables clean.prices_daily and features.market_calendar. Writes every large move with both
rule results to --out (a local CSV for evidence/pipeline/).

  spark-submit spark/split_detection.py --out evidence/pipeline/split_screen_<run_id>.csv
"""

import argparse
import csv

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

JUMP = 0.25
KS = [1.5, 2, 3, 4, 5, 10]
PRICE_TOL = 0.03
REVIEW_TOL = 0.05


def nearest_k(ratio):
    best = None
    for k in KS:
        for target, kind in ((1.0 / k, "split"), (float(k), "reverse")):
            err = abs(ratio / target - 1.0)
            if best is None or err < best[2]:
                best = (k, kind, err)
    return best


def volume_rule(kind, k, vol_shift):
    if vol_shift is None or vol_shift <= 0:
        return False
    if kind == "split":
        return k / 2 <= vol_shift <= 2 * k
    return 1 / (2 * k) <= vol_shift <= 2 / k


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True)
    a = p.parse_args(argv)
    spark = SparkSession.builder.appName("split_detection").enableHiveSupport().getOrCreate()

    bars = spark.table("clean.prices_daily").join(
        spark.table("features.market_calendar").select(F.col("trade_date").alias("date"), "day_idx"), "date"
    )
    w = Window.partitionBy("ticker").orderBy("day_idx")
    after = w.rowsBetween(0, 19)
    before = w.rowsBetween(-20, -1)
    moves = (
        bars.withColumn("prev_close", F.lag("close").over(w))
        .withColumn("prev_idx", F.lag("day_idx").over(w))
        .withColumn("prev_date", F.lag("date").over(w))
        .withColumn("vol_after", F.avg("volume").over(after))
        .withColumn("vol_before", F.avg("volume").over(before))
        .filter(F.col("prev_idx") == F.col("day_idx") - 1)
        .withColumn("log_gap", F.log(F.col("open") / F.col("prev_close")))
        .filter(F.abs("log_gap") > JUMP)
        .select("ticker", "prev_date", "date", "prev_close", "open", "log_gap", "vol_before", "vol_after")
        .orderBy("ticker", "date")
        .collect()
    )
    rows = []
    for r in moves:
        ratio = r["open"] / r["prev_close"]
        k, kind, err = nearest_k(ratio)
        shift = r["vol_after"] / r["vol_before"] if r["vol_before"] else None
        price_ok = err <= PRICE_TOL
        vol_ok = volume_rule(kind, k, shift)
        rows.append(
            {
                "ticker": r["ticker"],
                "prev_date": r["prev_date"].isoformat(),
                "date": r["date"].isoformat(),
                "prev_close": r["prev_close"],
                "open": r["open"],
                "ratio": round(ratio, 6),
                "log_gap": round(r["log_gap"], 6),
                "nearest_k": k,
                "kind": kind,
                "k_rel_error": round(err, 4),
                "volume_shift": round(shift, 3) if shift is not None else "",
                "price_rule": price_ok,
                "volume_rule": vol_ok,
                "both_rules": price_ok and vol_ok,
                "for_review": err <= REVIEW_TOL,
            }
        )
    with open(a.out, "w", newline="") as fh:
        fields = list(rows[0]) if rows else ["ticker"]
        wr = csv.DictWriter(fh, fieldnames=fields, lineterminator="\n")
        wr.writeheader()
        wr.writerows(rows)
    print(f"application_id={spark.sparkContext.applicationId}")
    print(
        f"large overnight moves: {len(rows)}; price rule: {sum(r['price_rule'] for r in rows)}; "
        f"both rules: {sum(r['both_rules'] for r in rows)}; for review: {sum(r['for_review'] for r in rows)}"
    )
    for r in rows:
        if r["for_review"]:
            print(r)
    spark.stop()


if __name__ == "__main__":
    main()
