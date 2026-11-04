#!/usr/bin/env python3
"""Distributed cleaning of the daily price CSV (PIPE-04).

Every raw data row ends up in exactly one of two outputs, so raw = clean + rejected:
  <root>/clean/prices_daily/<run_id>/      contract table clean.prices_daily
  <root>/rejected/prices_daily/<run_id>/   contract table clean.prices_rejected (+ reject_reason)

Rules (HANDOVER 6.1, findings in docs/corpus/quality_findings.md). First matching reason wins:
  wrong_field_count, missing_ticker, bad_date, missing_<col>, non_numeric_<col>,
  price_not_positive, negative_volume, high_below_low, open_outside_range, close_outside_range,
  duplicate_key (every copy of a (ticker, date) that appears more than once; never pick one silently),
  outside_study_period (after --study-end or before --study-start; kept as raw, not used downstream).
Rows are never repaired or filled. Zero volume alone is kept (genuine no-trade days are possible).

  spark-submit spark/clean_prices.py --input "<raw csv path or glob>" --root /inf2006 --run-id <run_id>
Optional development profile: --ticker-mod N keeps tickers with crc32(ticker) % N == 0 (about 1/N of them).
"""

import argparse
import json

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

HEADER = ["date", "open", "high", "low", "close", "volume", "Name"]
NUM = ["open", "high", "low", "close", "volume"]


def build(spark, input_path, run_id, study_start, study_end, ticker_mod=None):
    """Return (tagged DataFrame with reject_reason, raw line count incl. header)."""
    # Line numbers via zipWithIndex: deterministic source_row (1 = header line) for provenance.
    rdd = spark.sparkContext.textFile(input_path).zipWithIndex()
    lines = spark.createDataFrame(
        rdd.map(lambda x: (x[0], x[1] + 1)),
        T.StructType([T.StructField("value", T.StringType()), T.StructField("source_row", T.LongType())]),
    )
    n_lines = lines.count()
    header_line = ",".join(HEADER)
    lines = lines.filter(F.col("value") != header_line)
    f = F.split("value", ",", -1)
    df = lines.select(
        "value",
        "source_row",
        F.size(f).alias("n_fields"),
        *[F.trim(f[i]).alias(c + "_s") for i, c in enumerate(HEADER)],
    )
    df = df.withColumn("ticker", F.when(F.col("Name_s") != "", F.col("Name_s")))
    if ticker_mod:
        df = df.filter((F.crc32(F.coalesce("ticker", F.lit(""))) % ticker_mod) == 0)
    df = df.withColumn("date", F.to_date("date_s", "yyyy-MM-dd"))
    for c in NUM:
        df = df.withColumn(c, F.col(c + "_s").cast("double"))
    df = df.withColumn("volume", F.col("volume").cast("long"))

    conds = [
        ("wrong_field_count", F.col("n_fields") != 7),
        ("missing_ticker", F.col("ticker").isNull()),
        ("bad_date", F.col("date").isNull()),
    ]
    for c in NUM:
        conds.append(("missing_" + c, F.col(c + "_s") == ""))
        conds.append(("non_numeric_" + c, F.col(c).isNull()))
    conds += [
        ("price_not_positive", F.least("open", "high", "low", "close") <= 0),
        ("negative_volume", F.col("volume") < 0),
        ("high_below_low", F.col("high") < F.col("low")),
        ("open_outside_range", (F.col("open") > F.col("high")) | (F.col("open") < F.col("low"))),
        ("close_outside_range", (F.col("close") > F.col("high")) | (F.col("close") < F.col("low"))),
    ]
    reason = F.lit(None).cast("string")
    for name, cond in reversed(conds):
        reason = F.when(F.coalesce(cond, F.lit(False)), F.lit(name)).otherwise(reason)
    df = df.withColumn("reject_reason", reason)

    # Duplicate natural keys among otherwise valid rows: reject every copy (reported, not resolved).
    key = Window.partitionBy("ticker", "date")
    df = df.withColumn("key_n", F.sum(F.col("reject_reason").isNull().cast("int")).over(key))
    df = df.withColumn(
        "reject_reason",
        F.when(F.col("reject_reason").isNull() & (F.col("key_n") > 1), "duplicate_key").otherwise(
            F.col("reject_reason")
        ),
    )
    outside = (F.col("date") < F.lit(study_start).cast("date")) | (F.col("date") > F.lit(study_end).cast("date"))
    df = df.withColumn(
        "reject_reason",
        F.when(F.col("reject_reason").isNull() & outside, "outside_study_period").otherwise(F.col("reject_reason")),
    )
    df = df.withColumn("source_file", F.lit(input_path.rstrip("/").split("/")[-1]))
    df = df.withColumn("ingest_run_id", F.lit(run_id))
    return df, n_lines


CLEAN_COLS = ["ticker", "date", "open", "high", "low", "close", "volume", "source_file", "source_row", "ingest_run_id"]


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--input", required=True)
    p.add_argument("--root", default="/inf2006")
    p.add_argument("--run-id", required=True)
    p.add_argument("--study-start", default="2013-02-08")
    p.add_argument("--study-end", default="2017-12-29")
    p.add_argument("--ticker-mod", type=int, default=None)
    p.add_argument("--summary", default=None, help="local JSON path for the row-count summary (evidence)")
    a = p.parse_args(argv)

    spark = SparkSession.builder.appName(f"clean_prices_{a.run_id}").getOrCreate()
    df, n_lines = build(spark, a.input, a.run_id, a.study_start, a.study_end, a.ticker_mod)
    df = df.cache()

    clean_path = f"{a.root}/clean/prices_daily/{a.run_id}"
    rej_path = f"{a.root}/rejected/prices_daily/{a.run_id}"
    clean = df.filter(F.col("reject_reason").isNull()).select(*CLEAN_COLS)
    # Small table (about 606k rows): one unpartitioned Parquet dataset keeps Hive registration simple.
    clean.repartition(4).write.mode("errorifexists").parquet(clean_path)
    rejected = df.filter(F.col("reject_reason").isNotNull()).select(
        *CLEAN_COLS, "reject_reason", F.col("value").alias("raw_line")
    )
    rejected.coalesce(1).write.mode("errorifexists").parquet(rej_path)

    counts = {
        r["reason"] or "clean": r["n"]
        for r in df.groupBy(F.col("reject_reason").alias("reason")).agg(F.count("*").alias("n")).collect()
    }
    n_data = sum(counts.values())
    summary = {
        "run_id": a.run_id,
        "application_id": spark.sparkContext.applicationId,
        "input": a.input,
        "ticker_mod": a.ticker_mod,
        "raw_lines_including_header": n_lines,
        "raw_data_rows_profiled": n_data,
        "clean_rows": counts.get("clean", 0),
        "rejected_rows": n_data - counts.get("clean", 0),
        "rejected_by_reason": {k: v for k, v in sorted(counts.items()) if k != "clean"},
        "reconciles": n_data == counts.get("clean", 0) + sum(v for k, v in counts.items() if k != "clean"),
        "clean_path": clean_path,
        "rejected_path": rej_path,
    }
    print(json.dumps(summary, indent=2))
    if a.summary:
        with open(a.summary, "w") as fh:
            json.dump(summary, fh, indent=2)
    spark.stop()


if __name__ == "__main__":
    main()
