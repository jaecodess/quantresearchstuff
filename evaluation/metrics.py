"""Evaluation module (MODEL-01, HANDOVER 8.4 and 8.5).

Weeks are the unit of analysis: every metric is first computed per week in Spark (distributed, one row per
week and model), and only that small weekly table reaches the driver for the bootstrap.

  weekly_metrics(df, score_cols)  -> one row per (model_id, start_date): n, n_pos, auc, rank_ic, decile_spread
  pooled_classification(df, ...)  -> balanced accuracy, precision, recall (predict 1 above the week's median score)
  top_weights(df, score_col)      -> top 10% equal weights per week (hypothetical selection output)
  turnover(weights)               -> mean share of the top set replaced from one week to the next
  bootstrap_mean(values)          -> mean and 95% interval from resampling weeks (10,000 draws, fixed seed)

AUC per week uses the rank-sum formula with average ranks for ties (random-forest scores often tie):
  AUC_w = (R+ - n+(n+ + 1)/2) / (n+ * n-),  R+ = sum of average ranks of label-1 rows by score.
"""

import math
import random

from pyspark.sql import Window
from pyspark.sql import functions as F

WEEK = "start_date"
SEED = 2026
N_BOOT = 10_000


def _avg_rank(col, part):
    """Average rank (1-based, ties share the mean of their positions) of `col` within partition `part`."""
    w = Window.partitionBy(*part).orderBy(col)
    tie = Window.partitionBy(*part, col)
    return F.rank().over(w) + (F.count(F.lit(1)).over(tie) - 1) / 2.0


def long_scores(df, score_cols):
    """Stack score columns into (model_id, score) rows; keeps ticker, week, split, label, ratio."""
    keep = ["ticker", WEEK, "split", "label", "ratio"]
    stacked = None
    for c in score_cols:
        part = df.select(*keep, F.lit(c).alias("model_id"), F.col(c).cast("double").alias("score"))
        stacked = part if stacked is None else stacked.unionByName(part)
    return stacked


def weekly_metrics(long_df):
    """Per (model_id, week): n, n_pos, AUC (tie-aware), rank IC (Spearman with ratio), decile spread of ln(ratio)."""
    part = ["model_id", WEEK]
    d = long_df.withColumn("r_score", _avg_rank("score", part)).withColumn("r_ratio", _avg_rank("ratio", part))
    d = d.withColumn("decile", F.ntile(10).over(Window.partitionBy(*part).orderBy("score", "ticker")))
    agg = d.groupBy(*part, "split").agg(
        F.count("*").alias("n"),
        F.sum("label").alias("n_pos"),
        F.sum(F.when(F.col("label") == 1, F.col("r_score"))).alias("r_pos"),
        F.corr("r_score", "r_ratio").alias("rank_ic"),
        F.avg(F.when(F.col("decile") == 10, F.log("ratio"))).alias("top_ln"),
        F.avg(F.when(F.col("decile") == 1, F.log("ratio"))).alias("bottom_ln"),
    )
    n_neg = F.col("n") - F.col("n_pos")
    auc = (F.col("r_pos") - F.col("n_pos") * (F.col("n_pos") + 1) / 2.0) / (F.col("n_pos") * n_neg)
    return agg.select(
        *part,
        "split",
        "n",
        "n_pos",
        F.when((F.col("n_pos") > 0) & (n_neg > 0), auc).alias("auc"),
        "rank_ic",
        (F.col("top_ln") - F.col("bottom_ln")).alias("decile_spread"),
    )


def pooled_classification(long_df):
    """Predict 1 for scores above the week's median score (balanced each week); pooled counts per model."""
    med = long_df.groupBy("model_id", WEEK).agg(F.expr("percentile(score, 0.5)").alias("med"))
    d = long_df.join(med, ["model_id", WEEK]).withColumn("pred", (F.col("score") > F.col("med")).cast("int"))
    c = d.groupBy("model_id").agg(
        F.sum(((F.col("pred") == 1) & (F.col("label") == 1)).cast("int")).alias("tp"),
        F.sum(((F.col("pred") == 1) & (F.col("label") == 0)).cast("int")).alias("fp"),
        F.sum(((F.col("pred") == 0) & (F.col("label") == 0)).cast("int")).alias("tn"),
        F.sum(((F.col("pred") == 0) & (F.col("label") == 1)).cast("int")).alias("fn"),
    )
    out = {}
    for r in c.collect():
        tp, fp, tn, fn = r["tp"], r["fp"], r["tn"], r["fn"]
        tpr = tp / (tp + fn) if tp + fn else float("nan")
        tnr = tn / (tn + fp) if tn + fp else float("nan")
        out[r["model_id"]] = {
            "balanced_accuracy": (tpr + tnr) / 2,
            "precision": tp / (tp + fp) if tp + fp else float("nan"),
            "recall": tpr,
            "tp": tp,
            "fp": fp,
            "tn": tn,
            "fn": fn,
        }
    return out, d


def top_weights(df, score_col, share=0.10):
    """Top ceil(share * n_w) stocks by score each week get weight 1/k; others 0. Ties broken by ticker."""
    w = Window.partitionBy(WEEK)
    rn = F.row_number().over(Window.partitionBy(WEEK).orderBy(F.desc(score_col), "ticker"))
    k = F.ceil(F.count(F.lit(1)).over(w) * share)
    return df.select("ticker", WEEK, rn.alias("rn"), k.alias("k")).select(
        "ticker", WEEK, F.when(F.col("rn") <= F.col("k"), 1.0 / F.col("k")).otherwise(0.0).alias("weight")
    )


def turnover(weights_rows):
    """Mean share of the top set that is new compared with the previous week. Input: (ticker, week, weight) rows."""
    sets = {}
    for r in weights_rows:
        if r["weight"] > 0:
            sets.setdefault(r[WEEK], set()).add(r["ticker"])
    weeks = sorted(sets)
    shares = [1 - len(sets[b] & sets[a]) / len(sets[b]) for a, b in zip(weeks, weeks[1:])]
    return sum(shares) / len(shares) if shares else float("nan")


def bootstrap_mean(values, n_boot=N_BOOT, seed=SEED, level=0.95):
    """Mean of weekly values with a percentile bootstrap interval from resampling weeks with replacement."""
    vals = [v for v in values if v is not None and not math.isnan(v)]
    if not vals:
        return {"n_weeks": 0, "mean": None, "lo": None, "hi": None, "sd": None}
    rnd = random.Random(seed)
    n = len(vals)
    means = sorted(sum(rnd.choice(vals) for _ in range(n)) / n for _ in range(n_boot))
    a = (1 - level) / 2
    mean = sum(vals) / n
    sd = (sum((v - mean) ** 2 for v in vals) / (n - 1)) ** 0.5 if n > 1 else 0.0
    return {
        "n_weeks": n,
        "mean": mean,
        "lo": means[int(a * n_boot)],
        "hi": means[int((1 - a) * n_boot) - 1],
        "sd": sd,
        "se": sd / n**0.5,
    }


def paired_bootstrap(weekly, model_a, model_b, metric="auc", **kw):
    """Bootstrap of the mean weekly difference model_a - model_b on the weeks both have."""
    a = {r[WEEK]: r[metric] for r in weekly if r["model_id"] == model_a and r[metric] is not None}
    b = {r[WEEK]: r[metric] for r in weekly if r["model_id"] == model_b and r[metric] is not None}
    return bootstrap_mean([a[w] - b[w] for w in sorted(set(a) & set(b))], **kw)
