#!/usr/bin/env python3
"""Task 3: baselines, the pre-declared grid, validation, the single test evaluation, saving and reloading.

Three modes, run as separate spark-submit calls (each a fresh Spark session):

  validate  Fit the 8 Gate-1 configs on 2013-15 train rows, score 2016 validation, compare with baselines
            B0-B3 on the same rows, apply the one-standard-error simplicity rule and print the recommended
            frozen config. Reads ONLY train and validation rows; the test split is filtered out on read.
  test      The single 2017 evaluation (MODEL-07). Refuses to run unless Gate 2 in the decision log names the
            same frozen config and is locked, and unless no test evaluation has been recorded before. Writes the
            marker first, refits the frozen config on train, saves the full PipelineModel, writes predictions,
            weekly evaluation, top-10% weights and all metrics, and applies the success rule.
  reload    Fresh session: load the saved PipelineModel, predict the test rows again without retraining and
            compare with the stored predictions (tolerance 1e-9).

  spark-submit modeling/run_model.py validate --run-id <run_id>
  spark-submit modeling/run_model.py test     --run-id <run_id> --frozen <config_id>
  spark-submit modeling/run_model.py reload   --run-id <run_id>
Input: Hive table features.weekly (or --features-path <parquet dir> for local development).
"""

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from pyspark import SparkContext  # noqa: E402
from pyspark.ml import Pipeline, PipelineModel  # noqa: E402
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier  # noqa: E402
from pyspark.ml.evaluation import BinaryClassificationEvaluator  # noqa: E402
from pyspark.ml.feature import Imputer, StandardScaler, VectorAssembler  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.column import Column  # noqa: E402

from evaluation import metrics as M  # noqa: E402

SEED = 2026
F1 = ["mom_4w", "mom_12w", "vol_20d", "rel_volume"]
F2 = ["rev_1w", "mom_4w", "mom_12w_skip", "vol_20d", "vol_ratio", "max_move_20d", "rel_volume", "dollar_volume"]
# Gate 1 grid, in the locked simplicity order (simplest first).
GRID = [
    ("lr_f1_r001", "lr", F1, {"regParam": 0.01}),
    ("lr_f1_r0", "lr", F1, {"regParam": 0.0}),
    ("lr_f2_r001", "lr", F2, {"regParam": 0.01}),
    ("lr_f2_r0", "lr", F2, {"regParam": 0.0}),
    ("rf_f1_d4", "rf", F1, {"maxDepth": 4}),
    ("rf_f1_d8", "rf", F1, {"maxDepth": 8}),
    ("rf_f2_d4", "rf", F2, {"maxDepth": 4}),
    ("rf_f2_d8", "rf", F2, {"maxDepth": 8}),
]
CONFIGS = {c[0]: c for c in GRID}
BASE_KEEP = ["ticker", "start_date", "split", "label", "ratio"]
DECISION_LOG = os.path.join(REPO, "docs", "decisions", "decision-log.md")
TOL = 1e-9


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_pipeline(config_id):
    _, kind, feats, params = CONFIGS[config_id]
    ranks = [f + "_rank" for f in feats]
    imp = [f + "_imp" for f in feats]
    stages = [
        Imputer(inputCols=ranks, outputCols=imp, strategy="median"),
        VectorAssembler(inputCols=imp, outputCol="features_raw"),
        StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True),
    ]
    if kind == "lr":
        stages.append(
            LogisticRegression(featuresCol="features", labelCol="label", maxIter=100, elasticNetParam=0.0, **params)
        )
    else:
        stages.append(
            RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=200, seed=SEED, **params)
        )
    return Pipeline(stages=stages)


def p1(col_name):
    """Probability of label 1 from Spark's probability vector, computed entirely in the JVM.

    Calls org.apache.spark.ml.functions.vector_to_array directly. A Python UDF would run on the core
    nodes, which need numpy to unpickle ML vectors (EMR core nodes lack it), and the Python wrapper
    pyspark.ml.functions imports pandas. This keeps every model step on the JVM side.
    """
    jvm = SparkContext._active_spark_context._jvm
    arr = Column(jvm.org.apache.spark.ml.functions.vector_to_array(F.col(col_name)._jc, "float64"))
    return arr.getItem(1)


def score(model, df, name):
    """Probability of label 1 as column `name`."""
    out = model.transform(df).withColumn(name, p1("probability"))
    return out.select(*[c for c in df.columns], name)


def load_rows(spark, a, splits):
    df = spark.read.parquet(a.features_path) if a.features_path else spark.table("features.weekly")
    # The test split is filtered out on read unless this is the guarded test/reload mode.
    return df.filter(F.col("eligible") & F.col("split").isin(splits)).withColumn("label", F.col("label").cast("double"))


def baseline_scores(df, b3):
    """B0 constant, B1 momentum rank, B2 reversal (1 - rank), B3 best single indicator (direction from train)."""
    col, flip = b3
    return (
        df.withColumn("B0_majority", F.lit(0.5))
        .withColumn("B1_mom_4w", F.col("mom_4w_rank"))
        .withColumn("B2_reversal", 1 - F.col("rev_1w_rank"))
        .withColumn("B3_best_single", (1 - F.col(col + "_rank")) if flip else F.col(col + "_rank"))
    )


def choose_b3(train):
    """Pick the F2 indicator with the highest training mean weekly AUC, flipped if it points the wrong way."""
    long = M.long_scores(train, [f + "_rank" for f in F2])
    wk = M.weekly_metrics(long).collect()
    res = {}
    for f in F2:
        vals = [r["auc"] for r in wk if r["model_id"] == f + "_rank" and r["auc"] is not None]
        m = sum(vals) / len(vals)
        res[f] = {"train_mean_weekly_auc": m, "flip": m < 0.5, "effective_auc": max(m, 1 - m)}
    best = max(F2, key=lambda f: res[f]["effective_auc"])
    return (best, res[best]["flip"]), res


def summarise(weekly, model_ids, split):
    rows = [r for r in weekly if r["split"] == split]
    out = {}
    for m in model_ids:
        auc = M.bootstrap_mean([r["auc"] for r in rows if r["model_id"] == m])
        ic = M.bootstrap_mean([r["rank_ic"] for r in rows if r["model_id"] == m])
        ds = M.bootstrap_mean([r["decile_spread"] for r in rows if r["model_id"] == m])
        out[m] = {"weekly_auc": auc, "rank_ic": ic, "decile_spread_ln_ratio": ds}
        if m != "B3_best_single":
            out[m]["paired_auc_vs_B3"] = M.paired_bootstrap(rows, m, "B3_best_single")
    return out


def select(summary):
    """Highest mean weekly AUC; then the simplest config within one SE of it (Gate 1)."""
    best = max((c[0] for c in GRID), key=lambda c: summary[c]["weekly_auc"]["mean"])
    b = summary[best]["weekly_auc"]
    for cid, *_ in GRID:  # simplicity order
        if summary[cid]["weekly_auc"]["mean"] >= b["mean"] - b["se"]:
            return cid, best, b["se"]
    return best, best, b["se"]


def weekly_rows(scored, ids):
    return [r.asDict() for r in M.weekly_metrics(M.long_scores(scored, ids)).collect()]


def write_json(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as fh:
        json.dump(obj, fh, indent=2, default=str)
    print(json.dumps(obj, indent=2, default=str)[:6000])
    print(f"wrote {path}")


def write_weekly_csv(path, rows):
    import csv

    with open(path, "w", newline="") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["split", "start_date", "model_id", "n", "n_pos", "auc", "rank_ic", "decile_spread"])
        for r in sorted(rows, key=lambda r: (r["split"], str(r["start_date"]), r["model_id"])):
            w.writerow(
                [
                    r["split"],
                    r["start_date"],
                    r["model_id"],
                    r["n"],
                    r["n_pos"],
                    r["auc"],
                    r["rank_ic"],
                    r["decile_spread"],
                ]
            )


# ---------------------------------------------------------------- validate
def validate(spark, a):
    df = load_rows(spark, a, ["train", "validation"]).cache()
    train, val = df.filter("split = 'train'"), df.filter("split = 'validation'")
    b3, b3_table = choose_b3(train)
    scored = baseline_scores(val, b3)
    for cid, *_ in GRID:
        model = build_pipeline(cid).fit(train)
        scored = score(model, scored, cid)
        print(f"fitted {cid}")
    ids = [c[0] for c in GRID] + ["B0_majority", "B1_mom_4w", "B2_reversal", "B3_best_single"]
    weekly = weekly_rows(scored.cache(), ids)
    summary = summarise(weekly, ids, "validation")
    chosen, best, se = select(summary)
    out = {
        "mode": "validate",
        "run_id": a.run_id,
        "application_id": spark.sparkContext.applicationId,
        "finished_utc": now(),
        "rows": {"train": train.count(), "validation": val.count()},
        "weeks_validation": len({r["start_date"] for r in weekly}),
        "b3_choice": {"indicator": b3[0], "flipped": b3[1], "training_table": b3_table},
        "validation_summary": summary,
        "selection": {
            "best_by_mean_weekly_auc": best,
            "one_se": se,
            "recommended_frozen_config": chosen,
            "rule": "simplest config (Gate 1 order) whose mean weekly AUC >= best mean - 1 SE",
        },
        "test_split_read": False,
    }
    write_json(os.path.join(a.out, f"validation_{a.run_id}.json"), out)
    write_weekly_csv(os.path.join(a.out, f"validation_weekly_{a.run_id}.csv"), weekly)
    print(f"RECOMMENDED FROZEN CONFIG: {chosen} (best {best}, 1 SE = {se:.4f})")


# ---------------------------------------------------------------- guards
def is_local(root):
    """Local development roots are existing local folders; on EMR /inf2006 is an HDFS path."""
    return root.startswith("file:") or os.path.isdir(root)


def gate2_locked(frozen, log_path):
    text = open(log_path).read()
    sec = text.split("## Gate 2", 1)[1].split("\n## ", 1)[0] if "## Gate 2" in text else ""
    m = re.search(r"Frozen configuration:\s*`?([\w.]+)`?", sec)
    locked = re.search(r"Locked on:\s*(\S+)", sec)
    ok = bool(m and m.group(1) == frozen and locked and "____" not in locked.group(1))
    return ok, (m.group(1) if m else None)


def marker_exists(path, local):
    if local:
        return os.path.exists(path)
    return subprocess.run(["hdfs", "dfs", "-test", "-e", path]).returncode == 0


def write_marker(path, obj, local):
    if local:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as fh:
            json.dump(obj, fh)
    else:
        tmp = "/tmp/test_marker.json"
        with open(tmp, "w") as fh:
            json.dump(obj, fh)
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", os.path.dirname(path)], check=True)
        subprocess.run(["hdfs", "dfs", "-put", tmp, path], check=True)


# ---------------------------------------------------------------- test
def test(spark, a):
    ok, named = gate2_locked(a.frozen, a.decision_log)
    if not ok:
        sys.exit(f"REFUSED: Gate 2 in {a.decision_log} must name '{a.frozen}' and be locked (found '{named}').")
    local = is_local(a.root)
    repo_marker = os.path.join(a.marker_dir, "TEST_EVALUATED.json")
    hdfs_marker = f"{a.root}/models/TEST_EVALUATED.json"
    if os.path.exists(repo_marker) or marker_exists(hdfs_marker, local):
        sys.exit("REFUSED: the test set has already been evaluated once (marker found). It is evaluated ONCE.")
    marker = {"run_id": a.run_id, "frozen": a.frozen, "started_utc": now()}
    write_marker(hdfs_marker, marker, local)  # written BEFORE any test row is scored
    os.makedirs(os.path.dirname(repo_marker), exist_ok=True)
    with open(repo_marker, "w") as fh:
        json.dump(marker, fh, indent=2)

    df = load_rows(spark, a, ["train", "validation", "test"]).cache()
    train = df.filter("split = 'train'")
    b3, b3_table = choose_b3(train)
    model = build_pipeline(a.frozen).fit(train)
    model_path = f"{a.root}/models/{a.run_id}/pipeline_model"
    model.write().save(model_path)

    scored = score(model, baseline_scores(df, b3), "model").cache()
    ids = ["model", "B0_majority", "B1_mom_4w", "B2_reversal", "B3_best_single"]
    long = M.long_scores(scored, ids)
    weekly = weekly_rows(scored, ids)
    test_rows = [r for r in weekly if r["split"] == "test"]
    summary_test = summarise(weekly, ids, "test")
    summary_val = summarise(weekly, ids, "validation")
    cls, with_pred = M.pooled_classification(long.filter("split = 'test'"))
    pooled_auc = BinaryClassificationEvaluator(rawPredictionCol="model", labelCol="label").evaluate(
        scored.filter("split = 'test'")
    )
    weights = M.top_weights(scored.filter("split = 'test'"), "model")
    wrows = weights.collect()

    base = a.root
    pred_path = f"{base}/predictions/{a.run_id}"
    (
        with_pred.select("ticker", "start_date", "split", "model_id", "score", "pred", "label", "ratio")
        .unionByName(
            long.filter("split != 'test'")
            .withColumn("pred", F.lit(None).cast("int"))
            .select("ticker", "start_date", "split", "model_id", "score", "pred", "label", "ratio")
        )
        .withColumn("run_id", F.lit(a.run_id))
        .write.mode("errorifexists")
        .partitionBy("split")
        .parquet(f"{pred_path}/predictions")
    )
    M.weekly_metrics(long).withColumn("run_id", F.lit(a.run_id)).write.mode("errorifexists").parquet(
        f"{base}/eval/{a.run_id}/weekly_eval"
    )
    weights.withColumn("model_id", F.lit(a.frozen)).withColumn("run_id", F.lit(a.run_id)).write.mode(
        "errorifexists"
    ).parquet(f"{pred_path}/weights")

    auc_ci = summary_test["model"]["weekly_auc"]
    diff_ci = summary_test["model"]["paired_auc_vs_B3"]
    success = bool(auc_ci["lo"] is not None and auc_ci["lo"] > 0.5 and diff_ci["lo"] is not None and diff_ci["lo"] > 0)
    out = {
        "mode": "test",
        "run_id": a.run_id,
        "application_id": spark.sparkContext.applicationId,
        "frozen_config": a.frozen,
        "finished_utc": now(),
        "seed": SEED,
        "model_path": model_path,
        "training_rows": train.count(),
        "test_rows": scored.filter("split = 'test'").count(),
        "test_weeks": len({r["start_date"] for r in test_rows}),
        "b3_choice": {"indicator": b3[0], "flipped": b3[1], "training_table": b3_table},
        "test_summary": summary_test,
        "validation_summary_same_model": summary_val,
        "test_classification_model_vs_baselines": cls,
        "test_pooled_auc_model": pooled_auc,
        "test_turnover_top10": M.turnover(wrows),
        "success_rule": {
            "rule": "(1) 95% interval of mean weekly AUC above 0.5 AND "
            "(2) 95% interval of paired difference vs B3 above 0",
            "auc_interval": [auc_ci["lo"], auc_ci["hi"]],
            "paired_vs_B3_interval": [diff_ci["lo"], diff_ci["hi"]],
            "evidence_of_signal": success,
            "finding": "evidence of predictive signal beyond simple rules"
            if success
            else "no evidence that the model adds signal beyond simple rules",
        },
        "language": "source price ratio diagnostics only; weights are a hypothetical selection output, not returns",
    }
    write_json(os.path.join(a.out, f"test_{a.run_id}.json"), out)
    write_weekly_csv(os.path.join(a.out, f"test_weekly_{a.run_id}.csv"), weekly)


# ---------------------------------------------------------------- reload
def reload(spark, a):
    model_path = f"{a.root}/models/{a.run_id}/pipeline_model"
    model = PipelineModel.load(model_path)
    df = load_rows(spark, a, ["test"])
    new = score(model, df, "score_reloaded").select("ticker", "start_date", "score_reloaded")
    stored = (
        spark.read.parquet(f"{a.root}/predictions/{a.run_id}/predictions")
        .filter("split = 'test' AND model_id = 'model'")
        .select("ticker", "start_date", "score")
    )
    j = stored.join(new, ["ticker", "start_date"], "full_outer")
    r = j.agg(
        F.count("*").alias("rows"),
        F.sum(F.col("score").isNull().cast("int")).alias("missing_stored"),
        F.sum(F.col("score_reloaded").isNull().cast("int")).alias("missing_reloaded"),
        F.max(F.abs(F.col("score") - F.col("score_reloaded"))).alias("max_abs_diff"),
    ).first()
    passed = (
        r["missing_stored"] == 0
        and r["missing_reloaded"] == 0
        and r["max_abs_diff"] is not None
        and r["max_abs_diff"] <= TOL
    )
    out = {
        "mode": "reload",
        "run_id": a.run_id,
        "application_id": spark.sparkContext.applicationId,
        "finished_utc": now(),
        "model_path": model_path,
        "rows_compared": r["rows"],
        "missing_stored": r["missing_stored"],
        "missing_reloaded": r["missing_reloaded"],
        "max_abs_diff": r["max_abs_diff"],
        "tolerance": TOL,
        "passed": passed,
        "retrained": False,
    }
    write_json(os.path.join(a.out, f"reload_{a.run_id}.json"), out)
    if not passed:
        sys.exit(1)


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=["validate", "test", "reload"])
    p.add_argument("--run-id", required=True)
    p.add_argument("--root", default="/inf2006")
    p.add_argument("--out", default=os.path.join(REPO, "evidence", "model"))
    p.add_argument("--features-path", default=None, help="parquet dir instead of Hive features.weekly (dev)")
    p.add_argument("--frozen", default=None, help="config id locked in Gate 2 (test mode)")
    p.add_argument("--decision-log", default=DECISION_LOG, help="decision log holding Gate 2 (tests override)")
    p.add_argument("--marker-dir", default=os.path.join(REPO, "evidence", "model"), help="repo copy of the test marker")
    a = p.parse_args(argv)
    if a.mode == "test" and not a.frozen:
        p.error("test mode needs --frozen <config_id>")
    if a.frozen and a.frozen not in CONFIGS:
        p.error(f"unknown config {a.frozen}; choose from {list(CONFIGS)}")
    spark = SparkSession.builder.appName(f"model_{a.mode}_{a.run_id}").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    {"validate": validate, "test": test, "reload": reload}[a.mode](spark, a)
    spark.stop()


if __name__ == "__main__":
    main()
