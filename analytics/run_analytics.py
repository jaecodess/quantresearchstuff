#!/usr/bin/env python3
"""Task 4 analytics in PySpark (ANLY-01 to ANLY-04, HANDOVER sections 8.5, 8.9 and 10).

  spark-submit analytics/run_analytics.py --run-id <task4_run_id> [--out evidence/analytics]

Reads Hive tables registered by analytics/run_task4.sh:
  model.predictions     frozen-model and baseline scores (Task 3, all splits; `model` and `B3_best_single`)
  features.weekly       for the within-week volatility rank (vol_20d_rank)
  features.weekly_news  news_abn (pre-start) per week; optional (Q3 is skipped if the table is missing)
  analytics.q1_weekly   Hive output of analytics/20_q1_volume_quintiles.sql
  analytics.q3a_weekly  Hive output of analytics/30_q3a_news_market.sql (optional)

Tool choice: subgroup model evaluation needs per-week, per-subgroup tie-aware AUC (window functions with ranks) and
reuses the Task 3 evaluation module, so it runs in PySpark. All row-level work stays distributed; only week-level
tables (a few hundred rows) reach the driver for the week bootstrap, and only those are written and plotted.

Q2  reliability by subgroup: weekly AUC of the frozen model and of B3, and the paired weekly difference, by
    quarter, within-week volatility tercile (vol_20d_rank) and market-dispersion tercile (sd of ln ratio that week,
    terciles within each year). 2016 validation and 2017 test are reported separately; a pattern counts only if it
    points the same way in both.
Q3b conditional evaluation: out-of-sample weeks with GDELT coverage (2016 and Jan-Jun 2017) grouped into news_abn
    terciles; model and B3 weekly AUC, paired difference and rank IC per tercile, pooled and per year.
Q1, Q3a summaries: week bootstrap intervals for the Hive weekly tables.

Uses the stored predictions of the frozen model only: nothing is refitted, retuned or reselected.
"""

import argparse
import csv
import json
import math
import os
import random
import subprocess
import sys
from datetime import datetime, timezone

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from evaluation import metrics as M  # noqa: E402

MODEL, B3 = "model", "B3_best_single"
OOS = ("validation", "test")
SEED = M.SEED
N_BOOT = M.N_BOOT


# ---------- pure-Python helpers (driver side, week-level tables only) ----------


def ci(values):
    b = M.bootstrap_mean(values)
    return {k: b.get(k) for k in ("n_weeks", "mean", "lo", "hi")}


def diff_ci(a, b, n_boot=N_BOOT, seed=SEED):
    """Unpaired bootstrap of mean(a) - mean(b), resampling weeks within each group."""
    a = [v for v in a if v is not None and not math.isnan(v)]
    b = [v for v in b if v is not None and not math.isnan(v)]
    if not a or not b:
        return {"mean": None, "lo": None, "hi": None}
    rnd = random.Random(seed)
    d = sorted(sum(rnd.choice(a) for _ in a) / len(a) - sum(rnd.choice(b) for _ in b) / len(b) for _ in range(n_boot))
    return {"mean": sum(a) / len(a) - sum(b) / len(b), "lo": d[int(0.025 * n_boot)], "hi": d[int(0.975 * n_boot) - 1]}


def _ranks(xs):
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    r = [0.0] * len(xs)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        for k in range(i, j + 1):
            r[order[k]] = (i + j) / 2 + 1
        i = j + 1
    return r


def spearman(x, y):
    pairs = [(a, b) for a, b in zip(x, y) if a is not None and b is not None]
    if len(pairs) < 3:
        return None
    rx, ry = _ranks([p[0] for p in pairs]), _ranks([p[1] for p in pairs])
    mx, my = sum(rx) / len(rx), sum(ry) / len(ry)
    sxy = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    sxx = sum((a - mx) ** 2 for a in rx)
    syy = sum((b - my) ** 2 for b in ry)
    return sxy / math.sqrt(sxx * syy) if sxx and syy else None


def tercile_of(values_by_key):
    """Equal-count terciles 1..3 by value (ties broken by key), like Hive NTILE(3)."""
    keys = sorted(values_by_key, key=lambda k: (values_by_key[k], k))
    n = len(keys)
    return {k: i * 3 // n + 1 for i, k in enumerate(keys)}


def quarter(d):
    return f"Q{(d.month - 1) // 3 + 1}"


def group_summary(weekly, weeks, group_key=None):
    """Model and B3 mean weekly AUC, paired difference, model rank IC on the given weeks.

    weekly: rows (model_id, start_date, split, n, auc, rank_ic); group_key filters model ids like 'model|high'.
    """
    m_id = MODEL if group_key is None else f"{MODEL}|{group_key}"
    b_id = B3 if group_key is None else f"{B3}|{group_key}"
    rows = [r for r in weekly if r["start_date"] in weeks and r["model_id"] in (m_id, b_id)]
    m = [r for r in rows if r["model_id"] == m_id]
    paired = M.paired_bootstrap(rows, m_id, b_id)
    return {
        "n_weeks": len({r["start_date"] for r in m}),
        "n_rows": sum(r["n"] for r in m),
        "model_auc": ci([r["auc"] for r in m]),
        "b3_auc": ci([r["auc"] for r in rows if r["model_id"] == b_id]),
        "paired_model_minus_b3": {k: paired.get(k) for k in ("n_weeks", "mean", "lo", "hi")},
        "model_rank_ic": ci([r["rank_ic"] for r in m]),
    }


def flat(prefix, d):
    out = {}
    for k, v in d.items():
        key = f"{prefix}{k}"
        if isinstance(v, dict):
            out.update(flat(key + "_", v))
        else:
            out[key] = v
    return out


def write_csv(path, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    cols = []
    for r in rows:
        cols += [c for c in r if c not in cols]
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, lineterminator="\n")
        w.writeheader()
        for r in rows:
            w.writerow({k: (round(v, 6) if isinstance(v, float) else v) for k, v in r.items()})


# ---------- analyses ----------


def q1(spark):
    rows = [r.asDict() for r in spark.table("analytics.q1_weekly").collect()]
    summary = []
    for split in sorted({r["split"] for r in rows}):
        rs = [r for r in rows if r["split"] == split]
        for q in range(1, 6):
            g = [r for r in rs if r["quintile"] == q]
            summary.append(
                {
                    "split": split,
                    "quintile": q,
                    "n_weeks": len(g),
                    "n_rows": sum(r["n"] for r in g),
                    **flat("share_above_", ci([r["share_above"] for r in g])),
                    **flat("mean_excess_ln_", ci([r["mean_excess"] for r in g])),
                }
            )
        keyed = [dict(r, model_id=f"Q{r['quintile']}") for r in rs]
        for metric in ("share_above", "mean_excess"):
            p = M.paired_bootstrap(keyed, "Q5", "Q1", metric=metric)
            summary.append(
                {
                    "split": split,
                    "quintile": f"Q5-Q1 {metric}",
                    "n_weeks": p["n_weeks"],
                    **flat("paired_", {k: p.get(k) for k in ("mean", "lo", "hi")}),
                }
            )
    return summary, rows


def q2_weekly(spark):
    """Week-level metrics for the model and B3: whole week, and within each volatility tercile."""
    pred = spark.table("model.predictions").filter(F.col("split").isin(*OOS) & F.col("model_id").isin(MODEL, B3))
    feats = spark.table("features.weekly").filter("eligible").select("ticker", "start_date", "vol_20d_rank")
    p = pred.select("ticker", "start_date", "split", "model_id", "score", "label", "ratio").join(
        feats, ["ticker", "start_date"], "left"
    )
    terc = (
        F.when(F.col("vol_20d_rank") < 1 / 3, "vol_low")
        .when(F.col("vol_20d_rank") < 2 / 3, "vol_mid")
        .when(F.col("vol_20d_rank").isNotNull(), "vol_high")
    )
    by_vol = p.withColumn("t", terc).filter("t IS NOT NULL").withColumn("model_id", F.concat_ws("|", "model_id", "t"))
    weekly = M.weekly_metrics(p).unionByName(M.weekly_metrics(by_vol.drop("t", "vol_20d_rank")))
    disp = (
        pred.filter(F.col("model_id") == MODEL)
        .groupBy("split", "start_date")
        .agg(F.stddev_samp(F.log("ratio")).alias("dispersion"), F.count("*").alias("n"))
    )
    return [r.asDict() for r in weekly.collect()], [r.asDict() for r in disp.collect()]


def q2(weekly, disp):
    summary = []
    week_split = {r["start_date"]: r["split"] for r in disp}
    disp_terc = {}
    for split in OOS:
        disp_terc.update(tercile_of({r["start_date"]: r["dispersion"] for r in disp if r["split"] == split}))
    for split in OOS:
        weeks = {w for w, s in week_split.items() if s == split}
        groups = [("all", "all", weeks, None)]
        groups += [("quarter", q, {w for w in weeks if quarter(w) == q}, None) for q in ("Q1", "Q2", "Q3", "Q4")]
        groups += [("vol_tercile", t, weeks, t) for t in ("vol_low", "vol_mid", "vol_high")]
        groups += [
            ("dispersion_tercile", f"disp_{n}", {w for w in weeks if disp_terc.get(w) == i}, None)
            for i, n in ((1, "low"), (2, "mid"), (3, "high"))
        ]
        for dim, name, wk, key in groups:
            s = group_summary(weekly, wk, key)
            if dim == "dispersion_tercile" and wk:
                ds = [r["dispersion"] for r in disp if r["start_date"] in wk]
                s["dispersion_range"] = [min(ds), max(ds)]
            summary.append({"split": split, "dimension": dim, "group": name, **s})
    # Consistency: same sign of the paired difference in 2016 and 2017 for each group.
    by = {(r["dimension"], r["group"], r["split"]): r for r in summary}
    for r in summary:
        v = by.get((r["dimension"], r["group"], "validation"))
        t = by.get((r["dimension"], r["group"], "test"))
        mv = v and v["paired_model_minus_b3"]["mean"]
        mt = t and t["paired_model_minus_b3"]["mean"]
        r["same_sign_2016_2017"] = None if mv is None or mt is None else (mv > 0) == (mt > 0)
    return summary, disp_terc


def q3b(weekly, news):
    """news: {start_date: news_abn}. Terciles over out-of-sample weeks that have a pre-start measure."""
    whole = [r for r in weekly if r["model_id"] in (MODEL, B3) and r["split"] in OOS]
    split_of = {r["start_date"]: r["split"] for r in whole}
    covered = {w: news[w] for w in split_of if news.get(w) is not None}
    if not covered:
        return [], {}
    terc = tercile_of(covered)
    summary = []
    for i, name in ((1, "news_low"), (2, "news_mid"), (3, "news_high")):
        wk_all = {w for w, t in terc.items() if t == i}
        for scope, wk in (
            ("2016+2017", wk_all),
            ("2016 validation", {w for w in wk_all if split_of[w] == "validation"}),
            ("2017 test (Jan-Jun)", {w for w in wk_all if split_of[w] == "test"}),
        ):
            s = group_summary(whole, wk)
            vals = [covered[w] for w in wk]
            s["news_abn_range"] = [min(vals), max(vals)] if vals else None
            summary.append({"tercile": name, "scope": scope, **s})
    return summary, terc


def q3a(spark):
    rows = [r.asDict() for r in spark.table("analytics.q3a_weekly").collect()]
    summary = []
    measures = ("dispersion", "abs_market_ln", "mean_vol_20d")
    timings = (("pre_start", "tercile_pre", "news_abn"), ("same_week", "tercile_same", "news_abn_same_week"))
    for timing, tcol, ncol in timings:
        rs = [r for r in rows if r[tcol] is not None]
        for t in (1, 2, 3):
            g = [r for r in rs if r[tcol] == t]
            out = {"timing": timing, "tercile": t, "n_weeks": len(g)}
            out.update(flat("news_abn_", ci([r[ncol] for r in g])))
            for m in measures:
                out.update(flat(f"{m}_", ci([r[m] for r in g])))
            summary.append(out)
        hi = [r for r in rs if r[tcol] == 3]
        lo = [r for r in rs if r[tcol] == 1]
        out = {"timing": timing, "tercile": "high-low", "n_weeks": len(hi) + len(lo)}
        for m in measures:
            out.update(flat(f"{m}_", diff_ci([r[m] for r in hi], [r[m] for r in lo])))
            out[f"{m}_spearman_all_weeks"] = spearman([r[ncol] for r in rs], [r[m] for r in rs])
        summary.append(out)
    return summary, rows


def table_exists(spark, name):
    db, tbl = name.split(".")
    return spark.catalog.tableExists(tbl, db)


def git_rev():
    try:
        return subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], cwd=REPO, text=True).strip()
    except Exception:  # noqa: BLE001
        return None


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--run-id", required=True)
    p.add_argument("--out", default=os.path.join(REPO, "evidence", "analytics"))
    a = p.parse_args(argv)
    spark = SparkSession.builder.appName(f"analytics_{a.run_id}").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    ev = lambda name: os.path.join(a.out, f"{name}_{a.run_id}.csv")  # noqa: E731

    out = {
        "run_id": a.run_id,
        "application_id": spark.sparkContext.applicationId,
        "git": git_rev(),
        "started_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "seed": SEED,
        "n_boot": N_BOOT,
        "note": "Signal diagnostics on stored frozen-model predictions; association only, not returns.",
    }

    s1, w1 = q1(spark)
    write_csv(ev("q1_summary"), s1)
    write_csv(ev("q1_weekly"), w1)
    out["q1"] = s1

    weekly, disp = q2_weekly(spark)
    s2, disp_terc = q2(weekly, disp)
    write_csv(ev("q2_summary"), [flat("", r) for r in s2])
    wk_rows = sorted(weekly, key=lambda r: (r["split"], r["model_id"], r["start_date"]))
    for r in wk_rows:
        r["quarter"] = quarter(r["start_date"])
        r["dispersion_tercile"] = disp_terc.get(r["start_date"])
    write_csv(ev("q2_weekly"), wk_rows)
    write_csv(ev("q2_dispersion_weekly"), sorted(disp, key=lambda r: r["start_date"]))
    out["q2"] = s2

    if table_exists(spark, "features.weekly_news") and table_exists(spark, "analytics.q3a_weekly"):
        news = {r["start_date"]: r["news_abn"] for r in spark.table("features.weekly_news").collect()}
        s3b, terc = q3b(weekly, news)
        write_csv(ev("q3b_summary"), [flat("", r) for r in s3b])
        out["q3b"] = s3b
        out["q3b_weeks_by_tercile"] = {str(i): sum(1 for t in terc.values() if t == i) for i in (1, 2, 3)}
        s3a, w3a = q3a(spark)
        write_csv(ev("q3a_summary"), s3a)
        write_csv(ev("q3a_weekly"), sorted(w3a, key=lambda r: r["start_date"]))
        out["q3a"] = s3a
    else:
        out["q3"] = "skipped: features.weekly_news or analytics.q3a_weekly not registered (no GDELT features)"
        print("Q3 SKIPPED: register features.weekly_news first (see docs/analytics/aws_task4.md)")

    out["finished_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    path = os.path.join(a.out, f"task4_{a.run_id}.json")
    with open(path, "w") as fh:
        json.dump(out, fh, indent=2, default=str)
    print(f"wrote {path}")
    for r in s2:
        if r["dimension"] in ("all", "dispersion_tercile", "vol_tercile"):
            pm = r["paired_model_minus_b3"]
            print(f"Q2 {r['split']:<10} {r['group']:<9} weeks={r['n_weeks']:>2}", end=" ")
            print(f"paired model-B3 = {pm['mean']} [{pm['lo']}, {pm['hi']}]")
    spark.stop()


if __name__ == "__main__":
    main()
