#!/usr/bin/env python3
"""Summarise the Section 6 timing runs: median, min and max seconds per configuration and task.

  python3 perf/summarise_perf.py evidence/perf/timings_<base>.csv evidence/perf/summary_<base>

Writes <out>.csv and <out>.json and prints a table. Failed runs (exit != 0) are counted but left out of the
statistics. Speedup = median of the default configuration / median of the alternative (above 1 = faster).
"""

import csv
import json
import statistics
import sys


def summarise(rows):
    groups = {}
    for r in rows:
        groups.setdefault((r["task"], r["config"]), []).append(r)
    out = []
    for (task, cfg), rs in sorted(groups.items()):
        ok = [int(r["seconds"]) for r in rs if str(r["exit"]) == "0"]
        out.append(
            {
                "task": task,
                "config": cfg,
                "runs_ok": len(ok),
                "runs_failed": len(rs) - len(ok),
                "median_s": statistics.median(ok) if ok else None,
                "min_s": min(ok) if ok else None,
                "max_s": max(ok) if ok else None,
            }
        )
    by = {(o["task"], o["config"]): o for o in out}
    for o in out:
        ref = by.get((o["task"], "default"))
        if o["config"] != "default" and ref and ref["median_s"] and o["median_s"]:
            o["speedup_vs_default"] = round(ref["median_s"] / o["median_s"], 3)
    return out


def fmt(s):
    return "-" if s is None else f"{int(s) // 60} min {int(s) % 60:02d} s"


def main(argv=None):
    src, out = (argv or sys.argv[1:])[:2]
    with open(src, newline="") as fh:
        rows = list(csv.DictReader(fh))
    summary = summarise(rows)
    cols = ["task", "config", "runs_ok", "runs_failed", "median_s", "min_s", "max_s", "speedup_vs_default"]
    with open(out + ".csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols, lineterminator="\n")
        w.writeheader()
        for s in summary:
            w.writerow({k: s.get(k, "") for k in cols})
    with open(out + ".json", "w") as fh:
        json.dump({"source": src, "summary": summary}, fh, indent=2)
    failed = sum(s["runs_failed"] for s in summary)
    print(f"\nRESULT: {len(rows) - failed} of {len(rows)} timed runs succeeded")
    print(f"{'task':<20} {'config':<14} {'runs':>4} {'median':>12} {'min':>12} {'max':>12} {'speedup':>8}")
    for s in summary:
        sp = s.get("speedup_vs_default", "")
        print(
            f"{s['task']:<20} {s['config']:<14} {s['runs_ok']:>4} {fmt(s['median_s']):>12} "
            f"{fmt(s['min_s']):>12} {fmt(s['max_s']):>12} {sp:>8}"
        )
    return summary


if __name__ == "__main__":
    main()
