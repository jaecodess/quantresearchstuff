#!/usr/bin/env python3
"""Timing and resource evidence for the PERF run sets (PERF-01). Standard library only.

  timings.py collect   --run-dir evidence/perf/<run_id> [--table evidence/perf/timings.csv]
  timings.py resources --run-dir evidence/perf/<run_id>
  timings.py summary   [--table evidence/perf/timings.csv] [--out evidence/perf/summary.csv]
  timings.py check     --run-dir evidence/perf/<run_a> --against evidence/perf/<run_b> [--tol 1e-9]

collect    merges the wrapper's task times and every task's stage CSV into one long table
           (one row per run x task x stage; rerunning collect replaces that run's rows).
resources  turns the sampler's raw YARN / HDFS snapshots into per-node peaks (nodes renamed node1, node2, ...
           so no hostnames are committed) and the YARN application report into apps.csv.
summary    median, min and max of the total and of every stage per (profile, config), plus run and failure
           counts: the tables for report section 6.
check      compares every numeric result in two runs' evidence JSON (row counts, reconciliation, test metrics)
           so a config change is shown to give the same outputs.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import statistics
import sys
from pathlib import Path

TABLE_COLS = ["run_id", "profile", "config", "rep", "status", "task", "stage", "start_utc", "end_utc", "seconds"]
TOTAL = "_total"
# Per-task stage CSVs written by the existing runners, relative to the run dir after evidence is moved there.
STAGE_FILES = {
    "task2": "pipeline/stage_times_{run}.csv",
    "task3": "model/stage_times_{run}.csv",
    "task4": "analytics/timings_task4_{run}.csv",
}
# Keys whose values legitimately differ between runs.
VOLATILE = re.compile(r"(run_id|application|_utc|time|path|started|finished|seconds|git|input|glob|dir)", re.I)


def read_meta(run_dir: Path) -> dict:
    meta = {}
    for line in (run_dir / "meta.txt").read_text().splitlines():
        key, sep, value = line.partition("=")
        if sep:
            meta[key.strip()] = value.strip()
    return meta


def read_csv(path: Path) -> list[dict]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def collect_rows(run_dir: Path) -> list[dict]:
    meta = read_meta(run_dir)
    run = meta["run_id"]
    base = {k: meta.get(k, "") for k in ("run_id", "profile", "config", "rep", "status")}
    rows = []
    for r in read_csv(run_dir / "wrapper_times.csv"):
        rows.append(
            {
                **base,
                "task": r["task"],
                "stage": r["stage"],
                "start_utc": r["start_utc"],
                "end_utc": r["end_utc"],
                "seconds": r["seconds"],
            }
        )
    for task, pattern in STAGE_FILES.items():
        path = run_dir / pattern.format(run=run)
        if path.exists():
            for r in read_csv(path):
                rows.append(
                    {
                        **base,
                        "task": task,
                        "stage": r["stage"],
                        "start_utc": r["start_utc"],
                        "end_utc": r["end_utc"],
                        "seconds": r["seconds"],
                    }
                )
    return rows


def cmd_collect(a: argparse.Namespace) -> int:
    run_dir, table = Path(a.run_dir), Path(a.table)
    new = collect_rows(run_dir)
    run = new[0]["run_id"] if new else read_meta(run_dir)["run_id"]
    old = [r for r in read_csv(table) if r["run_id"] != run] if table.exists() else []
    table.parent.mkdir(parents=True, exist_ok=True)
    with table.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=TABLE_COLS)
        w.writeheader()
        w.writerows(old + new)
    print(f"collect: {len(new)} rows for {run} -> {table}")
    return 0


# ----------------------------------------------------------------------------- resources
def split_samples(text: str) -> list[str]:
    """The sampler writes '### sample <utc>' before every snapshot."""
    return [s for s in re.split(r"^### sample .*$", text, flags=re.M) if s.strip()]


def parse_dfs_report(text: str) -> dict[str, dict[str, int]]:
    """Per datanode: DFS Used, Non DFS Used, DFS Remaining (bytes), from `hdfs dfsadmin -report`."""
    nodes: dict[str, dict[str, int]] = {}
    live = text.split("Live datanodes", 1)[1] if "Live datanodes" in text else ""
    for block in re.split(r"^Name: ", live, flags=re.M)[1:]:
        name = block.split()[0]
        vals = {}
        for key, label in (
            ("dfs_used", "DFS Used"),
            ("non_dfs_used", "Non DFS Used"),
            ("dfs_remaining", "DFS Remaining"),
        ):
            m = re.search(rf"^{label}: (\d+)", block, flags=re.M)
            if m:
                vals[key] = int(m.group(1))
        nodes[name] = vals
    return nodes


def parse_yarn_nodes(text: str) -> dict[str, dict[str, int]]:
    """Per node: allocated container memory and node physical memory in MB, from `yarn node -list -showDetails`."""
    nodes: dict[str, dict[str, int]] = {}
    current = None
    for line in text.splitlines():
        m = re.match(r"^\s*(\S+:\d+)\s+RUNNING\b", line)
        if m:
            current = m.group(1)
            nodes[current] = {}
            continue
        if current is None:
            continue
        m = re.search(r"Allocated Resources\s*:\s*<memory:(\d+)", line)
        if m:
            nodes[current]["alloc_mb"] = int(m.group(1))
        m = re.search(r"Resource Utilization by Node\s*:\s*PMem:(\d+)", line)
        if m:
            nodes[current]["pmem_mb"] = int(m.group(1))
    return nodes


def peaks(samples: list[dict[str, dict[str, int]]], fields: dict[str, str]) -> dict[str, dict[str, int]]:
    """fields maps metric -> 'max' or 'min'; returns per-node extremes over all samples."""
    out: dict[str, dict[str, int]] = {}
    for sample in samples:
        for node, vals in sample.items():
            agg = out.setdefault(node, {"samples": 0})
            agg["samples"] += 1
            for key, how in fields.items():
                if key in vals:
                    pick = max if how == "max" else min
                    agg[key] = pick(agg[key], vals[key]) if key in agg else vals[key]
    return out


def parse_yarn_apps(text: str) -> list[dict]:
    """`yarn application -status <id>` reports, one after another."""
    apps = []
    for block in re.split(r"^Application Report\s*:", text, flags=re.M)[1:]:

        def field(label: str, block: str = block) -> str:
            m = re.search(rf"^\s*{label}\s*:\s*(.*)$", block, flags=re.M)
            return m.group(1).strip() if m else ""

        alloc = field("Aggregate Resource Allocation")
        mb = re.search(r"(\d+)\s*MB-seconds", alloc)
        vc = re.search(r"(\d+)\s*vcore-seconds", alloc)
        apps.append(
            {
                "application_id": field("Application-Id"),
                "name": field("Application-Name"),
                "type": field("Application-Type"),
                "final_state": field("Final-State"),
                "mb_seconds": mb.group(1) if mb else "",
                "vcore_seconds": vc.group(1) if vc else "",
            }
        )
    return apps


def write_csv(path: Path, rows: list[dict], cols: list[str]) -> None:
    with path.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)


def cmd_resources(a: argparse.Namespace) -> int:
    run_dir = Path(a.run_dir)
    raw = run_dir / "raw"
    hdfs = peaks(
        [parse_dfs_report(s) for s in split_samples((raw / "dfs_report.txt").read_text())]
        if (raw / "dfs_report.txt").exists()
        else [],
        {"dfs_used": "max", "non_dfs_used": "max", "dfs_remaining": "min"},
    )
    yarn = peaks(
        [parse_yarn_nodes(s) for s in split_samples((raw / "yarn_nodes.txt").read_text())]
        if (raw / "yarn_nodes.txt").exists()
        else [],
        {"alloc_mb": "max", "pmem_mb": "max"},
    )
    # Datanode and NodeManager ids differ (ip:9866 vs host:8041); both sort the same way on EMR, so pair by order.
    rows = []
    for i, (h, y) in enumerate(zip(sorted(hdfs.items()) or [], sorted(yarn.items()) or []), 1):
        rows.append(
            {
                "node": f"node{i}",
                "peak_alloc_mb": y[1].get("alloc_mb", ""),
                "peak_pmem_mb": y[1].get("pmem_mb", ""),
                "peak_dfs_used_bytes": h[1].get("dfs_used", ""),
                "peak_non_dfs_used_bytes": h[1].get("non_dfs_used", ""),
                "min_dfs_remaining_bytes": h[1].get("dfs_remaining", ""),
                "samples": min(h[1]["samples"], y[1]["samples"]),
            }
        )
    cols = [
        "node",
        "peak_alloc_mb",
        "peak_pmem_mb",
        "peak_dfs_used_bytes",
        "peak_non_dfs_used_bytes",
        "min_dfs_remaining_bytes",
        "samples",
    ]
    write_csv(run_dir / "resources_peak.csv", rows, cols)
    apps = parse_yarn_apps((raw / "yarn_apps.txt").read_text()) if (raw / "yarn_apps.txt").exists() else []
    write_csv(
        run_dir / "apps.csv", apps, ["application_id", "name", "type", "final_state", "mb_seconds", "vcore_seconds"]
    )
    print(f"resources: {len(rows)} nodes, {len(apps)} applications -> {run_dir}")
    if len(hdfs) != len(yarn):
        print(f"::warning::{len(hdfs)} datanodes but {len(yarn)} YARN nodes in the snapshots", file=sys.stderr)
    return 0


# ----------------------------------------------------------------------------- summary
def stats(values: list[float]) -> dict:
    return {"n": len(values), "median": statistics.median(values), "min": min(values), "max": max(values)}


def summarise(rows: list[dict]) -> list[dict]:
    out = []
    groups: dict[tuple, list[dict]] = {}
    for r in rows:
        groups.setdefault((r["profile"], r["config"]), []).append(r)
    for (profile, config), grp in sorted(groups.items()):
        runs = sorted({r["run_id"] for r in grp})
        ok_runs = sorted({r["run_id"] for r in grp if r["status"] == "ok"})
        base = {"profile": profile, "config": config, "runs": len(runs), "failed": len(runs) - len(ok_runs)}
        ok = [r for r in grp if r["status"] == "ok"]
        totals = [sum(float(r["seconds"]) for r in ok if r["run_id"] == run and r["stage"] == TOTAL) for run in ok_runs]
        if totals:
            out.append({**base, "level": "pipeline", "task": "", "stage": "", **stats(totals)})
        keys = sorted({(r["task"], r["stage"]) for r in ok}, key=lambda k: (k[0], k[1] != TOTAL, k[1]))
        for task, stage in keys:
            vals = [float(r["seconds"]) for r in ok if r["task"] == task and r["stage"] == stage]
            level = "task" if stage == TOTAL else "stage"
            out.append({**base, "level": level, "task": task, "stage": "" if stage == TOTAL else stage, **stats(vals)})
    return out


def cmd_summary(a: argparse.Namespace) -> int:
    rows = summarise(read_csv(Path(a.table)))
    cols = ["profile", "config", "runs", "failed", "level", "task", "stage", "n", "median", "min", "max"]
    write_csv(Path(a.out), rows, cols)
    for r in rows:
        if r["level"] == "pipeline":
            print(
                f"{r['profile']}/{r['config']}: {r['n']} ok of {r['runs']} runs, median {r['median']:.0f} s "
                f"(range {r['min']:.0f}-{r['max']:.0f} s)"
            )
    print(f"summary -> {a.out}")
    return 0


# ----------------------------------------------------------------------------- check
def numeric_leaves(obj, prefix: str = "") -> dict[str, float]:
    out: dict[str, float] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            out.update(numeric_leaves(v, f"{prefix}.{k}" if prefix else str(k)))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            out.update(numeric_leaves(v, f"{prefix}[{i}]"))
    elif isinstance(obj, (int, float)) and not isinstance(obj, bool):
        if not VOLATILE.search(prefix):
            out[prefix] = float(obj)
    return out


def run_results(run_dir: Path) -> dict[str, float]:
    run = read_meta(run_dir)["run_id"]
    out = {}
    for path in sorted(run_dir.rglob(f"*{run}*.json")):
        rel = re.sub(rf"_{re.escape(run)}(?=\.)", "_<run>", path.relative_to(run_dir).as_posix())
        for k, v in numeric_leaves(json.loads(path.read_text())).items():
            out[f"{rel}:{k}"] = v
    return out


def compare(a: dict[str, float], b: dict[str, float], tol: float) -> list[str]:
    problems = [f"only in first run: {k}" for k in sorted(a.keys() - b.keys())]
    problems += [f"only in second run: {k}" for k in sorted(b.keys() - a.keys())]
    for k in sorted(a.keys() & b.keys()):
        if abs(a[k] - b[k]) > tol:
            problems.append(f"{k}: {a[k]} vs {b[k]}")
    return problems


def cmd_check(a: argparse.Namespace) -> int:
    first, second = run_results(Path(a.run_dir)), run_results(Path(a.against))
    problems = compare(first, second, a.tol)
    for p in problems:
        print(f"DIFF {p}")
    print(f"check: {len(first.keys() & second.keys())} values compared, {len(problems)} differences (tol {a.tol})")
    return 1 if problems else 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("collect")
    s.add_argument("--run-dir", required=True)
    s.add_argument("--table", default="evidence/perf/timings.csv")
    s.set_defaults(fn=cmd_collect)
    s = sub.add_parser("resources")
    s.add_argument("--run-dir", required=True)
    s.set_defaults(fn=cmd_resources)
    s = sub.add_parser("summary")
    s.add_argument("--table", default="evidence/perf/timings.csv")
    s.add_argument("--out", default="evidence/perf/summary.csv")
    s.set_defaults(fn=cmd_summary)
    s = sub.add_parser("check")
    s.add_argument("--run-dir", required=True)
    s.add_argument("--against", required=True)
    s.add_argument("--tol", type=float, default=1e-9)
    s.set_defaults(fn=cmd_check)
    a = p.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
