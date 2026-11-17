"""Helpers for the fresh-session rerun notebook (perf/inf2006_pipeline.ipynb, Appendix A).

The notebook stays thin: each section calls the owned scripts through Rerun.sh and records verification
checks through Rerun.check. Each check row = check, steps/command, expected, actual, evidence location,
pass/fail (the brief's verification record). Full command output goes to evidence/rerun/live_<id>.log;
the notebook keeps only the tail, so the exported .ipynb stays small.

DRY_RUN=1 prints the commands instead of running them (used by tests/perf; never evidence).
"""

import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone


def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def repo_root():
    out = subprocess.run(["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True)
    return out.stdout.strip() if out.returncode == 0 else os.getcwd()


class Rerun:
    def __init__(self, rerun_id, out_dir="evidence/rerun", dry=False, tail=30):
        self.id = rerun_id
        self.out = out_dir
        self.dry = dry
        self.tail = tail
        self.checks = []
        self.timings = []
        self.started = now()
        os.makedirs(out_dir, exist_ok=True)
        self.live = os.path.join(out_dir, f"live_{rerun_id}.log")

    def sh(self, cmd, stage):
        """Run one shell command; stream it to the live log, show its tail, stop the notebook on failure."""
        t0, s0 = now(), time.time()
        print(f"[{t0}] {stage}: {cmd}")
        if self.dry:
            self.timings.append({"stage": stage, "start_utc": t0, "end_utc": t0, "seconds": 0, "exit": "dry"})
            return ""
        lines = []
        with open(self.live, "a") as log:
            log.write(f"\n===== {stage} {t0}\n$ {cmd}\n")
            p = subprocess.Popen(["bash", "-c", cmd], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
            for line in p.stdout:
                log.write(line)
                log.flush()
                lines.append(line)
            code = p.wait()
        secs = round(time.time() - s0)
        self.timings.append({"stage": stage, "start_utc": t0, "end_utc": now(), "seconds": secs, "exit": code})
        print("".join(lines[-self.tail :]), end="")
        print(f"--- {stage}: exit {code}, {secs // 60} min {secs % 60} s (full output: {self.live})")
        if code != 0:
            self.save()
            raise RuntimeError(f"{stage} failed with exit code {code}; see {self.live}")
        return "".join(lines)

    def load(self, path):
        if self.dry or not os.path.exists(path):
            if not self.dry:
                print(f"MISSING: {path}")
            return None
        with open(path) as fh:
            return json.load(fh)

    def check(self, name, command, expected, actual, evidence, tol=None):
        if self.dry:
            status = "dry"
        elif actual is None:
            status = "FAIL"
        elif tol is not None:
            status = "pass" if abs(float(actual) - float(expected)) <= tol else "FAIL"
        else:
            status = "pass" if actual == expected else "FAIL"
        row = {
            "check": name,
            "command": command,
            "expected": expected,
            "actual": actual,
            "evidence": evidence,
            "result": status,
        }
        self.checks.append(row)
        print(f"{status.upper():<5} {name}: expected {expected}, actual {actual}")
        return status == "pass"

    def save(self):
        passed = sum(c["result"] == "pass" for c in self.checks)
        summary = {
            "rerun_id": self.id,
            "started_utc": self.started,
            "saved_utc": now(),
            "dry_run": self.dry,
            "checks_passed": passed,
            "checks_total": len(self.checks),
            "python": sys.version.split()[0],
            "timings": self.timings,
            "checks": self.checks,
        }
        with open(os.path.join(self.out, f"rerun_{self.id}.json"), "w") as fh:
            json.dump(summary, fh, indent=2, default=str)
        for name, rows in (("checks", self.checks), ("timings", self.timings)):
            if rows:
                with open(os.path.join(self.out, f"{name}_{self.id}.csv"), "w", newline="") as fh:
                    w = csv.DictWriter(fh, fieldnames=list(rows[0]), lineterminator="\n")
                    w.writeheader()
                    w.writerows(rows)
        return summary

    def report(self):
        s = self.save()
        print(f"\n{s['checks_passed']} of {s['checks_total']} checks passed (rerun {self.id})\n")
        for c in self.checks:
            print(f"{c['result'].upper():<5} | {c['check']} | expected {c['expected']} | actual {c['actual']}")
        print("\nStage times:")
        for t in self.timings:
            print(f"  {t['stage']:<28} {t['seconds'] // 60:>3} min {t['seconds'] % 60:>2} s")
        return s


# ---------- lookups into evidence JSON (return None when a value is absent) ----------


def get(d, *keys):
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


def problems_total(inventory):
    if not inventory:
        return None
    return sum(s["problems"] for s in inventory["per_source"].values())


def max_auc_diff(ref, new):
    """Largest difference in mean weekly validation AUC between two validation runs, over the reference configs."""
    if not ref or not new:
        return None
    a, b = ref["validation_summary"], new["validation_summary"]
    return max(abs(b[k]["weekly_auc"]["mean"] - a[k]["weekly_auc"]["mean"]) for k in a)


def q2_paired(task4):
    for r in (task4 or {}).get("q2", []):
        if r["split"] == "test" and r["group"] == "all":
            return r["paired_model_minus_b3"]["mean"]
    return None


def q1_paired(task4):
    for r in (task4 or {}).get("q1", []):
        if r["split"] == "test" and r["quintile"] == "Q5-Q1 share_above":
            return r["paired_mean"]
    return None


def q3a_dispersion(task4):
    for r in (task4 or {}).get("q3a", []):
        if r["timing"] == "pre_start" and r["tercile"] == "high-low":
            return r["dispersion_mean"]
    return None
