#!/usr/bin/env python3
"""Check that every file in the acquisition manifests is in HDFS, unchanged (CORP-06, CORP-07, CORP-11).

For each manifest row with check=ok: the HDFS file exists and its size matches. SHA-256 is recomputed
from the HDFS copy for non-ZIP files (the price CSV). ZIP archives are re-hashed in parallel by the
GDELT Streaming job and compared with the manifest in Hive (hive/10_gdelt_daily.sql, part 6), because
one `hdfs dfs -cat` per archive costs a JVM start each (about 35 minutes for 731 files).
--hash-zips hashes the archives here as well (slow); --no-hash skips hashing entirely.
Writes evidence/corpus/hdfs_verify_<run_id>.csv (one row per file) and inventory_<run_id>.json
(files, bytes and rows per source) for the raw-corpus inventory.

  python3 corpus/verify_hdfs.py --manifests manifests/*.csv --run-id <run_id> --out evidence/corpus
"""

import argparse
import csv
import hashlib
import json
import os
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone


def hdfs_sizes(paths):
    """Return {path: bytes} for files under the parent folders of `paths` (one ls per folder)."""
    sizes = {}
    for folder in sorted({os.path.dirname(p) for p in paths}):
        res = subprocess.run(["hdfs", "dfs", "-ls", folder], capture_output=True, text=True)
        for line in res.stdout.splitlines():
            parts = line.split()
            if len(parts) >= 8 and parts[0].startswith("-"):
                sizes[parts[-1]] = int(parts[4])
    return sizes


def hdfs_sha256_and_lines(path):
    proc = subprocess.Popen(["hdfs", "dfs", "-cat", path], stdout=subprocess.PIPE)
    sha, n_lines = hashlib.sha256(), 0
    for chunk in iter(lambda: proc.stdout.read(1 << 20), b""):
        sha.update(chunk)
        n_lines += chunk.count(b"\n")
    proc.wait()
    return sha.hexdigest(), n_lines


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--manifests", nargs="+", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--out", default="evidence/corpus")
    p.add_argument("--no-hash", action="store_true", help="size check only (faster)")
    p.add_argument("--hash-zips", action="store_true", help="also re-hash ZIP archives here (slow)")
    a = p.parse_args(argv)

    rows = []
    for m in a.manifests:
        with open(m) as fh:
            rows += [dict(r, manifest=os.path.basename(m)) for r in csv.DictReader(fh)]
    wanted = [r for r in rows if r["check"] == "ok"]
    not_acquired = [r for r in rows if r["check"] != "ok"]
    sizes = hdfs_sizes([r["hdfs_path"] for r in wanted])

    results, per_source = [], defaultdict(lambda: {"files": 0, "bytes": 0, "text_lines": 0, "problems": 0})
    for r in wanted:
        path = r["hdfs_path"]
        size = sizes.get(path)
        status = []
        if size is None:
            status.append("missing_in_hdfs")
        elif str(size) != r["bytes"]:
            status.append("size_mismatch")
        sha, n_lines = "", ""
        hash_this = not a.no_hash and (a.hash_zips or not path.endswith(".zip"))
        if size is not None and hash_this:
            sha, lines = hdfs_sha256_and_lines(path)
            if sha != r["sha256"]:
                status.append("sha256_mismatch")
            if not path.endswith(".zip"):
                n_lines = lines
        s = per_source[r["source"]]
        s["files"] += 1
        s["bytes"] += size or 0
        s["text_lines"] += n_lines or 0
        s["problems"] += bool(status)
        results.append(
            {
                "source": r["source"],
                "file_name": r["file_name"],
                "hdfs_path": path,
                "manifest_bytes": r["bytes"],
                "hdfs_bytes": "" if size is None else size,
                "manifest_sha256": r["sha256"],
                "hdfs_sha256": sha,
                "text_lines": n_lines,
                "status": ";".join(status) or "ok",
            }
        )

    os.makedirs(a.out, exist_ok=True)
    out_csv = os.path.join(a.out, f"hdfs_verify_{a.run_id}.csv")
    with open(out_csv, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(results[0]) if results else ["status"], lineterminator="\n")
        w.writeheader()
        w.writerows(results)
    inventory = {
        "run_id": a.run_id,
        "checked_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "hash_checked": "none" if a.no_hash else ("all" if a.hash_zips else "non-zip files (zips: Streaming job)"),
        "manifests": sorted({r["manifest"] for r in rows}),
        "per_source": per_source,
        "not_acquired": [{"file_name": r["file_name"], "check": r["check"]} for r in not_acquired],
        "files_ok": sum(x["status"] == "ok" for x in results),
        "files_checked": len(results),
    }
    with open(os.path.join(a.out, f"inventory_{a.run_id}.json"), "w") as fh:
        json.dump(inventory, fh, indent=2)
    print(json.dumps(inventory, indent=2))
    print(f"wrote {out_csv}")
    return 0 if inventory["files_ok"] == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
