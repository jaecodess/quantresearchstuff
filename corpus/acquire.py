#!/usr/bin/env python3
"""Acquire raw sources once into S3 and write a manifest row per file (CORP-01, CORP-03, CORP-07).

S3 is the durable raw store: EMR clusters are terminated after every session and HDFS goes with
them, so each session copies S3 -> HDFS with corpus/stage_to_hdfs.sh. Files are never modified.

Run on the EMR primary node (it has internet access and the AWS CLI):
  python3 corpus/acquire.py prices --s3-raw s3://<bucket>/inf2006/raw
  python3 corpus/acquire.py gdelt  --s3-raw s3://<bucket>/inf2006/raw [--start 20150701 --end 20170630]

Each file is downloaded to local temp, hashed (SHA-256 and MD5), checked against the expected
value, uploaded to S3, then deleted locally. One manifest CSV per run is written to manifests/
(commit it). GDELT's own md5sums and filesizes lists are saved next to the archives as provenance.
"""

import argparse
import csv
import hashlib
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

# Price snapshot named in the brief, pinned to the exact commit and checksum we profiled.
PRICE_COMMIT = "0c447c47b757ad74edecab31f0d72f849d2e67c2"
PRICE_VERSION = "plotly-datasets-0c447c4"
PRICE_URL = f"https://raw.githubusercontent.com/plotly/datasets/{PRICE_COMMIT}/all_stocks_5yr.csv"
PRICE_BRIEF_URL = "https://raw.githubusercontent.com/plotly/datasets/master/all_stocks_5yr.csv"
PRICE_FILE = "all_stocks_5yr.csv"
PRICE_SHA256 = "6aea253cd19de60b568143991aaf1fa482456565c389205658d236e595e716cf"
PRICE_GIT_BLOB = "2a1f02e3675ad8bb6f3a8e536e15f601cfe32fbf"
PRICE_BYTES = 29580549

GDELT_BASE = "http://data.gdeltproject.org/events/"
GDELT_VERSION = "v1-daily"

MANIFEST_FIELDS = [
    "source",
    "version",
    "file_name",
    "source_url",
    "retrieved_at_utc",
    "bytes",
    "sha256",
    "md5",
    "expected_bytes",
    "expected_md5",
    "expected_sha256",
    "check",
    "s3_uri",
    "hdfs_path",
]


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def hash_file(path):
    sha, md5, n = hashlib.sha256(), hashlib.md5(), 0
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            sha.update(chunk)
            md5.update(chunk)
            n += len(chunk)
    return n, sha.hexdigest(), md5.hexdigest()


def download(url, dest, tries=3):
    for attempt in range(1, tries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "inf2006-corpus/1.0"})
            with urllib.request.urlopen(req, timeout=120) as r, open(dest, "wb") as out:
                while True:
                    chunk = r.read(1 << 20)
                    if not chunk:
                        break
                    out.write(chunk)
            return None
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return "unavailable_404"
            err = f"http_{e.code}"
        except (urllib.error.URLError, OSError) as e:
            err = f"error_{type(e).__name__}"
        time.sleep(5 * attempt)
    return err


def parse_gdelt_list(text):
    """Parse GDELT's md5sums or filesizes list: two tokens per line, value and file name in either order."""
    out = {}
    for line in text.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        a, b = parts
        name, value = (b, a) if b.endswith(".zip") else (a, b)
        if name.endswith(".zip"):
            out[name] = value
    return out


def date_range(start, end):
    d0 = datetime.strptime(start, "%Y%m%d").date()
    d1 = datetime.strptime(end, "%Y%m%d").date()
    days = []
    while d0 <= d1:
        days.append(d0)
        d0 += timedelta(days=1)
    return days


def s3_put(local, uri, dry_run):
    if dry_run:
        return
    subprocess.run(["aws", "s3", "cp", "--only-show-errors", local, uri], check=True)


def check_result(n, sha, md5, exp_bytes, exp_md5, exp_sha):
    problems = []
    if exp_bytes and str(n) != str(exp_bytes):
        problems.append("size_mismatch")
    if exp_md5 and md5 != exp_md5:
        problems.append("md5_mismatch")
    if exp_sha and sha != exp_sha:
        problems.append("sha256_mismatch")
    return ";".join(problems) or "ok"


def fetch_one(spec, tmpdir, dry_run):
    """spec: dict with source, version, file_name, url, s3_uri, hdfs_path, expected_*."""
    local = os.path.join(tmpdir, spec["file_name"])
    retrieved = utc_now()
    err = download(spec["url"], local)
    row = {
        "source": spec["source"],
        "version": spec["version"],
        "file_name": spec["file_name"],
        "source_url": spec["url"],
        "retrieved_at_utc": retrieved,
        "expected_bytes": spec.get("expected_bytes", ""),
        "expected_md5": spec.get("expected_md5", ""),
        "expected_sha256": spec.get("expected_sha256", ""),
        "s3_uri": spec["s3_uri"],
        "hdfs_path": spec["hdfs_path"],
        "bytes": "",
        "sha256": "",
        "md5": "",
    }
    if err:
        row["check"], row["s3_uri"], row["hdfs_path"] = err, "", ""
        return row
    n, sha, md5 = hash_file(local)
    row.update(bytes=n, sha256=sha, md5=md5)
    row["check"] = check_result(n, sha, md5, row["expected_bytes"], row["expected_md5"], row["expected_sha256"])
    if row["check"] == "ok":
        s3_put(local, spec["s3_uri"], dry_run)
    else:
        row["s3_uri"], row["hdfs_path"] = "", ""  # never store a file that failed its check
    os.remove(local)
    return row


def write_manifest(path, rows):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=MANIFEST_FIELDS, lineterminator="\n")
        w.writeheader()
        for r in sorted(rows, key=lambda r: r["file_name"]):
            w.writerow(r)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("source", choices=["prices", "gdelt"])
    p.add_argument("--s3-raw", required=True, help="s3://<bucket>/<prefix>/raw (mirrors /inf2006/raw in HDFS)")
    p.add_argument("--hdfs-raw", default="/inf2006/raw")
    p.add_argument("--start", default="20150701")
    p.add_argument("--end", default="20170630")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--manifest-dir", default="manifests")
    p.add_argument("--dry-run", action="store_true", help="download and hash, but do not upload to S3")
    a = p.parse_args(argv)

    acquired = datetime.now(timezone.utc).date().isoformat()
    s3_raw = a.s3_raw.rstrip("/")
    tmpdir = tempfile.mkdtemp(prefix="acquire_")
    specs = []

    if a.source == "prices":
        rel = f"prices/{PRICE_VERSION}/acquired={acquired}/{PRICE_FILE}"
        specs.append(
            {
                "source": "daily_price_csv",
                "version": f"{PRICE_VERSION} (git blob {PRICE_GIT_BLOB}; brief URL {PRICE_BRIEF_URL})",
                "file_name": PRICE_FILE,
                "url": PRICE_URL,
                "s3_uri": f"{s3_raw}/{rel}",
                "hdfs_path": f"{a.hdfs_raw}/{rel}",
                "expected_bytes": PRICE_BYTES,
                "expected_sha256": PRICE_SHA256,
            }
        )
    else:
        prefix = f"gdelt/{GDELT_VERSION}/acquired={acquired}"
        # GDELT's own checksum and size lists: provenance plus the expected values per archive.
        lists = {}
        for name in ("md5sums", "filesizes"):
            local = os.path.join(tmpdir, name)
            err = download(GDELT_BASE + name, local)
            if err:
                sys.exit(f"could not download GDELT {name}: {err}")
            with open(local) as fh:
                lists[name] = parse_gdelt_list(fh.read())
            s3_put(local, f"{s3_raw}/{prefix}/_provenance/{name}", a.dry_run)
        for d in date_range(a.start, a.end):
            fname = f"{d:%Y%m%d}.export.CSV.zip"
            specs.append(
                {
                    "source": "gdelt_events_v1",
                    "version": GDELT_VERSION,
                    "file_name": fname,
                    "url": GDELT_BASE + fname,
                    "s3_uri": f"{s3_raw}/{prefix}/{fname}",
                    "hdfs_path": f"{a.hdfs_raw}/{prefix}/{fname}",
                    "expected_bytes": lists["filesizes"].get(fname, ""),
                    "expected_md5": lists["md5sums"].get(fname, ""),
                }
            )

    rows = []
    with ThreadPoolExecutor(max_workers=a.workers) as pool:
        for i, row in enumerate(pool.map(lambda s: fetch_one(s, tmpdir, a.dry_run), specs), 1):
            rows.append(row)
            if i % 25 == 0 or i == len(specs):
                print(f"{i}/{len(specs)} files processed", flush=True)

    manifest = os.path.join(a.manifest_dir, f"{a.source}_acquired={acquired}.csv")
    write_manifest(manifest, rows)
    if not a.dry_run:
        s3_put(manifest, f"{s3_raw}/_manifests/{os.path.basename(manifest)}", False)

    ok = sum(r["check"] == "ok" for r in rows)
    total_bytes = sum(int(r["bytes"]) for r in rows if r["check"] == "ok")
    print(f"manifest: {manifest}")
    print(f"files ok: {ok}/{len(rows)}  bytes ok: {total_bytes}")
    for r in rows:
        if r["check"] != "ok":
            print(f"  NOT OK {r['file_name']}: {r['check']}")
    return 0 if ok == len(rows) else 1


if __name__ == "__main__":
    sys.exit(main())
