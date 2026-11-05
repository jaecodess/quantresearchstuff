#!/usr/bin/env python3
"""Hadoop Streaming mapper: one GDELT 1.0 daily archive in, one audit/aggregate row out (PIPE-03).

Input: a manifest with one archive path per line, read with NLineInputFormat (1 line per mapper),
so each mapper handles exactly one archive. ZIPs are not splittable and are never expanded in HDFS
(decision D-010): the mapper copies the ZIP to local temp, reads it in-stream, and keeps only aggregates.
NLineInputFormat passes "offset<TAB>path"; plain "path" lines (local smoke test) also work.

Path forms: hdfs://... or s3://... (copied with `hadoop fs -copyToLocal`), or a local file.
A local file may be a .zip or an uncompressed .CSV (the CI fixture, since ZIPs cannot be committed).

Event layout: GDELT 1.0 daily export, 58 tab-separated fields, no header (verified for CORP-05).
Events are assigned to the archive date (= DATEADDED), not SQLDATE (HANDOVER section 5).

Output (tab-separated, key first): see OUT_FIELDS. Sums only, so the reducer can combine safely.
Counters (reporter:counter:gdelt,*) appear in the job history as execution evidence.
"""

import hashlib
import io
import os
import re
import shutil
import subprocess
import sys
import tempfile
import zipfile
from datetime import date

N_FIELDS = 58
# 0-based positions in the GDELT 1.0 event record
F_ID, F_SQLDATE = 0, 1
F_QUAD, F_GOLD, F_MENTIONS, F_SOURCES, F_ARTICLES, F_TONE = 29, 30, 31, 32, 33, 34
F_ACTIONGEO_CC = 51
F_DATEADDED = 56

OUT_FIELDS = [
    "archive_date",  # key, yyyy-mm-dd from the archive file name
    "n_archives",
    "n_corrupt_archive",
    "n_lines",
    "n_malformed",  # wrong field count
    "n_bad_value",  # unparseable id, date or numeric field
    "n_dup",  # duplicate GLOBALEVENTID within the archive
    "n_events",  # valid, de-duplicated events
    "n_us",  # ActionGeo_CountryCode = 'US' (FIPS)
    "n_quad1",
    "n_quad2",
    "n_quad3",
    "n_quad4",
    "n_conflict",  # QuadClass 3 or 4
    "sum_mentions",
    "sum_articles",
    "sum_tone_x_articles",  # for the article-weighted mean tone
    "sum_goldstein",
    "n_dateadded_mismatch",  # DATEADDED differs from the archive date
    "n_lag_0",  # DATEADDED - SQLDATE in days
    "n_lag_1_7",
    "n_lag_8_30",
    "n_lag_31_plus",
    "n_lag_negative",
    "min_event_id",
    "max_event_id",
    "source_file",
    "sha256",  # of the archive bytes as read from HDFS; compared with the acquisition manifest in Hive
]

DATE_RE = re.compile(r"(\d{8})")


def counter(name, n=1):
    sys.stderr.write(f"reporter:counter:gdelt,{name},{n}\n")


def parse_yyyymmdd(s):
    if len(s) != 8 or not s.isdigit():
        return None
    try:
        return date(int(s[:4]), int(s[4:6]), int(s[6:]))
    except ValueError:
        return None


def fetch(path, tmpdir):
    """Return a local path for the archive. HDFS/S3 paths (with a scheme, or a plain /path that is not a
    local file, which is how `hdfs dfs -ls -C` prints them) are copied to local temp first."""
    if "://" in path or not os.path.exists(path):
        local = os.path.join(tmpdir, os.path.basename(path))
        subprocess.run(["hadoop", "fs", "-copyToLocal", path, local], check=True)
        return local
    return path


def sha256_of(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def open_lines(path):
    """Yield decoded lines of the local archive at `path` (.zip, or plain text for the test fixture)."""
    if path.lower().endswith(".zip"):
        with zipfile.ZipFile(path) as zf:
            for member in zf.namelist():
                with zf.open(member) as raw:
                    for line in io.TextIOWrapper(raw, encoding="utf-8", errors="replace"):
                        yield line
    else:
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                yield line


def empty_row():
    row = {k: 0 for k in OUT_FIELDS}
    row["min_event_id"], row["max_event_id"], row["sha256"] = "", "", ""
    return row


def process(path, tmpdir):
    name = os.path.basename(path)
    m = DATE_RE.search(name)
    archive_day = parse_yyyymmdd(m.group(1)) if m else None
    row = empty_row()
    row["archive_date"] = archive_day.isoformat() if archive_day else "_unknown"
    row["source_file"] = name
    row["n_archives"] = 1
    seen = set()
    min_id, max_id = None, None
    try:
        local = fetch(path, tmpdir)
        row["sha256"] = sha256_of(local)
        for line in open_lines(local):
            line = line.rstrip("\r\n")
            if not line:
                continue
            row["n_lines"] += 1
            f = line.split("\t")
            if len(f) != N_FIELDS:
                row["n_malformed"] += 1
                continue
            try:
                eid = int(f[F_ID])
                sqldate = parse_yyyymmdd(f[F_SQLDATE])
                added = parse_yyyymmdd(f[F_DATEADDED])
                if sqldate is None or added is None:
                    raise ValueError("bad date")
                quad = int(f[F_QUAD])
                gold = float(f[F_GOLD]) if f[F_GOLD] else 0.0
                mentions = int(f[F_MENTIONS])
                articles = int(f[F_ARTICLES])
                tone = float(f[F_TONE])
                if quad not in (1, 2, 3, 4) or not -10.0 <= gold <= 10.0:
                    raise ValueError("out of range")
            except ValueError:
                row["n_bad_value"] += 1
                continue
            if eid in seen:
                row["n_dup"] += 1
                continue
            seen.add(eid)
            min_id = eid if min_id is None else min(min_id, eid)
            max_id = eid if max_id is None else max(max_id, eid)
            row["n_events"] += 1
            row[f"n_quad{quad}"] += 1
            if quad in (3, 4):
                row["n_conflict"] += 1
            if f[F_ACTIONGEO_CC] == "US":
                row["n_us"] += 1
            row["sum_mentions"] += mentions
            row["sum_articles"] += articles
            row["sum_tone_x_articles"] += tone * articles
            row["sum_goldstein"] += gold
            if archive_day is not None and added != archive_day:
                row["n_dateadded_mismatch"] += 1
            lag = (added - sqldate).days
            if lag < 0:
                row["n_lag_negative"] += 1
            elif lag == 0:
                row["n_lag_0"] += 1
            elif lag <= 7:
                row["n_lag_1_7"] += 1
            elif lag <= 30:
                row["n_lag_8_30"] += 1
            else:
                row["n_lag_31_plus"] += 1
    except (OSError, zipfile.BadZipFile, subprocess.CalledProcessError) as exc:
        sys.stderr.write(f"corrupt or unreadable archive {name}: {exc}\n")
        row["n_corrupt_archive"] = 1
        counter("corrupt_archive")
    row["min_event_id"] = "" if min_id is None else min_id
    row["max_event_id"] = "" if max_id is None else max_id
    for k in ("n_lines", "n_malformed", "n_bad_value", "n_dup", "n_events"):
        counter(k, row[k])
    counter("archives")
    return row


def fmt(v):
    if isinstance(v, float):
        return repr(round(v, 6))
    return str(v)


def main():
    tmpdir = tempfile.mkdtemp(prefix="gdelt_")
    try:
        for line in sys.stdin:
            line = line.rstrip("\r\n")
            if not line.strip():
                continue
            path = line.split("\t")[-1].strip()  # drop the NLineInputFormat offset if present
            row = process(path, tmpdir)
            print("\t".join(fmt(row[k]) for k in OUT_FIELDS))
            for leftover in os.listdir(tmpdir):  # free local disk before the next archive
                os.remove(os.path.join(tmpdir, leftover))
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


if __name__ == "__main__":
    main()
