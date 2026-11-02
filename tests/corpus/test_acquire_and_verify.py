"""Unit tests for the corpus acquisition and HDFS verification helpers (no network, no Hadoop)."""

import csv
import hashlib
import importlib.util
import json
import os
import stat
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / "corpus" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


acquire = load("acquire")
verify = load("verify_hdfs")


def test_gdelt_window_is_731_days():
    days = acquire.date_range("20150701", "20170630")
    assert len(days) == 731
    assert days[0].isoformat() == "2015-07-01" and days[-1].isoformat() == "2017-06-30"


def test_parse_gdelt_lists_either_column_order():
    md5 = acquire.parse_gdelt_list("abc123  20150701.export.CSV.zip\nnoise\n")
    sizes = acquire.parse_gdelt_list("20150702.export.CSV.zip 12345\n")
    assert md5 == {"20150701.export.CSV.zip": "abc123"}
    assert sizes == {"20150702.export.CSV.zip": "12345"}


def test_check_result_flags_each_mismatch():
    assert acquire.check_result(10, "s", "m", "10", "m", "s") == "ok"
    assert acquire.check_result(10, "s", "m", "", "", "") == "ok"
    assert acquire.check_result(9, "x", "y", "10", "m", "s") == "size_mismatch;md5_mismatch;sha256_mismatch"


def test_hash_file(tmp_path):
    f = tmp_path / "a.csv"
    f.write_bytes(b"hello\n")
    n, sha, md5 = acquire.hash_file(str(f))
    assert n == 6
    assert sha == hashlib.sha256(b"hello\n").hexdigest()
    assert md5 == hashlib.md5(b"hello\n").hexdigest()


def test_verify_hdfs_with_fake_hdfs(tmp_path, monkeypatch):
    # A fake `hdfs` command that serves HDFS paths from a local folder.
    store = tmp_path / "store"
    good, bad = b"a,b\n1,2\n", b"changed content\n"
    (store / "raw/prices").mkdir(parents=True)
    (store / "raw/prices/p.csv").write_bytes(good)
    (store / "raw/prices/q.csv").write_bytes(bad)
    fake = tmp_path / "bin" / "hdfs"
    fake.parent.mkdir()
    fake.write_text(
        "#!/usr/bin/env python3\n"
        "import os, sys\n"
        f"S = {str(store)!r}\n"
        "cmd, path = sys.argv[2], sys.argv[3]\n"
        "local = S + path\n"
        "if cmd == '-ls':\n"
        "    for n in sorted(os.listdir(local)):\n"
        "        p = os.path.join(local, n)\n"
        "        print('-rw-r--r--   2 hadoop hadoop', os.path.getsize(p), '2026-11-02 10:00', path + '/' + n)\n"
        "elif cmd == '-cat':\n"
        "    sys.stdout.buffer.write(open(local, 'rb').read())\n"
    )
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("PATH", f"{fake.parent}{os.pathsep}{os.environ['PATH']}")

    manifest = tmp_path / "prices_acquired=2026-11-02.csv"
    rows = [
        ("p.csv", good, "ok"),
        ("q.csv", good, "ok"),  # HDFS copy differs from what was downloaded
        ("r.csv", good, "ok"),  # never staged
        ("s.csv", good, "unavailable_404"),  # never acquired
    ]
    with open(manifest, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=acquire.MANIFEST_FIELDS)
        w.writeheader()
        for name, content, check in rows:
            w.writerow(
                {
                    "source": "daily_price_csv",
                    "file_name": name,
                    "bytes": len(content),
                    "sha256": hashlib.sha256(content).hexdigest(),
                    "check": check,
                    "hdfs_path": f"/raw/prices/{name}",
                }
            )
    out = tmp_path / "evidence"
    rc = verify.main(["--manifests", str(manifest), "--run-id", "t1", "--out", str(out)])
    assert rc == 1
    with open(out / "hdfs_verify_t1.csv") as fh:
        status = {r["file_name"]: r["status"] for r in csv.DictReader(fh)}
    assert status == {"p.csv": "ok", "q.csv": "size_mismatch;sha256_mismatch", "r.csv": "missing_in_hdfs"}
    inv = json.loads((out / "inventory_t1.json").read_text())
    assert inv["per_source"]["daily_price_csv"]["text_lines"] == 3  # p.csv (2) + q.csv (1)
    assert inv["not_acquired"] == [{"file_name": "s.csv", "check": "unavailable_404"}]
