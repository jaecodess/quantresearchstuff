# Raw-corpus inventory and reproducibility record (CORP-11)

Everything needed to get exactly the same raw inputs again, or to prove which inputs were used. Fill the `…` cells from the EMR run (`evidence/corpus/inventory_<run_id>.json`, `stage_<run_id>.log`).

## 1. Sources and how to re-acquire them

| Source | Exact reference | Integrity check | Re-acquire with |
|---|---|---|---|
| Daily price CSV | `https://raw.githubusercontent.com/plotly/datasets/0c447c47b757ad74edecab31f0d72f849d2e67c2/all_stocks_5yr.csv` (the brief's `master` link, pinned to the commit current on acquisition; git blob `2a1f02e3675ad8bb6f3a8e536e15f601cfe32fbf`) | SHA-256 `6aea253cd19de60b568143991aaf1fa482456565c389205658d236e595e716cf`, 29,580,549 bytes; `acquire.py` refuses any other bytes | `python3 corpus/acquire.py prices --s3-raw <s3_raw>` |
| GDELT 1.0 Events | `http://data.gdeltproject.org/events/<YYYYMMDD>.export.CSV.zip` for 2015-07-01 … 2017-06-30 (731 files) | Each archive checked against GDELT's published `md5sums` and `filesizes` (both saved under `_provenance/`); our SHA-256 recorded per file | `python3 corpus/acquire.py gdelt --s3-raw <s3_raw>` |

If the pinned price URL ever disappears, any copy of the file can be identified by its SHA-256 or git blob ID. GDELT archives do not change after publication; the md5 lists prove whether a copy matches.

## 2. Acquisition record

One manifest per acquisition run, committed under [`manifests/`](../../manifests/) and copied to `<s3_raw>/_manifests/`. Columns: source, version, file name, source URL, retrieval time (UTC), bytes, SHA-256, MD5, expected bytes / MD5 / SHA-256, check result, S3 URI, HDFS path.

| Source | Manifest | Acquired (UTC) | Files ok / expected | Bytes |
|---|---|---|---|---|
| Daily price CSV | `manifests/prices_acquired=<date>.csv` | … | … / 1 | 29,580,549 |
| GDELT | `manifests/gdelt_acquired=<date>.csv` | … | … / 731 | … (brief: 9.61 GB) |

## 3. Storage and configuration

- Durable raw store: S3 `<s3_raw>/` (bucket name kept out of the repo; set it in `config/paths.yaml`). Same relative layout as HDFS.
- Session copy: `s3-dist-cp` into HDFS `/inf2006/raw/`, then made read-only (`corpus/stage_to_hdfs.sh`).
- HDFS replication: `dfs.replication=2` (cluster config, PERF-04). Physical HDFS use ≈ 2 × logical bytes: GDELT ZIPs ≈ 19.2 GB physical against a nominal 200 GiB logical ceiling.
- Formats: raw CSV and ZIP unchanged. No expanded GDELT copy is ever stored (D-010).
- Software: EMR release …, Hadoop …, Spark …, Python … (from the cluster; record in Appendix A).

## 4. Measured inventory in HDFS (EMR)

| Source | Files | Bytes (logical) | Bytes (physical, `hdfs dfs -du`) | Rows | Verification |
|---|---|---|---|---|---|
| Daily price CSV | 1 | 29,580,549 | … | 619,040 data rows + 1 header (dev, same bytes) | size + SHA-256 of the HDFS copy: … |
| GDELT | … | … | … | … events (streaming audit `n_lines`) | size + SHA-256 per archive: … |

Evidence: `evidence/corpus/hdfs_verify_<run_id>.csv`, `evidence/corpus/inventory_<run_id>.json`, `evidence/corpus/stage_<run_id>.log`, `evidence/corpus/prices_profile_<run_id>/`.
