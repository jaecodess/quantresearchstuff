# Raw-corpus inventory and reproducibility record (CORP-11)

Everything needed to get exactly the same raw inputs again, or to prove which inputs were used. Measured on EMR in run `20261103-2250-full-ref` (evidence in [`evidence/corpus/`](../../evidence/corpus/)). The bucket name is replaced by `<bucket>` in committed files.

## 1. Sources and how to re-acquire them

| Source | Exact reference | Integrity check | Re-acquire with |
|---|---|---|---|
| Daily price CSV | `https://raw.githubusercontent.com/plotly/datasets/0c447c47b757ad74edecab31f0d72f849d2e67c2/all_stocks_5yr.csv` (the brief's `master` link, pinned to the commit current on acquisition; git blob `2a1f02e3675ad8bb6f3a8e536e15f601cfe32fbf`) | SHA-256 `6aea253cd19de60b568143991aaf1fa482456565c389205658d236e595e716cf`, 29,580,549 bytes; `acquire.py` refuses any other bytes | `python3 corpus/acquire.py prices --s3-raw <s3_raw>` |
| GDELT 1.0 Events | `http://data.gdeltproject.org/events/<YYYYMMDD>.export.CSV.zip` for 2015-07-01 … 2017-06-30 (731 files) | Each archive checked against GDELT's published `md5sums` and `filesizes` (both saved under `_provenance/`); our SHA-256 recorded per file | `python3 corpus/acquire.py gdelt --s3-raw <s3_raw>` |

| SEC EDGAR (stored, not analysed) | The 311 complete-submission files listed in the course inventory [`config/sec_filing_inventory.csv`](../../config/sec_filing_inventory.csv) (from `SEC_Filing_Inventory.xlsx`, SHA-256 `a65b478a...54d7`), `https://www.sec.gov/Archives/edgar/data/<cik>/<accession>.txt` | Each file must contain its own `ACCESSION NUMBER`; SHA-256 recorded (SEC publishes no checksums) | `python3 corpus/acquire.py sec --s3-raw <s3_raw> --user-agent "<name> <email>"` |
| Wikinews (stored, not analysed) | `https://dumps.wikimedia.org/enwikinews/20261001/`, full-history dump (`pages-meta-history1`, 2 parts, `.bz2` and `.7z`) | MD5 and size from the dump's `dumpstatus.json` (saved under `_provenance/`) | `python3 corpus/acquire.py wikinews --s3-raw <s3_raw>` |
If the pinned price URL ever disappears, any copy of the file can be identified by its SHA-256 or git blob ID. GDELT archives do not change after publication; the md5 lists prove whether a copy matches.

## 2. Acquisition record

One manifest per acquisition run, committed under [`manifests/`](../../manifests/) and copied to `<s3_raw>/_manifests/`. Columns: source, version, file name, source URL, retrieval time (UTC), bytes, SHA-256, MD5, expected bytes / MD5 / SHA-256, check result, S3 URI, HDFS path.

| Source | Manifest | Acquired (UTC) | Files ok / expected | Bytes |
|---|---|---|---|---|
| Daily price CSV | [`manifests/prices_acquired=2026-11-03.csv`](../../manifests/prices_acquired=2026-11-03.csv) | 2026-11-03 14:30:43 | 1 / 1 (SHA-256 matches the pinned value) | 29,580,549 |
| GDELT | [`manifests/gdelt_acquired=2026-11-03.csv`](../../manifests/gdelt_acquired=2026-11-03.csv) | 2026-11-03 14:32:17 to 14:37:08 | 731 / 731 (MD5 and size match GDELT's lists for all) | 9,611,684,528 |
| SEC EDGAR | [`manifests/sec_acquired=2026-11-10.csv`](../../manifests/sec_acquired=2026-11-10.csv) | 2026-11-10 05:23:33 to 05:28:30 | 311 / 311 (each carries its own accession number) | 2,777,408,405 |
| Wikinews | [`manifests/wikinews_acquired=2026-11-10.csv`](../../manifests/wikinews_acquired=2026-11-10.csv) | 2026-11-10 05:15:43 to 05:18:36 | 4 / 4 (MD5 and size match `dumpstatus.json`) | 946,012,974 |

## 3. Storage and configuration

- Durable raw store: S3 `<s3_raw>/` (bucket name kept out of the repo; set it in `config/paths.yaml`). Same relative layout as HDFS.
- Session copy: `s3-dist-cp` into HDFS `/inf2006/raw/`, then made read-only (`corpus/stage_to_hdfs.sh`).
- HDFS replication: `dfs.replication=2` (cluster config, PERF-04). Core storage: one 100 GiB gp3 data volume per core node (brief update for Learner Lab limits), 200 GiB physical in total, so a nominal **100 GiB logical** HDFS ceiling at replication 2, before HDFS overhead and YARN shuffle/temp space. Root volumes are separate and not counted. Physical HDFS use ≈ 2 × logical bytes: GDELT ZIPs ≈ 19.2 GB physical (about 10% of physical capacity); an expanded copy (≈ 109 GB physical) would not fit, which is a second reason for D-010.
- Formats: raw CSV and ZIP unchanged. No expanded GDELT copy is ever stored (D-010).
- Software ([versions](../../evidence/corpus/versions_20261103-2250-full-ref.txt)): EMR emr-7.10.0, Hadoop 3.4.1-amzn-2, Spark 3.5.5-amzn-1, Hive 3.1.3-amzn-19, Python 3.9.25.

## 4. Measured inventory in HDFS (EMR)

| Source | Files | Bytes (logical) | Bytes (physical, `hdfs dfs -du`) | Rows | Verification |
|---|---|---|---|---|---|
| Daily price CSV | 1 | 29,580,549 (28.2 M) | 56.4 M | 619,040 data rows + 1 header (EMR profile, `application_1793714742178_0002`) | size + SHA-256 of the HDFS copy: match ([hdfs_verify_20261105-1430-full-ref.csv](../../evidence/corpus/hdfs_verify_20261105-1430-full-ref.csv)) |
| GDELT | 731 archives + 2 provenance lists | 9,611,684,528 (9.0 G) | 17.9 G | 144,207,180 events (Streaming audit, run `20261105-1430-full-ref`) | SHA-256 of every HDFS copy against the manifest, computed by the Streaming job: 731 / 731 match |
| SEC EDGAR | 311 filings + inventory | 2,777,408,405 (2.6 G) | 5.2 G | not parsed (stored only) | size of every HDFS copy against the manifest ([inventory_20261110-1300-aux.json](../../evidence/corpus/inventory_20261110-1300-aux.json)) |
| Wikinews | 4 dump files + `dumpstatus.json` | 946,012,974 (902.2 M) | 1.8 G | not parsed (stored only) | size of every HDFS copy against the manifest |

All four sources staged together on 2026-11-10 (run `20261110-1300-aux`, [stage log](../../evidence/corpus/stage_20261110-1300-aux.log)): S3 to HDFS in 1 min 29 s, 1,047 / 1,047 files present with manifest sizes, price CSV SHA-256 matches, `hdfs fsck /inf2006/raw` HEALTHY. Total raw corpus about 13.4 GB logical, 25 GB physical at replication 2.

HDFS health: `hdfs fsck /inf2006/raw` HEALTHY, replication 2.0, 0 missing or corrupt blocks. The first verifier run re-hashed each file with its own `hdfs dfs -cat` (about 35 minutes for 732 files) and was stopped before writing results; it now hashes the price CSV only and leaves the archives to the parallel Streaming job. Rerun on 2026-11-05 (`20261105-1430-full-ref`): 732 / 732 files present with manifest sizes, price CSV SHA-256 matches, 0 problems ([inventory_20261105-1430-full-ref.json](../../evidence/corpus/inventory_20261105-1430-full-ref.json)).

Evidence: `evidence/corpus/hdfs_verify_<run_id>.csv`, `evidence/corpus/inventory_<run_id>.json`, `evidence/corpus/stage_<run_id>.log`, `evidence/corpus/prices_profile_<run_id>/`.
