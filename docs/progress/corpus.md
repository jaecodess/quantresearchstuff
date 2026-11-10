# CORP: Corpus (Task 1)

Handover refs: §2 Task 1, §5 (sources), §6.1 (quality checks), §9 (HDFS layout). Report section 2 (~450 words), Appendix B.
Owns: `corpus/`, `manifests/`, `docs/corpus/`. Format and rules: [README.md](README.md).
Provides: raw files in HDFS `/inf2006/raw/...`, manifests, data dictionary, quality findings, survivorship counts. PIPE and PERF depend on these.

## Tasks
- [x] **CORP-01** Get the price CSV and record source reference, version identifier and acquisition time (needs teaching-team answer, §16 Q5) · owner: @husyGIT · due: 2026-11-02 · evidence: manifests/prices_acquired=2026-11-03.csv, docs/corpus/inventory.md
- [x] **CORP-02** Local profiling script on a sample of the price CSV: schema, counts, per-ticker date ranges, OHLC violations, duplicates, overnight-jump scan (§19 step 2) · owner: @husyGIT · due: 2026-11-03 · needs: CORP-01 · evidence: corpus/profile_prices.py, docs/corpus/dev_profile_2026-11-02/summary.json, tests/corpus/test_profile_prices.py
- [x] **CORP-03** Manifest script: source reference, size, SHA-256, retrieval timestamp (§19 step 9) · owner: @husyGIT · due: 2026-11-03 · evidence: corpus/acquire.py, corpus/verify_hdfs.py, tests/corpus/test_acquire_and_verify.py
- [x] **CORP-04** Confirm the real price schema (expected date, open, high, low, close, volume, ticker) and publish it to PIPE · owner: @husyGIT · due: 2026-11-02 · needs: CORP-01 · evidence: docs/corpus/data_dictionary.md, docs/corpus/dev_profile_2026-11-02/summary.json
- [x] **CORP-05** Verify the GDELT 1.0 event column count and field positions against the codebook and supplied metadata (§5) · owner: @husyGIT · due: 2026-11-03 · evidence: docs/corpus/data_dictionary.md
- [x] **CORP-06** Acquire and stage raw price CSV in HDFS unchanged under `/inf2006/raw/prices/<version>/acquired=<date>/` · owner: @husyGIT · due: 2026-11-09 · needs: CORP-03 · evidence: manifests/prices_acquired=2026-11-03.csv, evidence/corpus/stage_20261103-2250-full-ref.log, evidence/corpus/prices_profile_20261103-2250-full-ref/summary.json
- [~] **CORP-07** Acquire and stage the GDELT ZIPs (decide 731 archives vs documented subset, §16 Q4), no expanded copy in HDFS · owner: @husyGIT · due: 2026-11-09 · needs: CORP-03, CORP-05
- [x] **CORP-08** Survivorship check: series ending before 2017-12-29 and starting after 2013-02-08; report counts either way (§5) · owner: @husyGIT · due: 2026-11-05 · needs: CORP-02 · evidence: evidence/corpus/prices_profile_20261103-2250-full-ref/per_ticker.csv, docs/corpus/quality_findings.md
- [~] **CORP-09** Raw quality findings for price and GDELT: coverage, missing, duplicates, corrupt fields, limitations the pipeline must handle (§6.1) · owner: @husyGIT · due: 2026-11-10 · needs: CORP-06, CORP-07
- [x] **CORP-10** Data dictionary: sources, versions, date coverage, native identifiers, schemas, units, time semantics, table relationships · owner: @husyGIT · due: 2026-11-08 · needs: CORP-04, CORP-05 · evidence: docs/corpus/data_dictionary.md
- [~] **CORP-11** Corpus inventory and reproducibility record: measured rows and bytes, checksums, config, evidence inputs can be re-acquired · owner: @husyGIT · due: 2026-11-12 · needs: CORP-06, CORP-07, CORP-09
- [~] **CORP-12** Report section 2 draft and Appendix B locators for the corpus · owner: @husyGIT · due: 2026-11-17 · needs: CORP-11
- [ ] **CORP-13** Cross-review: read PIPE cleaning rules against your quality findings (does every finding have a handling rule?) · owner: @TODO · due: 2026-11-12 · needs: CORP-09, PIPE-04

## Progress log
<!-- Newest last. Format: - YYYY-MM-DD @handle: what you finished / changed / learned. One line, link evidence. -->
- 2026-11-02 @husyGIT: Price source pinned (plotly/datasets 0c447c4, SHA-256 recorded); acquisition-to-S3, S3-to-HDFS staging and HDFS verification scripts; PySpark raw profile (dev run reconciles exactly to the brief's 605,887 accepted bars; 23 invalid rows, 13,130 rows after 2017-12-29, survivorship confirmed); data dictionary, quality findings, inventory, section 2 draft and AWS guide. EMR runs pending. Evidence: corpus/, docs/corpus/ (CORP-01 to CORP-12)
- 2026-11-03 @husyGIT: AWS guide expanded with full S3 bucket, EC2 key pair/network/security group and EMR console settings, plus an equivalent `aws emr create-cluster` command. Evidence: docs/corpus/aws_guide.md (CORP-06, CORP-07)
- 2026-11-03 @husyGIT: AWS guide A1: step-by-step S3 folder creation (four folders, one at a time). Evidence: docs/corpus/aws_guide.md (CORP-06)
- 2026-11-03 @husyGIT: Brief update applied: core EBS data volume now 100 GiB per node (200 GiB physical, ~100 GiB logical at replication 2). Updated AWS guide console settings, CLI JSON and storage budget, and the inventory record. HANDOVER §2/§9 still say 200 GiB per node (TEAM file). Evidence: docs/corpus/aws_guide.md, docs/corpus/inventory.md (CORP-06, CORP-11)
- 2026-11-03 @husyGIT: AWS guide B3: spark-submit profile command written out on one line with the 2026-11-03 run_id. Evidence: docs/corpus/aws_guide.md (CORP-09)
- 2026-11-04 @husyGIT: AWS guide B7: bundle evidence and manifests into one tar.gz, download with scp, S3 fallback. Evidence: docs/corpus/aws_guide.md (CORP-11)
- 2026-11-04 @husyGIT: EMR run 20261103-2250-full-ref committed: price + 731 GDELT archives acquired (all checksums match GDELT's lists, 9,611,684,528 bytes), staged to HDFS (fsck HEALTHY, rep 2), price profile on EMR identical to dev (application_1793714742178_0002). Docs updated with EMR figures; bucket name redacted. GDELT content audit needs a rerun (mapper path bug, fixed in PIPE). Evidence: evidence/corpus/, manifests/ (CORP-01, CORP-06, CORP-07, CORP-08, CORP-09, CORP-11)
- 2026-11-10 @husyGIT: Teaching team requires all four sources in S3. Added SEC EDGAR (311 filings from the course inventory, config/sec_filing_inventory.csv) and Wikinews (2026-10-01 full-history dumps) to corpus/acquire.py with integrity checks and manifests; HDFS verifier checks them by size; EMR guide docs/corpus/aws_aux_sources.md. Stored only, not analysed. Remaining: EMR acquisition run. Evidence: corpus/acquire.py, config/sec_filing_inventory.csv, docs/corpus/aws_aux_sources.md (CORP-07, CORP-11)
