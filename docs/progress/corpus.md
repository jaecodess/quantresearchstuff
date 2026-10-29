# CORP: Corpus (Task 1)

Handover refs: §2 Task 1, §5 (sources), §6.1 (quality checks), §9 (HDFS layout). Report section 2 (~450 words), Appendix B.
Owns: `corpus/`, `manifests/`, `docs/corpus/`. Format and rules: [README.md](README.md).
Provides: raw files in HDFS `/inf2006/raw/...`, manifests, data dictionary, quality findings, survivorship counts. PIPE and PERF depend on these.

## Tasks
- [ ] **CORP-01** Get the price CSV and record source reference, version identifier and acquisition time (needs teaching-team answer, §16 Q5) · owner: @TODO · due: 2026-11-02
- [ ] **CORP-02** Local profiling script on a sample of the price CSV: schema, counts, per-ticker date ranges, OHLC violations, duplicates, overnight-jump scan (§19 step 2) · owner: @TODO · due: 2026-11-03 · needs: CORP-01
- [ ] **CORP-03** Manifest script: source reference, size, SHA-256, retrieval timestamp (§19 step 9) · owner: @TODO · due: 2026-11-03
- [ ] **CORP-04** Confirm the real price schema (expected date, open, high, low, close, volume, ticker) and publish it to PIPE · owner: @TODO · due: 2026-11-02 · needs: CORP-01
- [ ] **CORP-05** Verify the GDELT 1.0 event column count and field positions against the codebook and supplied metadata (§5) · owner: @TODO · due: 2026-11-03
- [ ] **CORP-06** Acquire and stage raw price CSV in HDFS unchanged under `/inf2006/raw/prices/<version>/acquired=<date>/` · owner: @TODO · due: 2026-11-09 · needs: CORP-03
- [ ] **CORP-07** Acquire and stage the GDELT ZIPs (decide 731 archives vs documented subset, §16 Q4), no expanded copy in HDFS · owner: @TODO · due: 2026-11-09 · needs: CORP-03, CORP-05
- [ ] **CORP-08** Survivorship check: series ending before 2017-12-29 and starting after 2013-02-08; report counts either way (§5) · owner: @TODO · due: 2026-11-05 · needs: CORP-02
- [ ] **CORP-09** Raw quality findings for price and GDELT: coverage, missing, duplicates, corrupt fields, limitations the pipeline must handle (§6.1) · owner: @TODO · due: 2026-11-10 · needs: CORP-06, CORP-07
- [ ] **CORP-10** Data dictionary: sources, versions, date coverage, native identifiers, schemas, units, time semantics, table relationships · owner: @TODO · due: 2026-11-08 · needs: CORP-04, CORP-05
- [ ] **CORP-11** Corpus inventory and reproducibility record: measured rows and bytes, checksums, config, evidence inputs can be re-acquired · owner: @TODO · due: 2026-11-12 · needs: CORP-06, CORP-07, CORP-09
- [ ] **CORP-12** Report section 2 draft and Appendix B locators for the corpus · owner: @TODO · due: 2026-11-17 · needs: CORP-11
- [ ] **CORP-13** Cross-review: read PIPE cleaning rules against your quality findings (does every finding have a handling rule?) · owner: @TODO · due: 2026-11-12 · needs: CORP-09, PIPE-04

## Progress log
<!-- Newest last. Format: - YYYY-MM-DD @handle: what you finished / changed / learned. One line, link evidence. -->
