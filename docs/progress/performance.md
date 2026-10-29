# PERF: Performance and reproducibility

Handover refs: §2 (cloud constraints, runtime rules), §11 (plan), §13 (timeline). Report section 6 (~350 words), Appendix A, Appendix B.
Owns: `perf/`, `evidence/`, `notebooks/` (one owner for the notebook file), `docs/rerun/`, `tests/perf/`. Format and rules: [README.md](README.md).
Provides to everyone: the shared logging helper (writes `evidence/timings.csv`, records YARN application IDs, Hadoop job IDs, Hive query IDs) and the single run script `--profile` / `--config`. Publish both early.
Timing starts after raw inputs are staged in HDFS; download, S3-to-HDFS copy, setup and cluster startup are reported separately. Three repetitions per profile/config.

## Tasks
- [ ] **PERF-01** Shared logging helper → `evidence/timings.csv`, application/job/query IDs · owner: @TODO · due: 2026-11-04
- [ ] **PERF-02** Single run script taking `--profile` and `--config`, fresh `run_id` outputs per run · owner: @TODO · due: 2026-11-06 · needs: PERF-01
- [ ] **PERF-03** Define the development and full profiles as config (deterministic ~50-ticker rule plus one GDELT month) · owner: @TODO · due: 2026-11-03 · needs: TEAM-06
- [ ] **PERF-04** EMR bring-up: reference cluster config (emr-7.10.0, 1 + 2 m5.xlarge, replication 2, Spark 2 x 2 x 6 GiB, dynamic allocation off); confirm with teaching team · owner: @TODO · due: 2026-11-06
- [ ] **PERF-05** Thin notebook skeleton: one section per stage calling owned `.py`, `.sql` and Streaming files · owner: @TODO · due: 2026-11-07 · needs: PERF-02
- [ ] **PERF-06** Run set A: development profile, reference config, 3 repetitions · owner: @TODO · due: 2026-11-13 · needs: PERF-02, PIPE-13
- [ ] **PERF-07** Run set B: full profile, reference config, 3 repetitions; measure memory and disk per worker (`hdfs dfsadmin -report`, `df -h`) · owner: @TODO · due: 2026-11-15 · needs: PERF-06, MODEL-09
- [ ] **PERF-08** Choose ONE justified alternative from run-set-B evidence (shuffle partitions, file format or join strategy) · owner: @TODO · due: 2026-11-15 · needs: PERF-07
- [ ] **PERF-09** Run set C: full profile, alternative config, 3 repetitions; same output checks (row counts, predictions) · owner: @TODO · due: 2026-11-17 · needs: PERF-08
- [ ] **PERF-10** Storage budget measured (GDELT ZIPs logical vs physical, Parquet, shuffle/temp, peak per worker) · owner: @TODO · due: 2026-11-15 · needs: PERF-07
- [ ] **PERF-11** Appendix A: EMR setup and rerun guide, including dated fresh-session rerun evidence and troubleshooting · owner: @TODO · due: 2026-11-18 · needs: PERF-07
- [ ] **PERF-12** Independent rerun test: someone OUTSIDE this workstream follows Appendix A on a fresh cluster; fix whatever they get stuck on · owner: @TODO · due: 2026-11-19 · needs: PERF-11
- [ ] **PERF-13** Export the actual `.ipynb` from EMR and build the submission ZIP with `make zip`; hygiene clean · owner: @TODO · due: 2026-11-20 · needs: PERF-09, PERF-12
- [ ] **PERF-14** Report section 6 draft (rep tables, medians, ranges, failures) and Appendix B locators · owner: @TODO · due: 2026-11-18 · needs: PERF-09

## Progress log
<!-- Newest last. Format: - YYYY-MM-DD @handle: what you finished / changed / learned. One line, link evidence. -->
