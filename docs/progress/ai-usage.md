# AI assistance log (feeds report Appendix C)

Append one row per AI session that affected committed work. Do not edit old rows. Merge conflicts auto-resolve (union merge), so keep each entry on a single line.
Required when a PR ticks "AI assistance was used". The brief requires AI use to be declared.

| Date | Who | Tool and model | Purpose | Affected work (paths or task IDs) |
|---|---|---|---|---|
| 2026-10-29 | @jaecodess | Planning chat in the Claude app | Project framing, review of the Final Draft, HANDOVER.md | HANDOVER.md |
| 2026-10-29 | @jaecodess | Claude Code (cloud session) | Repo scaffold, CI/CD pipeline, progress tracking | TEAM-01, .github/, scripts/, docs/progress/ |
| 2026-11-02 | @husyGIT | Claude (cloud session) | GDELT file profiling, Streaming mapper/reducer, Hive DDL and tests for the GDELT daily job | PIPE-03, streaming/gdelt_daily/, hive/10_gdelt_daily.sql |
| 2026-11-02 | @husyGIT | Claude (cloud session) | Corpus scripts (acquire, stage, verify, profile), tests, data dictionary, quality findings, inventory, section 2 draft and AWS guide | CORP-01 to CORP-12, corpus/, docs/corpus/, tests/corpus/ |
| 2026-11-04 | @husyGIT | Claude (cloud session) | Task 2 pipeline: Streaming price audit, PySpark cleaning/split screen/features/reconciliation, Hive SQL calendar and weekly news, tests, EMR runner, pipeline docs and section 3 draft | PIPE-01 to PIPE-14, spark/, hive/, streaming/price_audit/, docs/pipeline/ |
| 2026-11-04 | @husyGIT | Claude (cloud session) | Reviewed Task 1 EMR evidence, diagnosed and fixed the GDELT mapper path bug and the slow HDFS verifier, updated corpus docs with EMR figures | CORP-01 to CORP-12, PIPE-03, PIPE-11, evidence/corpus/, streaming/gdelt_daily/, corpus/verify_hdfs.py |
| 2026-11-04 | @jaecodess | Claude Code (cloud session) | Recorded the 100 GiB core storage decision and annotated HANDOVER | D-015, docs/decisions/decision-log.md, HANDOVER.md |
| 2026-11-05 | @husyGIT | Claude (cloud session) | Task 3: evaluation module, baselines, Spark ML grid, guarded validate/test/reload script, Hive model tables, tests, EMR runner, docs and section 4 draft; local validation on train and validation only | MODEL-01 to MODEL-09, MODEL-11, modeling/, evaluation/, tests/model/, docs/model/ |
| 2026-11-09 | @husyGIT | Claude (cloud session) | Checked the Task 3 EMR evidence, wrote up results and section 4 draft | MODEL-05 to MODEL-09, MODEL-11, evidence/model/, docs/model/ |
| 2026-11-09 | @husyGIT | Claude (cloud session) | Task 4: Hive and PySpark analytics code, charts, synthetic end-to-end test, EMR runner and guide, section 5 skeleton | ANLY-01 to ANLY-04, ANLY-06, ANLY-07, analytics/, docs/analytics/ |
