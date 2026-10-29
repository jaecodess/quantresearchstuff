# PIPE: Pipeline (Task 2)

Handover refs: §2 Task 2, §6 (quality, split detection), §7 (calendar, universe, label, features, ranks, splits), §9 (architecture, contracts). Report section 3 (~600 words), Appendix B.
Owns: `streaming/`, `hive/`, `spark/`, `config/split_events.csv`, `tests/pipeline/`, `tests/spark/`. Format and rules: [README.md](README.md).
Provides: the `features.weekly` contract (publish a stub early so MODEL and ANLY are not blocked), `clean.*` and `features.*` tables.
Streaming job convention: `streaming/<job>/mapper.py`, `reducer.py`, `tests/sample.in`, `tests/expected.out` (CI runs `cat sample.in | mapper | sort | reducer` and diffs; see `streaming/_template/`).

## Tasks
- [ ] **PIPE-01** Hive DDL scripts for every table in the §9 contracts, numbered by run order, recreatable on a fresh cluster · owner: @TODO · due: 2026-11-04 · needs: TEAM-03
- [ ] **PIPE-02** Hadoop Streaming price audit mapper/reducer (row counts, date range, null/invalid/duplicate counts, overnight-jump flags), tested locally · owner: @TODO · due: 2026-11-05 · needs: CORP-04
- [ ] **PIPE-03** Prototype GDELT mapper on 2 to 3 archives (field count, malformed lines, daily aggregates); NLineInputFormat over an archive manifest, one mapper per archive · owner: @TODO · due: 2026-11-06 · needs: CORP-05
- [ ] **PIPE-04** PySpark cleaning: validity rules (§6.1), clean and rejected outputs kept separate, never fill with future observations · owner: @TODO · due: 2026-11-06 · needs: PIPE-01, CORP-04
- [ ] **PIPE-05** Split detection (§6.2) and reviewed `config/split_events.csv`; register as Hive table · owner: @TODO · due: 2026-11-07 · needs: PIPE-04
- [ ] **PIPE-06** Trading calendar and weekly start dates (holiday Fridays cross-check, week lengths) → `features.calendar_weeks` · owner: @TODO · due: 2026-11-06 · needs: PIPE-04
- [ ] **PIPE-07** Weekly universe (eligibility incl. split exclusion) and labels (median over eligible stocks, ratio never a feature) · owner: @TODO · due: 2026-11-07 · needs: PIPE-05, PIPE-06
- [ ] **PIPE-08** Features F1/F2 and within-week percentile ranks (§7.4, §7.5) → `features.weekly` · owner: @TODO · due: 2026-11-08 · needs: PIPE-07
- [ ] **PIPE-09** Publish a STUB `features.weekly` from the dev sample with the agreed columns, so MODEL and ANLY can build against the contract · owner: @TODO · due: 2026-11-03 · needs: TEAM-03
- [ ] **PIPE-10** Reconciliation: raw = clean + rejected (+ documented duplicates) per source and per split; eligible stocks per week; split exclusion counts · owner: @TODO · due: 2026-11-08 · needs: PIPE-08
- [ ] **PIPE-11** GDELT: full Streaming job → `clean.gdelt_daily`; `features.weekly_news` (`news_abn`, `tone_mean`, `conflict_share`) with the conservative timing rule (§8.9) · owner: @TODO · due: 2026-11-10 · needs: PIPE-03, CORP-07
- [ ] **PIPE-12** Leakage cross-review of MODEL evaluation and training code (§12) · owner: @TODO · due: 2026-11-10 · needs: MODEL-01
- [ ] **PIPE-13** Full pipeline run on EMR with job and query IDs, mapper/reducer scripts, configs and output schemas as evidence · owner: @TODO · due: 2026-11-13 · needs: PIPE-10, PIPE-11
- [ ] **PIPE-14** Report section 3 draft and Appendix B locators for the pipeline · owner: @TODO · due: 2026-11-17 · needs: PIPE-13

## Progress log
<!-- Newest last. Format: - YYYY-MM-DD @handle: what you finished / changed / learned. One line, link evidence. -->
