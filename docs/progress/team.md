# TEAM: shared work

Owner: see `.github/workstreams.json`. Handover refs: §12 (kickoff), §13 (timeline), §14 (report plan), §18 (rules).
Format and rules: [README.md](README.md). Edit only your own task lines; add a line to the progress log whenever you finish or change something.

## Tasks

### Kickoff (do together before splitting work)
- [x] **TEAM-01** Repo scaffold (HANDOVER §17 layout) and CI/CD pipeline with progress tracking · owner: @jaecodess · due: 2026-10-29 · evidence: .github/workflows/ci.yml, CONTRIBUTING.md
- [ ] **TEAM-02** Agree HDFS folder layout (§9) and record it in the decision log · owner: @TODO · due: 2026-11-01
- [ ] **TEAM-03** Agree table names and columns for every stage, especially `features.weekly` (§9 contracts) · owner: @TODO · due: 2026-11-01
- [ ] **TEAM-04** Create project config (paths, dates, profiles, seeds, Spark settings) and `config/paths.yaml` per machine from the template · owner: @TODO · due: 2026-11-01 · needs: TEAM-02
- [ ] **TEAM-05** Accept or reject every `[PROPOSED]` item in the decision log (D-001 to D-014) · owner: @TODO · due: 2026-11-01
- [ ] **TEAM-06** Freeze development and full input profiles (the dev subset rule and the GDELT month) · owner: @TODO · due: 2026-11-01
- [ ] **TEAM-07** Send the teaching-team questions (§16) and record answers in the decision log · owner: @TODO · due: 2026-11-01
- [ ] **TEAM-08** Copy the brief PDF to `docs/brief/` and the Final Draft export to `docs/decisions/` · owner: @TODO · due: 2026-11-01
- [ ] **TEAM-09** Assign real GitHub handles in `.github/workstreams.json`, run `make codeowners`, turn on branch protection per `docs/ci/SETUP.md` · owner: @TODO · due: 2026-10-31

### Report (shared sections)
- [ ] **TEAM-10** Report section 1: overview, objectives and labelled end-to-end data-flow diagram (~300 words) · owner: @TODO · due: 2026-11-17 · needs: TEAM-06
- [ ] **TEAM-11** Report section 7: conclusions and limitations (~150 words) · owner: @TODO · due: 2026-11-19 · needs: MODEL-07, ANLY-07
- [ ] **TEAM-12** References with versions · owner: @TODO · due: 2026-11-19
- [ ] **TEAM-13** Appendix C: use of AI assistance, compiled from `ai-usage.md` · owner: @TODO · due: 2026-11-19
- [ ] **TEAM-14** Appendix D: team contributions table (name, work delivered, % total 100), compiled from the progress logs · owner: @TODO · due: 2026-11-19
- [ ] **TEAM-15** Cover page and main-text word count (max 3,000) · owner: @TODO · due: 2026-11-21 · needs: TEAM-10, TEAM-11
- [ ] **TEAM-16** Final ZIP checks: relative paths, no credentials or raw data, referenced files exist · owner: @TODO · due: 2026-11-21 · needs: PERF-13

## Progress log
<!-- Newest last. Format: - YYYY-MM-DD @handle: what you finished / changed / learned. One line, link evidence. -->
- 2026-10-29 @jaecodess: Repo scaffolded with CI/CD, per-workstream task files, STATUS.md generator and PR progress gate (TEAM-01).
- 2026-11-03 @husyGIT: Regenerated .github/CODEOWNERS and STATUS.md with `make codeowners` / `make status` (stale on main after the owners change in workstreams.json). No content changes. Evidence: .github/CODEOWNERS
- 2026-11-04 @jaecodess: Decision D-015: core storage finalised at 100 GiB per core node (~100 GiB logical HDFS at replication 2), superseding HANDOVER §2/§9; HANDOVER §2 annotated to point to D-015. Evidence: docs/decisions/decision-log.md, HANDOVER.md
- 2026-11-04 @husyGIT: Decision log D-016 to D-022 (pipeline rules and additive contract columns) and docs/pipeline/ (pipeline guide, EMR steps, section 3 draft, dev-run results). Evidence: docs/decisions/decision-log.md, docs/pipeline/README.md
- 2026-11-05 @husyGIT: Gate 1 (MODEL-03) locked in docs/decisions/decision-log.md before any validation run. Evidence: docs/decisions/decision-log.md
- 2026-11-05 @husyGIT: docs/model/ (Task 3 approach, EMR steps, section 4 draft, local validation results). Evidence: docs/model/README.md
- 2026-11-05 @husyGIT: requirements-spark.txt: added numpy==1.26.4 (pyspark.ml needs it in the CI Spark job; EMR ships numpy). Evidence: requirements-spark.txt
- 2026-11-05 @husyGIT: docs/model/aws_task3.md: full cluster setup commands (fetch and switch to ws/model/task3, "invalid reference" fix). Evidence: docs/model/aws_task3.md
- 2026-11-05 @husyGIT: docs/model/aws_task3.md: D0 section for a new cluster (check S3 for saved features, otherwise Task 2 C2 and C3 with WITH_GDELT=0). Evidence: docs/model/aws_task3.md
- 2026-11-08 @husyGIT: Gate 2 (MODEL-06) locked in docs/decisions/decision-log.md before the test run: frozen config lr_f1_r001. Evidence: docs/decisions/decision-log.md
- 2026-11-09 @husyGIT: docs/model/README.md EMR results and section 4 draft filled from run 20261106-1400-full-ref. Evidence: docs/model/README.md, docs/model/section4_draft.md
- 2026-11-09 @husyGIT: docs/analytics/ (Task 4 approach and tool choice per question, EMR steps aws_task4.md, section 5 draft skeleton). Evidence: docs/analytics/README.md, docs/analytics/aws_task4.md, docs/analytics/section5_draft.md
- 2026-11-10 @husyGIT: docs/analytics/README.md EMR results and section 5 draft filled from run 20261110-1000-full-ref. Evidence: docs/analytics/README.md, docs/analytics/section5_draft.md
