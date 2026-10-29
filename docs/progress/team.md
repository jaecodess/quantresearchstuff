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
