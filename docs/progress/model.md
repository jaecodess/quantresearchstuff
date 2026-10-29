# MODEL: Model (Task 3)

Handover refs: §2 Task 3, §7.6 (splits), §8 (baselines, grid, selection, metrics, bootstrap, success rule, weights, reload). Report section 4 (~600 words), Appendix B.
Owns: `modeling/`, `evaluation/`, `models/`, `tests/model/`. Format and rules: [README.md](README.md).
Gates (do not skip): the grid must be in the decision log BEFORE validation runs (MODEL-03); the success rule must be in the decision log BEFORE the test set is touched (MODEL-06); the test set is evaluated ONCE (MODEL-07).
Needs from PIPE: the `features.weekly` contract (stub first, real later).

## Tasks
- [ ] **MODEL-01** Evaluation module: tie-aware weekly AUC (rank-sum), week-level bootstrap (10,000 resamples, fixed seed), paired difference; tested on stub features · owner: @TODO · due: 2026-11-07 · needs: PIPE-09
- [ ] **MODEL-02** Baselines B0 to B3 on identical rows; B3 direction fixed on training data (§8.1) · owner: @TODO · due: 2026-11-08 · needs: MODEL-01
- [ ] **MODEL-03** GATE: write the 8-config grid into the decision log before running validation · owner: @TODO · due: 2026-11-08
- [ ] **MODEL-04** Spark ML pipelines (Imputer, VectorAssembler, StandardScaler, LR or RF) fitted on train only, with fixed seeds · owner: @TODO · due: 2026-11-09 · needs: PIPE-09, MODEL-03
- [ ] **MODEL-05** Validation comparison of the 8 configs on 2016; measure week-to-week AUC spread; apply the one-standard-error simplicity rule · owner: @TODO · due: 2026-11-11 · needs: MODEL-04, PIPE-08
- [ ] **MODEL-06** GATE: freeze the chosen config and write the success rule (§8.6) into the decision log · owner: @TODO · due: 2026-11-11 · needs: MODEL-05
- [ ] **MODEL-07** Single evaluation on 2017 test: mean weekly AUC, paired difference vs B3, balanced accuracy, precision, recall, rank IC, decile spread, turnover · owner: @TODO · due: 2026-11-12 · needs: MODEL-06
- [ ] **MODEL-08** Output weights: top 10% equal weight per week, described as hypothetical selection output · owner: @TODO · due: 2026-11-12 · needs: MODEL-07
- [ ] **MODEL-09** Save complete PipelineModel; fresh-session reload reproduces predictions within 1e-9 · owner: @TODO · due: 2026-11-13 · needs: MODEL-07
- [ ] **MODEL-10** Leakage cross-review of PIPE feature, universe and split code (§12) · owner: @TODO · due: 2026-11-10 · needs: PIPE-08
- [ ] **MODEL-11** Report section 4 draft and Appendix B locators for the model · owner: @TODO · due: 2026-11-17 · needs: MODEL-09

## Progress log
<!-- Newest last. Format: - YYYY-MM-DD @handle: what you finished / changed / learned. One line, link evidence. -->
