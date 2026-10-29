# ANLY: Analytics (Task 4)

Handover refs: §2 Task 4, §8.5 (subgroups), §8.9 (GDELT), §10 (the three questions). Report section 5 (~550 words), Appendix B.
Owns: `analytics/`, `tests/analytics/`. Format and rules: [README.md](README.md).
Rules: full data stays distributed, plot only aggregated results; association is not causation; never call results returns or investability; anything that could influence feature or model choices runs on training data only until the model is frozen (MODEL-06).

## Tasks
- [ ] **ANLY-01** Q1 (Hive): within-week quintiles of `rel_volume` vs next-week outcomes, with group sizes and intervals; training years only until the model is frozen · owner: @TODO · due: 2026-11-14 · needs: PIPE-08
- [ ] **ANLY-02** Q2 (PySpark): weekly AUC and paired difference vs B3 by quarter, volatility tercile and market-dispersion tercile, 2017 plus 2016 consistency; weeks, rows and intervals reported · owner: @TODO · due: 2026-11-16 · needs: MODEL-07
- [ ] **ANLY-03** Q3a (Hive + PySpark): market-wide volatility and dispersion by `news_abn` tercile; pre-start and same-week labelled separately · owner: @TODO · due: 2026-11-16 · needs: PIPE-11
- [ ] **ANLY-04** Q3b: conditional evaluation of the frozen model across news terciles (2016 and 2017 reported separately) · owner: @TODO · due: 2026-11-16 · needs: ANLY-03, MODEL-07
- [ ] **ANLY-05** Optional: exploratory interaction experiment on the frozen configuration, with the ~26-week training limitation stated · owner: @TODO · due: 2026-11-18 · needs: ANLY-04
- [ ] **ANLY-06** Labelled charts of aggregated results; explain tool choice (Hive vs PySpark) per analysis · owner: @TODO · due: 2026-11-17 · needs: ANLY-02, ANLY-04
- [ ] **ANLY-07** Report section 5 draft with limitations and uncertainty; compare with the Task 3 baseline; Appendix B locators · owner: @TODO · due: 2026-11-18 · needs: ANLY-06

## Progress log
<!-- Newest last. Format: - YYYY-MM-DD @handle: what you finished / changed / learned. One line, link evidence. -->
