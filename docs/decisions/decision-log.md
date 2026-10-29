# Decision log

Append decisions; do not rewrite history. Status: `PROPOSED` (from review, not yet confirmed), `ACCEPTED`, `REJECTED`. Record the date and who agreed. TEAM-05 is done when no row says PROPOSED.

## Items from HANDOVER §3 and §6 to §9

| ID | Item | Handover | Status | Decided | By |
|---|---|---|---|---|---|
| D-001 | F2: 8 indicators in four families, fixed standard windows, validation never tunes windows | §7.4 | PROPOSED | | |
| D-002 | Within-week percentile-rank features | §7.5 | PROPOSED | | |
| D-003 | Baselines B2 (1-week reversal) and B3 (best single-indicator rule); headline comparison is vs B3 | §8.1 | PROPOSED | | |
| D-004 | Weeks as the unit of analysis; week-level bootstrap intervals; paired difference vs B3 | §8.5 | PROPOSED | | |
| D-005 | Split detection rule and reviewed `config/split_events.csv`; stock-weeks crossing a split are ineligible | §6.2, §7.2 | PROPOSED | | |
| D-006 | Pre-declared 8-config grid and one-standard-error simplicity rule | §8.2, §8.3 | PROPOSED | | |
| D-007 | Pre-registered success rule | §8.6 | PROPOSED | | |
| D-008 | GDELT is not a main-model feature; conditional evaluation across `news_abn` terciles | §8.9 | PROPOSED | | |
| D-009 | Conservative GDELT timing rule (archives at least one calendar day before the cutoff day) | §8.9 | PROPOSED | | |
| D-010 | No expanded GDELT copy in HDFS; one mapper per archive via NLineInputFormat | §5, §9 | PROPOSED | | |
| D-011 | Task 4 analyses that could influence choices run on training data only until the model is frozen | §7.6 | PROPOSED | | |
| D-012 | Survivorship check reported either way | §5 | PROPOSED | | |
| D-013 | Hive DDL kept in versioned scripts (metastore may vanish with the cluster) | §9 | PROPOSED | | |
| D-014 | Quarters and terciles instead of months for subgroup analysis; a subgroup finding needs the same direction in 2016 and 2017 | §8.5 | PROPOSED | | |

## Teaching-team answers (HANDOVER §16)

| # | Question | Answer | Date |
|---|---|---|---|
| 1 | Output weights format; assessed workload | | |
| 2 | EMR release, permitted configuration changes, timing policy | | |
| 3 | Does market-wide GDELT processing count as meaningful pipeline use? | | |
| 4 | Full GDELT run = all 731 archives, or documented subset? | | |
| 5 | Where and how to acquire the price snapshot; version identifier | | |
| 6 | External data allowed? (current plan: no) | | |
| 7 | Appendix lettering (AI use C vs contributions D) | | |

## Gate 1: grid (MODEL-03). Lock BEFORE running validation

Pre-declared grid (HANDOVER §8.2). Confirm or edit, then fill "Locked":

- LogisticRegression on F1 and F2, `regParam` in {0.0, 0.01}: 4 configs
- RandomForestClassifier on F1 and F2, `numTrees` = 200, `maxDepth` in {4, 8}, fixed `seed`: 4 configs

Locked on: ____ by: ____ in commit: ____

## Gate 2: frozen model and success rule (MODEL-06). Lock BEFORE the test set is touched

Frozen configuration: ____ (selected on 2016 mean weekly AUC; simplicity rule applied: ____)

Success rule (HANDOVER §8.6): the frozen model shows evidence of predictive signal only if, on 2017 test, (1) its mean weekly AUC has a 95% interval above 0.5, and (2) the mean paired difference against B3 has a 95% interval above 0. Otherwise the finding is "no evidence that the model adds signal beyond simple rules".

Locked on: ____ by: ____ in commit: ____

## Other decisions

<!-- Newest last. Format: D-0xx · date · @handle · decision · why · affected tasks -->
