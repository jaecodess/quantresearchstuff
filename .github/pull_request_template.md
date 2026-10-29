## What and why

<!-- One or two sentences. Link the task IDs from docs/progress/ (e.g. PIPE-04). -->

Tasks: <!-- PIPE-04, PIPE-05 -->

## Progress tracking (CI enforces this)

- [ ] I updated my workstream task file in `docs/progress/`: task state, `evidence:` paths for anything marked done, and one line in the progress log
- [ ] I ran `make status` and committed the regenerated `docs/progress/STATUS.md`
- [ ] I ran `make check` locally and it passes
- [ ] AI assistance was used (if ticked, add a row to `docs/progress/ai-usage.md`)

If this PR genuinely needs no progress update (typo, CI tweak), replace this line with `Progress-Exempt: <reason>` and a reviewer will confirm.

## Handover rules (HANDOVER §18)

- [ ] No credentials, tokens, raw datasets or machine-specific paths committed
- [ ] No future information in features; no random shuffles across splits; test set untouched (or this PR is the single test evaluation, after the success rule was written)
- [ ] No pandas on full data (only small aggregated results leave Spark)
- [ ] Table contracts in HANDOVER §9 unchanged, or the change is in the decision log and the consumers were told
- [ ] Assessed evidence comes from EMR, not from a local run (or this PR says it is development only)

## Cross-review

- [ ] Requested review from the cross-reviewer workstream (PIPE reviews MODEL evaluation code; MODEL reviews PIPE feature, universe and split code)

## Evidence

<!-- Job/query IDs, notebook cell, file paths, test output. Claims without evidence earn no marks. -->
