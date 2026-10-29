# Contributing: working in parallel as five

Five people, five workstreams, one repo. Each workstream owns separate directories and one task file, so branches rarely collide.

| Workstream | Prefix | Owns (paths) | Task file |
|---|---|---|---|
| Corpus (Task 1) | `CORP` | `corpus/ manifests/ docs/corpus/` | [corpus.md](docs/progress/corpus.md) |
| Pipeline (Task 2) | `PIPE` | `streaming/ hive/ spark/ config/split_events.csv` | [pipeline.md](docs/progress/pipeline.md) |
| Model (Task 3) | `MODEL` | `modeling/ evaluation/ models/` | [model.md](docs/progress/model.md) |
| Analytics (Task 4) | `ANLY` | `analytics/` | [analytics.md](docs/progress/analytics.md) |
| Performance & reproducibility | `PERF` | `perf/ evidence/ notebooks/ docs/rerun/` | [performance.md](docs/progress/performance.md) |
| Shared | `TEAM` | `report/ config/ docs/decisions/ .github/ scripts/` | [team.md](docs/progress/team.md) |

The exact mapping lives in [.github/workstreams.json](.github/workstreams.json).

## The loop

1. `git switch -c ws/pipe/split-detection` (always `ws/<workstream>/<topic>`, branched from `main`).
2. Open `docs/progress/STATUS.md` and your task file. Pick a task whose `needs:` are done (or build against the contract/stub). Set it `[~]` with your `owner: @handle` and push that first so everyone sees it.
3. Work inside your owned paths. Small PRs, merged often (at least every day or two).
4. Before the PR: tick the task `[x]` with `evidence:` paths, add a progress-log line, `make status`, `make check`.
5. Open the PR using the template. CI runs; a teammate from the cross-review workstream approves; squash-merge.
6. If you hit a dependency you need from someone else, set your task `[!]` with `blocked: waiting for PIPE-09 stub`, and tell them. Do not edit their files.

AI agents follow the same loop: [CLAUDE.md](CLAUDE.md) tells them to read STATUS.md and the task file first and to update the task file when done.

## What CI checks (and how to fix a red check)

| Job | Fails when | Fix |
|---|---|---|
| Progress files | A task is invalid (done without existing `evidence:`, `[~]` without owner, bad `needs:`/`due:`), STATUS.md or CODEOWNERS is stale | Fix the task line, run `make status` / `make codeowners` |
| Progress files (PR only) | The PR changes files of workstream X but not X's task file; or "AI assistance was used" is ticked without an `ai-usage.md` row | Update the task file / ai-usage log. For genuine no-ops add `Progress-Exempt: reason` to the PR body |
| Hygiene | Credentials, tokens, `/Users/..` or `/home/..` paths, raw data files (.zip, .gz, .csv outside allowed dirs), files over 10 MB, broken relative markdown links | Remove it; use placeholders from `config/paths.template.yaml` |
| Lint and unit tests | `ruff` or `pytest` fails | `make lint`, `make test` |
| Spark tests | A test marked `@pytest.mark.spark` fails (local Spark 3.5, Java 17) | `make spark-test` |
| Hadoop Streaming smoke | `streaming/<job>` output differs from `tests/expected.out` | Fix mapper/reducer or update expected output deliberately |
| Hive SQL lint | `sqlfluff` parse or correctness error in `hive/` or `analytics/` | `sqlfluff lint hive --dialect hive` |
| Notebooks parse | An `.ipynb` is not valid | Re-export |
| **CI OK** | Any job above failed | This is the one required status check |

CI proves repo hygiene, unit behaviour and tracking. It does NOT run on EMR: assessed evidence (job IDs, timings, the fresh-session reload) must come from EMR runs recorded in `evidence/`.

## Rules of thumb for parallel work

- **Contracts first.** The §9 tables are the interface between workstreams. Change them only through the decision log.
- **Stubs unblock people.** PIPE publishes a stub `features.weekly` early (PIPE-09). Build against it.
- **Cross-review.** PIPE reviews MODEL's evaluation/training code for leakage; MODEL reviews PIPE's feature/universe/split code (CODEOWNERS routes this once handles are set).
- **Gates.** MODEL-03 (grid) and MODEL-06 (success rule) must be merged to the decision log before validation and test runs respectively. The test set is evaluated once.
- **Notebook.** One owner (PERF). Everyone else supplies `.py`/`.sql`/Streaming files the notebook calls.
- **Conflicts.** Each person edits only their own task file. STATUS.md is generated: on a conflict, take either side and rerun `make status`. `ai-usage.md` is append-only (union merge).

## Setup

```bash
git clone <repo-url> && cd quantresearchstuff
make setup        # pytest, ruff, sqlfluff
make check        # should pass on a fresh clone
```

Repository admin steps (branch protection, handles, secrets): [docs/ci/SETUP.md](docs/ci/SETUP.md).
