# How progress is tracked

Work is delegated and tracked in markdown. Humans and their AI agents read the same files.

| File | Purpose | Who edits |
|---|---|---|
| [STATUS.md](STATUS.md) | Team dashboard: counts, in progress, blocked, ready to start, recent activity. **Generated.** | Nobody by hand: run `make status` |
| [team.md](team.md), [corpus.md](corpus.md), [pipeline.md](pipeline.md), [model.md](model.md), [analytics.md](analytics.md), [performance.md](performance.md) | One task file per workstream: the delegated tasks, their state, evidence, and a progress log | The workstream owner |
| [ai-usage.md](ai-usage.md) | Append-only log of AI assistance (report Appendix C) | Anyone who used AI |
| [../decisions/decision-log.md](../decisions/decision-log.md) | Accepted decisions, pre-registered grid and success rule | Whole team, via PR |

## Task line format

```
- [ ] **PIPE-04** Title · owner: @handle · due: 2026-11-06 · needs: PIPE-01, CORP-04 · evidence: spark/clean.py, evidence/pipe-04.md
```

- State: `[ ]` todo, `[~]` in progress, `[x]` done, `[!]` blocked.
- `owner` is required (a real GitHub handle, not `@TODO`) once a task is `[~]` or `[x]`.
- `evidence` is required for `[x]`: comma-separated repo paths (optionally `path#anchor`) or URLs. Paths must exist. "Marks go to verifiable evidence" (brief).
- `blocked: <reason>` is required for `[!]`.
- `needs` lists task IDs that must be done first. STATUS.md lists a task as "ready" only when all of them are done.
- Task IDs use the workstream prefix (`CORP`, `PIPE`, `MODEL`, `ANLY`, `PERF`, `TEAM`) and never change. Add new tasks with the next free number.

## Progress log

Each file ends with `## Progress log`. Add one line per finished or changed thing, newest last:

```
- 2026-11-06 @handle: Cleaning rules merged; 3 rejected-row categories found. Evidence: spark/clean.py (PIPE-04)
```

## Commands

```bash
make status      # regenerate STATUS.md from the task files
make check       # everything CI runs, locally
```

CI fails a PR when task files are invalid, STATUS.md is stale, or the PR changes files owned by a workstream without updating that workstream's task file. See [../../CONTRIBUTING.md](../../CONTRIBUTING.md).
