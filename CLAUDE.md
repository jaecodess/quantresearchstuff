# Instructions for AI agents (Claude Code and others)

Project: INF2006 Big Data cross-sectional signal research on AWS EMR. Source of truth is [HANDOVER.md](HANDOVER.md) (read the sections your task cites). Deadline: one team ZIP by 2026-11-22 23:59 SGT.

Current team status (generated, refreshed on every PR):

@docs/progress/STATUS.md

## Start of every session

1. Find out which workstream the human is on (CORP, PIPE, MODEL, ANLY, PERF, TEAM). Ask if unclear. Branches are named `ws/<workstream>/<topic>`.
2. Read that workstream's task file in `docs/progress/` (for example `docs/progress/pipeline.md`). Tasks there are what has been delegated. Do not invent work outside it; propose new tasks by adding a line.
3. Check each task's `needs:`. If a dependency is not `[x]`, say so and work against the contract in HANDOVER §9 or a stub instead of waiting.
4. Claim the task: set `[~]` and `owner: @handle` in the task file, and commit that early so teammates and their agents can see it.

## Rules that apply to every change

- Stay inside the paths your workstream owns (`.github/workstreams.json`). If you must touch another workstream's files, say so in the PR and tag its owner. Never change a §9 table contract silently: record it in `docs/decisions/decision-log.md` first.
- HANDOVER §18 is binding: no future information in features, no random shuffles across splits, the test set is touched once and only after the success rule is in the decision log, no pandas on full data, raw data immutable, every run writes to a new `run_id`, seeds and versions recorded.
- No credentials, tokens, raw datasets or machine-specific paths in committed files. Use `config/paths.template.yaml` placeholders. `make hygiene` checks this.
- Language: "source price ratio", "hypothetical selection output", "signal diagnostics". Never "returns", "profit", "alpha generated" or investability claims.
- Local runs are development only. Say so; assessed evidence comes from EMR.
- Keep code in owned `.py`, `.sql` and Streaming files; the notebook only calls them.

## Finishing work (required, CI enforces it)

Before you open or update a PR:

1. In your workstream task file: mark the task `[x]`, make sure `owner:` is a real handle, and add `evidence:` listing repo paths that exist (code, outputs, notebook locators, `evidence/` records). Done without evidence fails CI. If you did not finish, leave `[~]` or set `[!]` with `blocked: <reason>`.
2. Add one line to that file's `## Progress log`: `- YYYY-MM-DD @handle: what changed. Evidence: path (TASK-ID)`.
3. If AI assistance shaped the work, append a row to `docs/progress/ai-usage.md` and tick the PR checkbox. Describe the tool and purpose; do not put model identifiers in commits or code.
4. Run `make status` (regenerates `docs/progress/STATUS.md`) and `make check`. Fix failures instead of bypassing them. Never skip or weaken a test or check to get green.
5. Fill in the PR template. Use `Progress-Exempt: <reason>` only for changes that truly need no progress update.

Details: [CONTRIBUTING.md](CONTRIBUTING.md), [docs/progress/README.md](docs/progress/README.md).

## Useful commands

```bash
make help      # list targets
make status    # regenerate STATUS.md
make check     # what CI runs
```
